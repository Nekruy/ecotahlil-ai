'use strict';

// ── liveData.js — кэш реальных данных НБТ/МВФ/ВБ/сырьё ──────────────────────
//
// Единое место для хранения и обновления live-данных.
// server.js вызывает startLiveDataScheduler() один раз при старте.
// Клиенты читают через getLiveCache() — никогда не ждут внешних запросов.

const fs   = require('fs');
const path = require('path');

const LIVE_CACHE_FILE = path.join(__dirname, 'live_cache.json');
const LIVE_TTL_MS     = 60 * 60 * 1000;  // 1 час — частота авто-обновления

let _memCache    = null;  // in-memory кэш (быстрый доступ)
let _schedulerID = null;  // ID setInterval чтобы не запустить дважды

// ── Загрузка/сохранение дискового кэша ───────────────────────────────────────

function _loadDiskCache() {
  try {
    if (fs.existsSync(LIVE_CACHE_FILE)) {
      return JSON.parse(fs.readFileSync(LIVE_CACHE_FILE, 'utf8'));
    }
  } catch {}
  return null;
}

function _saveDiskCache(data) {
  try {
    fs.writeFileSync(LIVE_CACHE_FILE, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {
    console.warn('[liveData] Ошибка записи кэша:', e.message);
  }
}

// ── Источник 1: курсы НБТ ────────────────────────────────────────────────────

async function fetchNBTLive() {
  const { fetchNBT } = require('./dataCollector');
  try {
    const result = await fetchNBT();
    return {
      usd:  result.rates?.USD?.rate ?? null,
      eur:  result.rates?.EUR?.rate ?? null,
      rub:  result.rates?.RUB?.rate ?? null,
      date: result.date,
      ok:   true,
    };
  } catch (e) {
    console.warn('[liveData] НБТ:', e.message);
    return { usd: null, eur: null, rub: null, date: null, ok: false, error: e.message };
  }
}

// ── Источник 2: МВФ (6 индикаторов) ─────────────────────────────────────────

async function fetchIMFLive() {
  const { fetchIMFRealtime } = require('./dataCollector');
  try {
    const result = await fetchIMFRealtime();
    return { ...result, ok: true };
  } catch (e) {
    console.warn('[liveData] МВФ:', e.message);
    return { ok: false, error: e.message };
  }
}

// ── Источник 3: Всемирный банк — инфляция ────────────────────────────────────

async function fetchWorldBankLive() {
  const { fetchWorldBank } = require('./dataCollector');
  try {
    const cpi = await fetchWorldBank('FP.CPI.TOTL.ZG');
    const currentYear = new Date().getFullYear();
    const series = (cpi.series || []).filter(d => d.year <= currentYear + 1);
    const latest = series[series.length - 1] || null;
    return {
      cpi_inflation_pct: latest?.value ?? null,
      cpi_year:          latest?.year  ?? null,
      series:            series.slice(-5),
      source:            'data.worldbank.org',
      ok:                true,
    };
  } catch (e) {
    console.warn('[liveData] Всемирный банк:', e.message);
    return { cpi_inflation_pct: null, cpi_year: null, ok: false, error: e.message };
  }
}

// ── Источник 4: сырьевые цены (Yahoo Finance) ────────────────────────────────

async function fetchCommodityPrices() {
  const { fetchOilPrice, fetchAluminumPrice, fetchWheatPrice } = require('./dataCollector');
  const [oilR, alR, wheatR] = await Promise.allSettled([
    fetchOilPrice(),
    fetchAluminumPrice(),
    fetchWheatPrice(),
  ]);
  const pick = r => r.status === 'fulfilled'
    ? { price: r.value.price, changePct: r.value.changePct, unit: r.value.unit, label: r.value.label, ok: true }
    : { price: null, ok: false, error: r.reason?.message };
  return {
    oil:      pick(oilR),
    aluminum: pick(alR),
    wheat:    pick(wheatR),
  };
}

// ── Основная функция обновления ───────────────────────────────────────────────

async function refreshLiveData() {
  console.log('[liveData] Обновление live-данных...');
  const started = Date.now();

  const [nbt, imf, wb, commodities] = await Promise.allSettled([
    fetchNBTLive(),
    fetchIMFLive(),
    fetchWorldBankLive(),
    fetchCommodityPrices(),
  ]);

  const pick = r => r.status === 'fulfilled' ? r.value : { ok: false, error: r.reason?.message };

  const data = {
    nbt:         pick(nbt),
    imf:         pick(imf),
    worldbank:   pick(wb),
    commodities: pick(commodities),
    updated_at:  new Date().toISOString(),
    refresh_ms:  Date.now() - started,
  };

  _memCache = data;
  _saveDiskCache(data);

  const okCount = ['nbt','imf','worldbank','commodities'].filter(k => data[k]?.ok !== false).length;
  console.log(`[liveData] Обновлено за ${data.refresh_ms}ms: ${okCount}/4 источников успешно`);
  return data;
}

// ── Получить кэш (никогда не ждёт внешних запросов) ─────────────────────────

function getLiveCache() {
  if (_memCache) return _memCache;
  const disk = _loadDiskCache();
  if (disk) { _memCache = disk; return disk; }
  return null;
}

// ── Планировщик авто-обновления ──────────────────────────────────────────────

function startLiveDataScheduler(intervalMs) {
  if (_schedulerID) return; // уже запущен

  const interval = intervalMs || LIVE_TTL_MS;

  // Первое обновление — через 5 секунд после старта (не блокируем запуск сервера)
  setTimeout(() => {
    refreshLiveData().catch(e => console.warn('[liveData] Первоначальное обновление не удалось:', e.message));
  }, 5000);

  // Периодическое обновление
  _schedulerID = setInterval(() => {
    refreshLiveData().catch(e => console.warn('[liveData] Периодическое обновление не удалось:', e.message));
  }, interval);

  // Не мешаем graceful shutdown
  if (_schedulerID.unref) _schedulerID.unref();

  console.log(`[liveData] Планировщик запущен: обновление каждые ${Math.round(interval / 60000)} мин`);
}

module.exports = {
  fetchNBTLive,
  fetchIMFLive,
  fetchWorldBankLive,
  fetchCommodityPrices,
  refreshLiveData,
  getLiveCache,
  startLiveDataScheduler,
};
