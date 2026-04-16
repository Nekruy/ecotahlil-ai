'use strict';

/**
 * nbtParser.js — Автопарсинг курсов НБТ (nbt.tj)
 * Использует те же URL и технику что и dataCollector.js
 * Без внешних зависимостей (встроенные https/http, regex)
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

const TIMESERIES_FILE = path.join(__dirname, 'data', 'rates_timeseries.json');
const RATES_HIST_FILE = path.join(__dirname, 'rates-history.json');

// NBT XML URLs (в порядке приоритета)
const NBT_XML_URLS = [
  'https://nbt.tj/rates/xml.php',
  'https://nbt.tj/ru/kurs/kursi.xml',
];
const NBT_HTML_URL = 'https://nbt.tj/ru/kurs/kurs.php';

// ─── HTTP-утилита (аналогична dataCollector) ─────────────────────────────────

function fetchUrl(urlStr) {
  return new Promise((resolve, reject) => {
    const url    = new URL(urlStr);
    const client = url.protocol === 'https:' ? https : http;
    const req = client.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method:   'GET',
      headers: {
        'User-Agent':      'Mozilla/5.0 (compatible; EcotahlilAI/1.0)',
        'Accept':          'text/html,application/xml,*/*',
        'Accept-Language': 'ru,en;q=0.9',
      },
      rejectUnauthorized: false,  // НБТ использует self-signed cert
      timeout: 15000,
    }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const next = res.headers.location.startsWith('http')
          ? res.headers.location : url.origin + res.headers.location;
        return fetchUrl(next).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('NBT request timeout')); });
    req.end();
  });
}

// ─── Парсинг XML (формат ЦБ РФ, совместимый с НБТ) ──────────────────────────

const CURRENCIES = [
  { key: 'usd', code: 'USD' },
  { key: 'eur', code: 'EUR' },
  { key: 'rub', code: 'RUB' },
  { key: 'cny', code: 'CNY' },
];

function parseXml(body) {
  const result = {};
  for (const { key, code } of CURRENCIES) {
    // Ищем блок с CharCode, извлекаем Value и Nominal
    const pat = new RegExp(
      `<CharCode>${code}<\\/CharCode>[\\s\\S]{0,400}?<Value>([\\d.,]+)<\\/Value>`, 'i'
    );
    const m = body.match(pat);
    if (m) {
      const nomM = body.match(
        new RegExp(`<CharCode>${code}<\\/CharCode>[\\s\\S]{0,200}?<Nominal>([\\d]+)<\\/Nominal>`, 'i')
      );
      const nominal = nomM ? parseInt(nomM[1]) : 1;
      const val = parseFloat(m[1].replace(',', '.')) / nominal;
      result[key] = isFinite(val) && val > 0 ? Math.round(val * 10000) / 10000 : null;
    } else {
      result[key] = null;
    }
  }
  // Дата
  let date = new Date().toISOString().slice(0, 10);
  const dm = body.match(/Date="(\d{2})\.(\d{2})\.(\d{4})"/i);
  if (dm) date = `${dm[3]}-${dm[2]}-${dm[1]}`;
  else {
    const dm2 = body.match(/<Date>(\d{4}-\d{2}-\d{2})/i);
    if (dm2) date = dm2[1];
  }
  return { ...result, date };
}

function parseHtml(body) {
  const result = {};
  for (const { key, code } of CURRENCIES) {
    const patterns = [
      new RegExp(`"${code}"\\s*:\\s*"?([\\d]+[.,][\\d]+)"?`, 'i'),
      new RegExp(`<td[^>]*>${code}<\\/td>[\\s\\S]{0,200}?<td[^>]*>([\\d]+[.,][\\d]+)<\\/td>`, 'i'),
    ];
    let found = null;
    for (const pat of patterns) {
      const m = body.match(pat);
      if (m) { const v = parseFloat(m[1].replace(',', '.')); if (v > 0) { found = v; break; } }
    }
    result[key] = found;
  }
  let date = new Date().toISOString().slice(0, 10);
  const dm = body.match(/(\d{2})[.\-/](\d{2})[.\-/](\d{4})/);
  if (dm) date = `${dm[3]}-${dm[2]}-${dm[1]}`;
  return { ...result, date };
}

// ─── Публичные функции ────────────────────────────────────────────────────────

/**
 * Загрузить текущие курсы НБТ (USD, EUR, RUB, CNY).
 * Сначала пробует XML, потом делегирует dataCollector.fetchNBT().
 */
async function fetchNBTRates() {
  // Попытка 1: XML API (напрямую)
  for (const xmlUrl of NBT_XML_URLS) {
    try {
      const { status, body } = await fetchUrl(xmlUrl);
      if (status === 200 && (body.includes('CharCode') || body.includes('ValCurs'))) {
        const parsed = parseXml(body);
        if (parsed.usd || parsed.eur || parsed.rub) {
          console.log(`[nbtParser] XML OK: ${xmlUrl}`);
          return { ...parsed, fetchedAt: new Date().toISOString(), source: xmlUrl };
        }
      }
    } catch (e) {
      console.warn(`[nbtParser] XML failed (${xmlUrl}):`, e.message);
    }
  }

  // Попытка 2: делегируем проверенному dataCollector.fetchNBT()
  try {
    const { fetchNBT } = require('./dataCollector');
    const result = await fetchNBT();
    const r = result.rates || {};
    // Парсим дату ДД.ММ.ГГГГ → ГГГГ-ММ-ДД
    let date = new Date().toISOString().slice(0, 10);
    const dm = (result.date || '').match(/(\d{2})\.(\d{2})\.(\d{4})/);
    if (dm) date = `${dm[3]}-${dm[2]}-${dm[1]}`;
    else if (result.date && result.date.length >= 10) date = result.date.slice(0, 10);
    return {
      date,
      usd: r.USD?.rate ?? null,
      eur: r.EUR?.rate ?? null,
      rub: r.RUB?.rate ?? null,
      cny: r.CNY?.rate ?? null,
      fetchedAt: new Date().toISOString(),
      source: 'dataCollector.fetchNBT',
    };
  } catch (e) {
    console.warn('[nbtParser] dataCollector.fetchNBT failed:', e.message);
  }

  throw new Error('Не удалось загрузить курсы НБТ ни по XML, ни через dataCollector');
}

/** Загрузить timeseries с диска */
function loadTimeseries() {
  try {
    if (fs.existsSync(TIMESERIES_FILE)) return JSON.parse(fs.readFileSync(TIMESERIES_FILE, 'utf8'));
  } catch (e) { console.error('[nbtParser] loadTimeseries:', e.message); }
  return [];
}

/**
 * Сохранить текущие курсы в data/rates_timeseries.json.
 * Импортирует исторические данные из rates-history.json.
 * Дедупликация по дате.
 */
async function saveRatesToDB() {
  const series = loadTimeseries();
  const byDate = new Map(series.map(e => [e.date, e]));

  // 1. Импорт из rates-history.json (fallback историческая база)
  try {
    if (fs.existsSync(RATES_HIST_FILE)) {
      const hist = JSON.parse(fs.readFileSync(RATES_HIST_FILE, 'utf8'));
      for (const entry of hist) {
        const date = entry.date || (entry.fetchedAt || '').slice(0, 10);
        if (!date || byDate.has(date)) continue;
        byDate.set(date, {
          date,
          usd: entry.USD ?? entry.usd ?? null,
          eur: entry.EUR ?? entry.eur ?? null,
          rub: entry.RUB ?? entry.rub ?? null,
          cny: entry.CNY ?? entry.cny ?? null,
        });
      }
    }
  } catch (e) { console.warn('[nbtParser] fallback import:', e.message); }

  // 2. Свежие данные с НБТ
  let latest = null;
  try {
    const rates = await fetchNBTRates();
    const { date, usd, eur, rub, cny } = rates;
    byDate.set(date, { date, usd, eur, rub, cny });
    latest = { date, usd, eur, rub, cny };
    console.log(`[nbtParser] Курсы НБТ ${date}: USD=${usd} EUR=${eur} RUB=${rub} CNY=${cny}`);
  } catch (e) {
    console.warn('[nbtParser] fetchNBTRates:', e.message);
    // Используем последнюю известную запись
    const all = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
    latest = all[all.length - 1] || null;
  }

  // 3. Сохраняем
  const sorted = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
  fs.writeFileSync(TIMESERIES_FILE, JSON.stringify(sorted, null, 2), 'utf8');
  console.log(`[nbtParser] Сохранено ${sorted.length} записей в rates_timeseries.json`);
  return { ok: true, entries: sorted.length, latest };
}

/**
 * Получить историю курсов за последние N дней.
 * @param {string} currency — 'usd' | 'eur' | 'rub' | 'cny'
 * @param {number} days
 */
async function getRatesHistory(currency, days) {
  const cur    = currency.toLowerCase();
  const series = loadTimeseries();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutStr = cutoff.toISOString().slice(0, 10);
  return series.filter(e => e.date >= cutStr && e[cur] != null);
}

// ─── Обогащение историческими данными ────────────────────────────────────────

/** Детерминированный PRNG на основе sin — воспроизводимый по seed. */
function seededRandom(seed) {
  const x = Math.sin(seed + 1) * 10000;
  return x - Math.floor(x);
}

/** Box-Muller: два U[0,1) → N(0,1). */
function boxMuller(u1, u2) {
  return Math.sqrt(-2 * Math.log(Math.max(u1, 1e-10))) * Math.cos(2 * Math.PI * u2);
}

/**
 * Дополняет rates_timeseries.json историческими данными если < 100 реальных записей.
 * Алгоритм: mean-reverting random walk вокруг линейного тренда между годовыми якорями.
 *   currentRate = currentRate*(1 + vol*z)*(1-λ) + targetRate*λ
 *   vol = 0.6%/мес, λ = 0.15 (возврат к тренду), z ~ N(0,1) детерминирован по (year, month).
 * Реальные НБТ-данные никогда не перезаписываются.
 */
async function loadHistoricalRates() {
  const existing   = loadTimeseries();
  const realEntries = existing.filter(e => !e.synthetic);

  if (realEntries.length >= 100) {
    console.log(`[nbtParser] Достаточно реальных данных (${realEntries.length}), обогащение не нужно`);
    return;
  }

  // Реальные точки имеют приоритет
  const byDate = new Map(realEntries.map(e => [e.date, e]));

  try {
    const hdb     = require('./historicalDB');
    const hist    = hdb.getExchangeRateHistory();
    const anchors = [...hist].sort((a, b) => a.year - b.year);

    const MONTHLY_VOL = 0.006;  // 0.6% σ/мес ≈ 2.1% годовых
    const MEAN_REVERT = 0.15;   // возврат к линейному тренду
    let added = 0;

    for (let i = 0; i < anchors.length; i++) {
      const cur  = anchors[i];
      const next = anchors[i + 1];

      // Стартуем с годового значения
      let curUsd = cur.usd_tjs;
      let curEur = cur.eur_tjs;
      let curRub = cur.rub_tjs;

      for (let month = 1; month <= 12; month++) {
        const mm   = String(month).padStart(2, '0');
        const date = `${cur.year}-${mm}-15`;

        if (byDate.has(date)) {
          // Обновляем текущее значение из реальной точки для непрерывности
          const real = byDate.get(date);
          if (real.usd) curUsd = real.usd;
          if (real.eur) curEur = real.eur;
          if (real.rub) curRub = real.rub;
          continue;
        }

        // Линейный якорь: начало года → начало следующего
        const t      = month / 12;
        const tgtUsd = cur.usd_tjs + ((next?.usd_tjs ?? cur.usd_tjs) - cur.usd_tjs) * t;
        const tgtEur = cur.eur_tjs + ((next?.eur_tjs ?? cur.eur_tjs) - cur.eur_tjs) * t;
        const tgtRub = cur.rub_tjs + ((next?.rub_tjs ?? cur.rub_tjs) - cur.rub_tjs) * t;

        // Детерминированные шоки через Box-Muller + seededRandom
        const seed = cur.year * 100 + month;
        const zUsd = boxMuller(seededRandom(seed * 7 + 1), seededRandom(seed * 7 + 2));
        const zEur = boxMuller(seededRandom(seed * 7 + 3), seededRandom(seed * 7 + 4));
        const zRub = boxMuller(seededRandom(seed * 7 + 5), seededRandom(seed * 7 + 6));

        // Mean-reverting step: стохастика + дрейф к тренду
        curUsd = curUsd * (1 + MONTHLY_VOL * zUsd) * (1 - MEAN_REVERT) + tgtUsd * MEAN_REVERT;
        curEur = curEur * (1 + MONTHLY_VOL * zEur) * (1 - MEAN_REVERT) + tgtEur * MEAN_REVERT;
        curRub = curRub * (1 + MONTHLY_VOL * zRub) * (1 - MEAN_REVERT) + tgtRub * MEAN_REVERT;

        byDate.set(date, {
          date,
          usd: Math.round(curUsd * 10000) / 10000,
          eur: Math.round(curEur * 10000) / 10000,
          rub: Math.round(curRub * 10000) / 10000,
          cny: null,
          synthetic: true,
        });
        added++;
      }
    }

    // Добавляем дневные срезы из rates-history.json если есть
    const legacyPath = path.join(__dirname, 'rates-history.json');
    if (fs.existsSync(legacyPath)) {
      try {
        const legacy = JSON.parse(fs.readFileSync(legacyPath, 'utf8'));
        for (const entry of legacy) {
          const date = entry.date || (entry.fetchedAt || '').slice(0, 10);
          if (!date || byDate.has(date)) continue;
          byDate.set(date, {
            date,
            usd: entry.USD ?? entry.usd ?? null,
            eur: entry.EUR ?? entry.eur ?? null,
            rub: entry.RUB ?? entry.rub ?? null,
            cny: entry.CNY ?? entry.cny ?? null,
          });
          added++;
        }
      } catch (_) {}
    }

    const result = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
    fs.writeFileSync(TIMESERIES_FILE, JSON.stringify(result, null, 2), 'utf8');
    console.log(`[nbtParser] Обогащение завершено: +${added} точек, итого ${result.length}`);
  } catch (e) {
    console.error('[nbtParser] loadHistoricalRates:', e.message);
  }
}

// ─── Авто-обновление каждые 24 ч ─────────────────────────────────────────────

let _timer = null;

function startAutoRefresh() {
  if (_timer) return;
  const MS_24H = 24 * 60 * 60 * 1000;
  _timer = setInterval(() => {
    saveRatesToDB().catch(e => console.error('[nbtParser] auto-refresh:', e.message));
  }, MS_24H);
  if (_timer.unref) _timer.unref();
  console.log('[nbtParser] Авто-обновление курсов НБТ запущено (каждые 24 ч)');
}

module.exports = { fetchNBTRates, saveRatesToDB, getRatesHistory, startAutoRefresh, loadTimeseries, loadHistoricalRates };
