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

/**
 * Если rates_timeseries.json содержит < 100 записей — дополняет синтетическими
 * месячными точками из historicalDB.getExchangeRateHistory().
 * Реальные НБТ-данные имеют приоритет при дедупликации.
 */
async function loadHistoricalRates() {
  const existing = loadTimeseries();
  if (existing.length >= 100) {
    console.log(`[nbtParser] Достаточно данных (${existing.length} записей), обогащение не нужно`);
    return;
  }

  const byDate = new Map(existing.map(e => [e.date, e]));

  try {
    const hdb  = require('./historicalDB');
    const hist = hdb.getExchangeRateHistory(); // [{year, usd_tjs, eur_tjs, rub_tjs}]

    let added = 0;
    for (const row of hist) {
      const { year, usd_tjs, eur_tjs, rub_tjs } = row;
      if (!year || !usd_tjs) continue;

      for (let month = 1; month <= 12; month++) {
        const mm   = String(month).padStart(2, '0');
        const date = `${year}-${mm}-15`;

        // Не перезаписываем реальные данные
        if (byDate.has(date)) continue;

        const noise = () => 1 + (Math.random() - 0.5) * 0.01; // ±0.5%
        byDate.set(date, {
          date,
          usd: usd_tjs  ? Math.round(usd_tjs  * noise() * 10000) / 10000 : null,
          eur: eur_tjs  ? Math.round(eur_tjs  * noise() * 10000) / 10000 : null,
          rub: rub_tjs  ? Math.round(rub_tjs  * noise() * 10000) / 10000 : null,
          cny: null,
          synthetic: true,
        });
        added++;
      }
    }

    const sorted = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
    fs.writeFileSync(TIMESERIES_FILE, JSON.stringify(sorted, null, 2), 'utf8');
    console.log(`[nbtParser] Обогащение: добавлено ${added} синтетических точек. Итого: ${sorted.length}`);
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
