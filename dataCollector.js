/**
 * dataCollector.js — сбор данных из Всемирного банка, НБТ и внешних источников
 * Чистый Node.js, без внешних зависимостей
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

const HISTORY_FILE = path.join(__dirname, 'rates-history.json');
const CACHE_FILE   = path.join(__dirname, 'cache.json');
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 час

// ─── HTTP/HTTPS утилита ───────────────────────────────────────────────────────

function fetchUrl(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const url    = new URL(urlStr);
    const client = url.protocol === 'https:' ? https : http;
    const options = {
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method:   'GET',
      headers:  {
        'User-Agent': 'Mozilla/5.0 (compatible; EcotahlilAI/1.0; +https://nbt.tj)',
        'Accept':     'text/html,application/json,*/*',
        'Accept-Language': 'ru,en;q=0.9',
      },
      rejectUnauthorized: false,   // NBT может иметь self-signed cert
      timeout: 12000,
      ...opts,
    };

    const req = client.request(options, (res) => {
      // Обрабатываем редиректы
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const nextUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : url.origin + res.headers.location;
        return fetchUrl(nextUrl, opts).then(resolve).catch(reject);
      }

      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        // Пробуем определить кодировку
        const contentType = res.headers['content-type'] || '';
        let body;
        if (contentType.includes('windows-1251') || contentType.includes('cp1251')) {
          body = buf.toString('latin1'); // ближайшее к 1251 в Node
        } else {
          body = buf.toString('utf8');
        }
        resolve({ status: res.statusCode, body, headers: res.headers });
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    req.end();
  });
}

// ─── Всемирный банк ───────────────────────────────────────────────────────────

const WB_INDICATORS = {
  GDP:          'NY.GDP.MKTP.CD',
  CPI:          'FP.CPI.TOTL.ZG',
  INFLATION:    'FP.CPI.TOTL.ZG',
  UNEMPLOYMENT: 'SL.UEM.TOTL.ZS',
  // Прямые коды тоже принимаем
  'NY.GDP.MKTP.CD':  'NY.GDP.MKTP.CD',
  'FP.CPI.TOTL.ZG':  'FP.CPI.TOTL.ZG',
  'SL.UEM.TOTL.ZS':  'SL.UEM.TOTL.ZS',
};

const WB_LABELS = {
  'NY.GDP.MKTP.CD': 'ВВП (текущие USD)',
  'FP.CPI.TOTL.ZG': 'Инфляция (% в год)',
  'SL.UEM.TOTL.ZS': 'Безработица (% от раб. силы)',
};

async function fetchWorldBank(indicator) {
  const code = WB_INDICATORS[indicator.toUpperCase()] || WB_INDICATORS[indicator] || indicator;
  const url  = `https://api.worldbank.org/v2/country/TJ/indicator/${code}?format=json&per_page=25&mrv=25`;

  const { body, status } = await fetchUrl(url);

  if (status !== 200) {
    throw new Error(`Всемирный банк вернул статус ${status}`);
  }

  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    throw new Error('Неверный формат ответа Всемирного банка (не JSON)');
  }

  if (!Array.isArray(parsed) || !parsed[1]) {
    const msg = parsed?.[0]?.message?.[0]?.value || 'Нет данных';
    throw new Error(`Всемирный банк: ${msg}`);
  }

  const series = parsed[1]
    .filter(item => item.value !== null && item.value !== undefined)
    .map(item => ({
      year:  parseInt(item.date),
      value: Math.round(item.value * 100) / 100,
    }))
    .sort((a, b) => a.year - b.year);

  return {
    indicator: code,
    label:     WB_LABELS[code] || code,
    country:   'Таджикистан',
    series,
    source:    'data.worldbank.org',
    fetched:   new Date().toISOString(),
  };
}

// ─── Национальный банк Таджикистана ──────────────────────────────────────────

async function fetchNBT() {
  let body = '';

  try {
    // index.php редиректит на kurs.php — запрашиваем сразу финальный URL
    const res = await fetchUrl('https://nbt.tj/ru/kurs/kurs.php');
    body = res.body;
  } catch (err) {
    throw new Error('Не удалось подключиться к сайту НБТ: ' + err.message);
  }

  const rates = {};

  // Структура страницы НБТ:
  // <td ...>Доллар США</td><td ...>9.5751</td>
  // Ищем по русскому названию валюты → следующий td с числом
  const currencyMap = [
    { key: 'USD', name: 'Доллар США',     flag: '🇺🇸', search: 'Доллар США' },
    { key: 'EUR', name: 'Евро',           flag: '🇪🇺', search: 'Евро' },
    { key: 'RUB', name: 'Российский рубль', flag: '🇷🇺', search: 'Рубл' },
  ];

  for (const cur of currencyMap) {
    // Паттерн: после названия валюты идёт <td ...>ЧИСЛО</td>
    const pat = new RegExp(
      cur.search + '[\\s\\S]{0,300}?<td[^>]*>\\s*([\\d]+[.,][\\d]+)\\s*<\\/td>',
      'i'
    );
    const m = body.match(pat);
    if (m) {
      const v = parseFloat(m[1].replace(',', '.'));
      if (!isNaN(v) && v > 0) {
        rates[cur.key] = { rate: v, name: cur.name, flag: cur.flag };
        continue;
      }
    }
    rates[cur.key] = { rate: null, name: cur.name, flag: cur.flag };
  }

  // Ставка рефинансирования (обычно на отдельной странице, ищем на текущей)
  let refinancingRate = null;
  const refPatterns = [
    /рефинансиров[^<]{0,80}([\d]+[.,][\d]+)\s*%/i,
    /учётн[^<]{0,80}([\d]+[.,][\d]+)\s*%/i,
    /ставк[^<]{0,80}([\d]+[.,][\d]+)\s*%/i,
  ];
  for (const pat of refPatterns) {
    const m = body.match(pat);
    if (m) {
      const v = parseFloat(m[1].replace(',', '.'));
      if (!isNaN(v) && v > 0 && v < 50) { refinancingRate = v; break; }
    }
  }

  // Дата из страницы
  const dateMatch = body.match(/(\d{2})[.\-/](\d{2})[.\-/](\d{4})/);
  const rateDate  = dateMatch
    ? `${dateMatch[1]}.${dateMatch[2]}.${dateMatch[3]}`
    : new Date().toLocaleDateString('ru-RU');

  const result = {
    date:           rateDate,
    fetchedAt:      new Date().toISOString(),
    rates,
    refinancingRate,
    source:         'nbt.tj',
  };

  saveRatesHistory(result);
  return result;
}

// ─── История курсов (для графика динамики) ────────────────────────────────────

function loadRatesHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
    }
  } catch {}
  return [];
}

function saveRatesHistory(entry) {
  try {
    const history = loadRatesHistory();
    // Один срез в день — ищем сегодняшнюю запись и обновляем
    const today = new Date().toISOString().slice(0, 10);
    const idx   = history.findIndex(h => h.fetchedAt && h.fetchedAt.slice(0, 10) === today);

    const record = {
      date:      today,
      fetchedAt: entry.fetchedAt,
      USD: entry.rates.USD?.rate,
      EUR: entry.rates.EUR?.rate,
      RUB: entry.rates.RUB?.rate,
    };

    if (idx >= 0) {
      history[idx] = record;
    } else {
      history.push(record);
    }

    // Хранить максимум 90 дней
    const trimmed = history.slice(-90);
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(trimmed, null, 2), 'utf8');
  } catch {}
}

function getRatesHistory() {
  return loadRatesHistory().slice(-30); // последние 30 дней
}

// ─── Кэш (cache.json) ────────────────────────────────────────────────────────

function loadCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
    }
  } catch {}
  return {};
}

function saveCache(cache) {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), 'utf8');
  } catch (e) {
    console.error('[cache] Ошибка записи:', e.message);
  }
}

function getCached(key) {
  const cache = loadCache();
  const entry = cache[key];
  if (!entry || !entry.fetchedAt) return null;
  const age = Date.now() - new Date(entry.fetchedAt).getTime();
  if (age > CACHE_TTL_MS) return null;
  return entry.data;
}

function setCached(key, data) {
  const cache = loadCache();
  cache[key] = { data, fetchedAt: new Date().toISOString() };
  saveCache(cache);
}

// ─── Yahoo Finance (нефть, алюминий, пшеница) ─────────────────────────────

async function fetchYahooFinance(symbol, label, unit) {
  const cacheKey = `yahoo_${symbol}`;
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=1mo&interval=1d`;

  let parsed;
  try {
    const { body, status } = await fetchUrl(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    });

    if (status !== 200) throw new Error(`Yahoo Finance вернул статус ${status}`);
    parsed = JSON.parse(body);
  } catch (err) {
    console.error(`[Yahoo ${symbol}] Ошибка:`, err.message);
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`[Yahoo ${symbol}] Возврат из кэша`);
      return { ...cached, fromCache: true };
    }
    throw new Error(`${label}: нет данных (${err.message})`);
  }

  const result = parsed?.chart?.result?.[0];
  if (!result) {
    const cached = getCached(cacheKey);
    if (cached) return { ...cached, fromCache: true };
    throw new Error(`${label}: пустой ответ от Yahoo Finance`);
  }

  const meta      = result.meta || {};
  const price     = meta.regularMarketPrice ?? meta.regularMarketPreviousClose ?? null;
  const prevClose = meta.previousClose ?? meta.chartPreviousClose ?? null;
  const change    = price !== null && prevClose ? price - prevClose : null;
  const changePct = change !== null && prevClose ? (change / prevClose) * 100 : null;

  // 30-дневная история
  const timestamps = result.timestamp || [];
  const closes     = result.indicators?.quote?.[0]?.close || [];
  const history30  = timestamps.map((ts, i) => ({
    date:  new Date(ts * 1000).toISOString().slice(0, 10),
    value: closes[i] !== null && closes[i] !== undefined ? Math.round(closes[i] * 100) / 100 : null,
  })).filter(d => d.value !== null);

  const data = {
    symbol,
    label,
    unit,
    price:     price !== null ? Math.round(price * 100) / 100 : null,
    prevClose: prevClose !== null ? Math.round(prevClose * 100) / 100 : null,
    change:    change !== null ? Math.round(change * 100) / 100 : null,
    changePct: changePct !== null ? Math.round(changePct * 100) / 100 : null,
    currency:  meta.currency || 'USD',
    history30,
    source:    'finance.yahoo.com',
    fetched:   new Date().toISOString(),
    fromCache: false,
  };

  setCached(cacheKey, data);
  console.log(`[Yahoo ${symbol}] OK — ${price} ${data.currency}`);
  return data;
}

// ─── Источник 1: Азиатский банк развития (ADB) ───────────────────────────

async function fetchADB() {
  const cacheKey = 'adb';
  const url = 'https://kidb.adb.org/api/v2/indicators?economy=TAJ';

  let parsed;
  try {
    const { body, status } = await fetchUrl(url);
    if (status !== 200) throw new Error(`ADB вернул статус ${status}`);
    parsed = JSON.parse(body);
    console.log('[ADB] OK');
  } catch (err) {
    console.error('[ADB] Ошибка:', err.message);
    const cached = getCached(cacheKey);
    if (cached) {
      console.log('[ADB] Возврат из кэша');
      return { ...cached, fromCache: true };
    }
    throw new Error('ADB: нет данных (' + err.message + ')');
  }

  // ADB KIDB возвращает список индикаторов — берём ключевые
  const indicators = Array.isArray(parsed) ? parsed : (parsed.data || parsed.indicators || []);
  const KEY_CODES  = ['GDP', 'INFLATION', 'TRADE', 'FDI', 'IMPORT', 'EXPORT'];
  const filtered   = indicators.filter(ind => {
    const code = (ind.code || ind.indicator_code || '').toUpperCase();
    return KEY_CODES.some(k => code.includes(k));
  }).slice(0, 20);

  const data = {
    source:     'kidb.adb.org',
    country:    'Таджикистан (TAJ)',
    indicators: filtered,
    total:      indicators.length,
    fetched:    new Date().toISOString(),
    fromCache:  false,
  };

  setCached(cacheKey, data);
  return data;
}

// ─── Источник 2: ВТО — торговля Таджикистана ─────────────────────────────

async function fetchWTO() {
  const cacheKey = 'wto';
  const url = 'https://api.wto.org/timeseries/v1/data?i=ITS_MTV_AM&r=TJK&fmt=json&max=10&lang=1';

  let parsed;
  try {
    const { body, status } = await fetchUrl(url);
    if (status !== 200) throw new Error(`ВТО вернул статус ${status}`);
    parsed = JSON.parse(body);
    console.log('[WTO] OK');
  } catch (err) {
    console.error('[WTO] Ошибка:', err.message);
    const cached = getCached(cacheKey);
    if (cached) {
      console.log('[WTO] Возврат из кэша');
      return { ...cached, fromCache: true };
    }
    throw new Error('ВТО: нет данных (' + err.message + ')');
  }

  const rows  = parsed.Dataset || parsed.data || [];
  const series = rows.map(row => ({
    year:        row.Year || row.year,
    value:       row.Value || row.value,
    description: row.IndicatorDescription || row.description || 'Торговля',
    unit:        row.Unit || row.unit || 'млн USD',
  })).filter(r => r.year && r.value !== null).sort((a, b) => a.year - b.year);

  const data = {
    source:   'api.wto.org',
    country:  'Таджикистан (TJK)',
    indicator: 'ITS_MTV_AM — Товарный экспорт/импорт',
    series,
    fetched:  new Date().toISOString(),
    fromCache: false,
  };

  setCached(cacheKey, data);
  return data;
}

// ─── Источник 3: Центральный банк России (RUB/TJS) ───────────────────────

async function fetchCBR() {
  const cacheKey = 'cbr';
  const url = 'https://www.cbr.ru/scripts/XML_daily.asp';

  let body;
  try {
    const res = await fetchUrl(url);
    if (res.status !== 200) throw new Error(`ЦБ РФ вернул статус ${res.status}`);
    body = res.body;
    console.log('[CBR] OK');
  } catch (err) {
    console.error('[CBR] Ошибка:', err.message);
    const cached = getCached(cacheKey);
    if (cached) {
      console.log('[CBR] Возврат из кэша');
      return { ...cached, fromCache: true };
    }
    throw new Error('ЦБ РФ: нет данных (' + err.message + ')');
  }

  // Парсим XML вручную: ищем TJS
  const tjsMatch = body.match(
    /<Valute[^>]*>[\s\S]*?<CharCode>TJS<\/CharCode>[\s\S]*?<Nominal>([\d]+)<\/Nominal>[\s\S]*?<Name>[^<]+<\/Name>[\s\S]*?<Value>([\d,]+)<\/Value>[\s\S]*?<\/Valute>/i
  );

  let rubPerTjs = null, tjsPerRub = null, nominal = null;
  if (tjsMatch) {
    nominal    = parseInt(tjsMatch[1]);
    const val  = parseFloat(tjsMatch[2].replace(',', '.'));
    rubPerTjs  = Math.round((val / nominal) * 10000) / 10000; // рублей за 1 TJS
    tjsPerRub  = Math.round((nominal / val) * 10000) / 10000; // TJS за 1 рубль
  }

  // Также берём USD/RUB для контекста
  const usdMatch = body.match(
    /<Valute[^>]*>[\s\S]*?<CharCode>USD<\/CharCode>[\s\S]*?<Nominal>([\d]+)<\/Nominal>[\s\S]*?<Name>[^<]+<\/Name>[\s\S]*?<Value>([\d,]+)<\/Value>[\s\S]*?<\/Valute>/i
  );
  let usdRub = null;
  if (usdMatch) {
    usdRub = parseFloat(usdMatch[2].replace(',', '.')) / parseInt(usdMatch[1]);
  }

  const dateMatch = body.match(/Date="([\d.]+)"/);
  const rateDate  = dateMatch ? dateMatch[1] : new Date().toLocaleDateString('ru-RU');

  const data = {
    source:    'cbr.ru',
    date:      rateDate,
    rubPerTjs, // рублей за 1 TJS
    tjsPerRub, // TJS за 1 рубль (для переводов мигрантов: сколько сомони получит мигрант за 1 рубль)
    usdRub,    // USD/RUB справочно
    note:      'Курс важен для расчёта денежных переводов мигрантов из России в Таджикистан',
    fetched:   new Date().toISOString(),
    fromCache: false,
  };

  setCached(cacheKey, data);
  return data;
}

// ─── Источник 4: Цена нефти Brent ────────────────────────────────────────

async function fetchOilPrice() {
  return fetchYahooFinance('CL=F', 'Нефть Brent (WTI)', '$/барр.');
}

// ─── Источник 5: Цена алюминия ────────────────────────────────────────────

async function fetchAluminumPrice() {
  return fetchYahooFinance('ALI=F', 'Алюминий', '$/т');
}

// ─── Источник 6: Цена пшеницы ────────────────────────────────────────────

async function fetchWheatPrice() {
  return fetchYahooFinance('ZW=F', 'Пшеница', '$/бушель');
}

module.exports = {
  fetchWorldBank,
  fetchNBT,
  getRatesHistory,
  fetchADB,
  fetchWTO,
  fetchCBR,
  fetchOilPrice,
  fetchAluminumPrice,
  fetchWheatPrice,
};
