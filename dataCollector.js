/**
 * dataCollector.js — сбор данных из Всемирного банка и НБТ
 * Чистый Node.js, без внешних зависимостей
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

const HISTORY_FILE = path.join(__dirname, 'rates-history.json');

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

module.exports = { fetchWorldBank, fetchNBT, getRatesHistory };
