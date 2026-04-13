'use strict';

/**
 * dataManager.js — гибридная система управления данными
 *
 * Приоритеты:
 *   1. Данные районных сотрудников (district_reports.json)
 *   2. Данные НБТ (курсы валют)
 *   3. Данные МВФ и Всемирного банка (макропоказатели)
 *   4. Кэш (при недоступности интернета)
 */

const fs   = require('fs');
const path = require('path');

const REPORTS_FILE    = path.join(__dirname, 'district_reports.json');
const SMART_CACHE_FILE = path.join(__dirname, 'smart_cache.json');
const CACHE_TTL_MS    = 6 * 60 * 60 * 1000; // 6 часов

// ─── Утилиты ────────────────────────────────────────────────────────────────

function readJSON(file, def) {
  try {
    if (!fs.existsSync(file)) return def;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch { return def; }
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function fmt(v, digits = 2) {
  if (v === null || v === undefined || isNaN(v)) return null;
  return Math.round(Number(v) * Math.pow(10, digits)) / Math.pow(10, digits);
}

// ─── Чтение районных отчётов ─────────────────────────────────────────────────

/**
 * Возвращает все отчёты, отсортированные по дате (новые первые).
 */
function getAllReports() {
  const raw = readJSON(REPORTS_FILE, []);
  return raw.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
}

/**
 * Отчёты за сегодня.
 */
function getTodayReports() {
  const today = todayStr();
  return getAllReports().filter(r => r.date === today);
}

/**
 * Уникальные районы, сдавшие отчёт за указанную дату.
 */
function getDistrictsForDate(dateStr) {
  const all = getAllReports().filter(r => r.date === dateStr);
  return [...new Set(all.map(r => r.district).filter(Boolean))];
}

/**
 * Среднее значение показателя цены по всем отчётам за дату.
 */
function avgPriceFromReports(product, reports) {
  const vals = reports
    .map(r => r.prices && r.prices[product])
    .filter(v => v !== undefined && v !== null && !isNaN(v))
    .map(Number);
  if (!vals.length) return null;
  return fmt(vals.reduce((s, v) => s + v, 0) / vals.length);
}

/**
 * Получить агрегированные цены из районных отчётов.
 * Возвращает объект { [product]: { value, source, date, districts } }
 */
function getPricesFromReports() {
  const today      = todayStr();
  const todayReps  = getTodayReports();
  const recentReps = todayReps.length
    ? todayReps
    : getAllReports().slice(0, 10); // последние 10 если сегодня нет

  const isToday = todayReps.length > 0;
  const dateUsed = isToday ? today : (recentReps[0]?.date || today);
  const districts = [...new Set(recentReps.map(r => r.district).filter(Boolean))];
  const districtLabel = districts.length
    ? districts.slice(0, 3).join(', ') + (districts.length > 3 ? ` (+${districts.length - 3})` : '')
    : 'МЭРиТ';
  const sourceLabel = `МЭРиТ (${districtLabel})`;

  const products = ['bread', 'flour', 'beef', 'rice', 'oil', 'sugar', 'milk', 'potato'];
  const result = {};
  for (const prod of products) {
    const val = avgPriceFromReports(prod, recentReps);
    if (val !== null) {
      result[prod] = {
        value:  val,
        source: sourceLabel,
        date:   isToday ? 'сегодня' : dateUsed,
        fresh:  isToday,
      };
    }
  }
  return result;
}

// ─── Агрегация метрик из отчётов ─────────────────────────────────────────────

function getMetricsFromReports() {
  const today     = todayStr();
  const todayReps = getTodayReports();
  const reps      = todayReps.length ? todayReps : getAllReports().slice(0, 20);
  const isToday   = todayReps.length > 0;

  function avgMetric(key) {
    const vals = reps
      .map(r => r.metrics && r.metrics[key])
      .filter(v => v !== undefined && v !== null && !isNaN(v))
      .map(Number);
    return vals.length ? fmt(vals.reduce((s, v) => s + v, 0) / vals.length) : null;
  }

  return {
    avg_wage:  avgMetric('avg_wage'),
    employed:  avgMetric('employed'),
    trade_vol: avgMetric('trade_vol'),
    date:      isToday ? 'сегодня' : reps[0]?.date,
    fresh:     isToday,
  };
}

// ─── Данные coverage ─────────────────────────────────────────────────────────

function getDataQuality() {
  const today          = todayStr();
  const todayDistricts = getDistrictsForDate(today);
  const total          = 32;
  return {
    districts_reported_today: todayDistricts.length,
    total_districts:          total,
    coverage:                 fmt((todayDistricts.length / total) * 100, 1),
    districts_list:           todayDistricts,
  };
}

// ─── Умный выбор данных ───────────────────────────────────────────────────────

/**
 * getSmartData(indicator) — возвращает наилучший доступный источник
 * для конкретного показателя.
 *
 * Поддерживаемые indicators:
 *   'prices'      — цены на продукты
 *   'usd_tjs'     — курс USD/TJS
 *   'eur_tjs'     — курс EUR/TJS
 *   'rub_tjs'     — курс RUB/TJS
 *   'gdp_growth'  — рост ВВП
 *   'inflation'   — инфляция
 *   'gov_budget'  — госбюджет / торговый оборот
 *   'all'         — все показатели
 */
async function getSmartData(indicator) {
  const { fetchNBT, fetchIMF } = require('./dataCollector');

  // Загружаем нужные источники параллельно
  const [nbtResult, imfInflation, imfGDP] = await Promise.allSettled([
    fetchNBT(),
    fetchIMF('PCPIPCH', 'TJK'),
    fetchIMF('NGDP_RPCH', 'TJK'),
  ]);

  const nbt = nbtResult.status === 'fulfilled' ? nbtResult.value : null;

  function lastImfValue(res) {
    if (res.status !== 'fulfilled') return null;
    const series = (res.value || []).filter(s => s.value !== null);
    return series.length ? series[series.length - 1] : null;
  }

  const imfInfl = lastImfValue(imfInflation);
  const imfGdp  = lastImfValue(imfGDP);

  switch (indicator) {
    case 'prices': {
      return getPricesFromReports();
    }

    case 'usd_tjs': {
      if (nbt?.rates?.USD) {
        return { value: nbt.rates.USD.rate, source: 'НБТ', date: nbt.date || 'сегодня', fresh: true };
      }
      const cached = readJSON(SMART_CACHE_FILE, {});
      return cached.usd_tjs || { value: null, source: 'кэш', date: '—', fresh: false };
    }

    case 'eur_tjs': {
      if (nbt?.rates?.EUR) {
        return { value: nbt.rates.EUR.rate, source: 'НБТ', date: nbt.date || 'сегодня', fresh: true };
      }
      const cached = readJSON(SMART_CACHE_FILE, {});
      return cached.eur_tjs || { value: null, source: 'кэш', date: '—', fresh: false };
    }

    case 'rub_tjs': {
      if (nbt?.rates?.RUB) {
        return { value: nbt.rates.RUB.rate, source: 'НБТ', date: nbt.date || 'сегодня', fresh: true };
      }
      const cached = readJSON(SMART_CACHE_FILE, {});
      return cached.rub_tjs || { value: null, source: 'кэш', date: '—', fresh: false };
    }

    case 'gdp_growth': {
      if (imfGdp) {
        return { value: imfGdp.value, source: `МВФ ${imfGdp.year}`, date: String(imfGdp.year), fresh: false };
      }
      const cached = readJSON(SMART_CACHE_FILE, {});
      return cached.gdp_growth || { value: null, source: 'кэш', date: '—', fresh: false };
    }

    case 'inflation': {
      // Комбинируем: МВФ как базис + районные данные как текущий сигнал
      const districtPrices = getPricesFromReports();
      const hasFresh = Object.values(districtPrices).some(p => p.fresh);

      if (imfInfl && hasFresh) {
        return {
          value:       imfInfl.value,
          source:      `МВФ ${imfInfl.year} + районные данные`,
          date:        'сегодня',
          fresh:       true,
          imf_base:    imfInfl.value,
          district_signal: true,
        };
      } else if (imfInfl) {
        return { value: imfInfl.value, source: `МВФ ${imfInfl.year}`, date: String(imfInfl.year), fresh: false };
      }
      const cached = readJSON(SMART_CACHE_FILE, {});
      return cached.inflation || { value: null, source: 'кэш', date: '—', fresh: false };
    }

    case 'gov_budget': {
      const metrics = getMetricsFromReports();
      if (metrics.trade_vol !== null) {
        return {
          value:  metrics.trade_vol,
          source: 'МЭРиТ (районные данные)',
          date:   metrics.date,
          fresh:  metrics.fresh,
        };
      }
      return { value: null, source: 'нет данных', date: '—', fresh: false };
    }

    case 'all':
    default:
      return mergeDataSources();
  }
}

// ─── Объединение источников ───────────────────────────────────────────────────

/**
 * mergeDataSources() — собирает данные из всех источников и возвращает
 * единый объект с пометкой источника для каждого показателя.
 */
async function mergeDataSources() {
  const { fetchNBT, fetchIMF, fetchWorldBank } = require('./dataCollector');

  const [nbtR, imfInflR, imfGdpR, imfDebtR, imfUrR, wbInflR] = await Promise.allSettled([
    fetchNBT(),
    fetchIMF('PCPIPCH',    'TJK'),
    fetchIMF('NGDP_RPCH',  'TJK'),
    fetchIMF('GGXWDG_NGDP','TJK'),
    fetchIMF('LUR',        'TJK'),
    fetchWorldBank('FP.CPI.TOTL.ZG'),
  ]);

  const nbt = nbtR.status === 'fulfilled' ? nbtR.value : null;

  function lastVal(res) {
    if (res.status !== 'fulfilled') return null;
    const arr = Array.isArray(res.value)
      ? res.value
      : (res.value?.series || []);
    const series = arr.filter(s => s.value !== null && s.value !== undefined);
    return series.length ? series[series.length - 1] : null;
  }

  const imfInfl = lastVal(imfInflR);
  const imfGdp  = lastVal(imfGdpR);
  const imfDebt = lastVal(imfDebtR);
  const imfUr   = lastVal(imfUrR);
  const wbInfl  = lastVal(wbInflR);

  // Районные данные
  const districtPrices  = getPricesFromReports();
  const districtMetrics = getMetricsFromReports();
  const dataQuality     = getDataQuality();

  // ── Цены (приоритет 1: районы) ──────────────────────────────────────────
  const PRODUCT_LABELS = {
    bread:   'Хлеб',
    flour:   'Мука',
    beef:    'Говядина',
    rice:    'Рис',
    oil:     'Масло растительное',
    sugar:   'Сахар',
    milk:    'Молоко',
    potato:  'Картофель',
  };

  const prices = {};
  for (const [key, label] of Object.entries(PRODUCT_LABELS)) {
    if (districtPrices[key]) {
      prices[key] = { ...districtPrices[key], label };
    } else {
      prices[key] = { value: null, source: 'нет данных', date: '—', fresh: false, label };
    }
  }

  // ── Макро ───────────────────────────────────────────────────────────────
  const macro = {};

  // GDP growth
  if (imfGdp) {
    macro.gdp_growth = {
      value:  fmt(imfGdp.value),
      source: `МВФ ${imfGdp.year}`,
      date:   String(imfGdp.year),
      unit:   '% г/г',
    };
  }

  // Inflation — приоритет: МВФ + сигнал от районов, или только МВФ, или Всемирный банк
  const hasFreshDistricts = dataQuality.districts_reported_today > 0;
  if (imfInfl) {
    macro.inflation = {
      value:  fmt(imfInfl.value),
      source: hasFreshDistricts
        ? `МВФ ${imfInfl.year} + районные данные`
        : `МВФ ${imfInfl.year}`,
      date:   hasFreshDistricts ? 'сегодня' : String(imfInfl.year),
      unit:   '% г/г',
      imf_value: fmt(imfInfl.value),
      district_fresh: hasFreshDistricts,
    };
  } else if (wbInfl) {
    macro.inflation = {
      value:  fmt(wbInfl.value),
      source: `Всемирный банк ${wbInfl.year}`,
      date:   String(wbInfl.year),
      unit:   '% г/г',
    };
  }

  // Gov debt
  if (imfDebt) {
    macro.gov_debt = {
      value:  fmt(imfDebt.value),
      source: `МВФ ${imfDebt.year}`,
      date:   String(imfDebt.year),
      unit:   '% ВВП',
    };
  }

  // Unemployment
  if (imfUr) {
    macro.unemployment = {
      value:  fmt(imfUr.value),
      source: `МВФ ${imfUr.year}`,
      date:   String(imfUr.year),
      unit:   '% раб. силы',
    };
  }

  // Exchange rates (приоритет 2: НБТ)
  if (nbt?.rates?.USD) {
    macro.usd_tjs = {
      value:  fmt(nbt.rates.USD.rate),
      source: 'НБТ',
      date:   nbt.date || 'сегодня',
      unit:   'TJS за 1 USD',
    };
  }
  if (nbt?.rates?.EUR) {
    macro.eur_tjs = {
      value:  fmt(nbt.rates.EUR.rate),
      source: 'НБТ',
      date:   nbt.date || 'сегодня',
      unit:   'TJS за 1 EUR',
    };
  }
  if (nbt?.rates?.RUB) {
    macro.rub_tjs = {
      value:  fmt(nbt.rates.RUB.rate),
      source: 'НБТ',
      date:   nbt.date || 'сегодня',
      unit:   'TJS за 100 RUB',
    };
  }

  // Trade volume (из районных отчётов)
  if (districtMetrics.trade_vol !== null) {
    macro.trade_vol = {
      value:  fmt(districtMetrics.trade_vol),
      source: 'МЭРиТ (районные данные)',
      date:   districtMetrics.date || 'сегодня',
      unit:   'млн TJS',
    };
  }

  // Avg wage
  if (districtMetrics.avg_wage !== null) {
    macro.avg_wage = {
      value:  fmt(districtMetrics.avg_wage),
      source: 'МЭРиТ (районные данные)',
      date:   districtMetrics.date || 'сегодня',
      unit:   'TJS',
    };
  }

  // ── Сравнение ───────────────────────────────────────────────────────────
  const comparison = {};

  // Инфляция: районный сигнал vs МВФ (приблизительно по динамике цен хлеба)
  if (districtPrices.bread && imfInfl) {
    comparison.ministry_vs_imf = {
      note:     'Ценовой мониторинг МЭРиТ vs базовая инфляция МВФ',
      imf_inflation: fmt(imfInfl.value),
      imf_year:      imfInfl.year,
      district_bread_price: districtPrices.bread.value,
      district_date:        districtPrices.bread.date,
      available: true,
    };
  } else {
    comparison.ministry_vs_imf = { available: false, note: 'Недостаточно данных для сравнения' };
  }

  if (nbt?.rates?.USD && districtMetrics.trade_vol !== null) {
    comparison.ministry_vs_nbt = {
      note:          'Торговый оборот МЭРиТ vs курс НБТ',
      usd_rate:       fmt(nbt.rates.USD.rate),
      trade_vol:      fmt(districtMetrics.trade_vol),
      district_date:  districtMetrics.date,
      available: true,
    };
  } else {
    comparison.ministry_vs_nbt = { available: false, note: 'Недостаточно данных для сравнения' };
  }

  // ── Итог ────────────────────────────────────────────────────────────────
  const result = {
    prices,
    macro,
    comparison,
    data_quality: dataQuality,
    sources_used: {
      district_reports: dataQuality.districts_reported_today > 0,
      nbt:              !!nbt,
      imf:              !!(imfInfl || imfGdp),
      worldbank:        !!wbInfl,
    },
    generated_at: new Date().toISOString(),
  };

  // Сохраняем в кэш
  try {
    fs.writeFileSync(SMART_CACHE_FILE, JSON.stringify(result, null, 2), 'utf8');
  } catch {}

  return result;
}

// ─── Кэш-fallback при офлайн ─────────────────────────────────────────────────

/**
 * Возвращает кэшированные данные если они есть и не старше TTL.
 * Если данных нет — возвращает null.
 */
function getCachedSmartData() {
  const cached = readJSON(SMART_CACHE_FILE, null);
  if (!cached) return null;
  const age = Date.now() - new Date(cached.generated_at || 0).getTime();
  if (age > CACHE_TTL_MS) return { ...cached, stale: true, from_cache: true };
  return { ...cached, from_cache: true };
}

// ─── Экспорт ─────────────────────────────────────────────────────────────────

module.exports = {
  getSmartData,
  mergeDataSources,
  getPricesFromReports,
  getMetricsFromReports,
  getDataQuality,
  getAllReports,
  getTodayReports,
  getCachedSmartData,
};
