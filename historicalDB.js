'use strict';

/**
 * historicalDB.js — историческая база данных МЭРиТ за 10 лет (2015–2024)
 *
 * Источники:
 *   - МЭРиТ РТ (ВВП, оценки)
 *   - Агентство по статистике РТ (инфляция, торговля)
 *   - НБТ (курсы валют, переводы мигрантов)
 */

const fs   = require('fs');
const path = require('path');

const DATA_DIR      = path.join(__dirname, 'data');
const MINISTRY_FILE = path.join(DATA_DIR, 'ministry_gdp_model.json');

// ─── Чтение JSON файлов ──────────────────────────────────────────────────────

function readData(filename) {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA_DIR, filename), 'utf8'));
  } catch (e) {
    console.error(`[historicalDB] Ошибка чтения ${filename}:`, e.message);
    return [];
  }
}

/** Данные модели МЭРиТ 1997–2024 (ministry_gdp_model.json) */
function getMinistryData() {
  try { return JSON.parse(fs.readFileSync(MINISTRY_FILE, 'utf8')); }
  catch (e) { return null; }
}

// ─── Основные геттеры ────────────────────────────────────────────────────────

/** ВВП Таджикистана — приоритет МЭРиТ 1997–2024, fallback → gdp_history.json */
function getGDPHistory() {
  const m = getMinistryData();
  if (m && m.gdp && m.gdp.length) {
    return m.gdp.map(r => ({
      year:               r.year,
      gdp_bln_somoni:     r.gdp_bln_somoni,
      gdp_growth:         r.gdp_growth,
      gdp_per_capita_usd: r.gdp_per_capita_usd,
      population_thou:    r.population_thou,
      source:             'МЭРиТ РТ',
    }));
  }
  return readData('gdp_history.json');
}

/** Инфляция (ИПЦ) — приоритет МЭРиТ 1997–2024, fallback → inflation_history.json */
function getInflationHistory() {
  const m = getMinistryData();
  if (m && m.monetary && m.monetary.length) {
    return m.monetary.map(r => ({
      year:           r.year,
      cpi:            r.cpi_avg_annual,                                         // 100-based (105.8 = 5.8%)
      food_inflation: r.cpi_food != null ? Math.round((r.cpi_food - 100) * 10) / 10 : null, // % (6.8)
      source:         'МЭРиТ РТ',
    }));
  }
  return readData('inflation_history.json');
}

/**
 * История курсов валют 2015–2024
 * @param {string} [currency] — 'usd'|'eur'|'rub' или не передавать для всех
 */
function getExchangeRateHistory(currency) {
  const data = readData('exchange_rates_history.json');
  if (!currency) return data;
  const key = currency.toLowerCase() + '_tjs';
  return data.map(r => ({
    year:   r.year,
    rate:   r[key] ?? null,
    source: r.source,
  })).filter(r => r.rate !== null);
}

/** Переводы мигрантов 2015–2024 */
function getRemittancesHistory() {
  return readData('remittances_history.json');
}

/** Внешняя торговля — приоритет МЭРиТ 1997–2024, fallback → trade_history.json */
function getTradeHistory() {
  const m = getMinistryData();
  if (m && m.trade && m.trade.length) {
    return m.trade.map(r => ({
      year:            r.year,
      export_mln_usd:  r.export_mln_usd,
      import_mln_usd:  r.import_mln_usd,
      balance_mln_usd: r.balance_mln_usd,
      source:          'МЭРиТ РТ',
    }));
  }
  return readData('trade_history.json');
}

/** Все исторические данные одним объектом */
function getAllHistory() {
  return {
    gdp:           getGDPHistory(),
    inflation:     getInflationHistory(),
    exchange_rates: readData('exchange_rates_history.json'),
    remittances:   getRemittancesHistory(),
    trade:         getTradeHistory(),
    meta: {
      period:      '2015–2024',
      years:       10,
      sources:     ['МЭРиТ РТ', 'Агентство по статистике РТ', 'НБТ'],
      generated_at: new Date().toISOString(),
    },
  };
}

// ─── Данные для прогнозирования ──────────────────────────────────────────────

/**
 * getDataForForecasting(indicator) — возвращает числовой массив для ARIMA/Prophet
 *
 * Поддерживаемые indicator:
 *   'gdp_growth'    — темп роста ВВП, %
 *   'gdp_bln'       — ВВП в млрд сомони
 *   'gdp_per_capita'— ВВП на душу, USD
 *   'inflation'     — ИПЦ (%)
 *   'food_inflation'— продовольственная инфляция (%)
 *   'usd_tjs'       — курс USD/TJS
 *   'eur_tjs'       — курс EUR/TJS
 *   'rub_tjs'       — курс RUB/TJS
 *   'remittances'   — переводы мигрантов, млн USD
 *   'remittances_pct_gdp' — переводы % ВВП
 *   'export'        — экспорт, млн USD
 *   'import'        — импорт, млн USD
 *   'trade_balance' — торговый баланс, млн USD
 */
function getDataForForecasting(indicator) {
  const ind = (indicator || '').toLowerCase();

  switch (ind) {
    case 'gdp_growth':
      return getGDPHistory().map(r => r.gdp_growth);

    case 'gdp_bln':
    case 'gdp':
      return getGDPHistory().map(r => r.gdp_bln_somoni);

    case 'gdp_per_capita':
      return getGDPHistory().map(r => r.gdp_per_capita_usd);

    case 'inflation':
    case 'cpi':
      return getInflationHistory().map(r => r.cpi);

    case 'food_inflation':
      return getInflationHistory().map(r => r.food_inflation);

    case 'usd_tjs':
    case 'usd':
      return readData('exchange_rates_history.json').map(r => r.usd_tjs);

    case 'eur_tjs':
    case 'eur':
      return readData('exchange_rates_history.json').map(r => r.eur_tjs);

    case 'rub_tjs':
    case 'rub':
      return readData('exchange_rates_history.json').map(r => r.rub_tjs);

    case 'remittances':
      return getRemittancesHistory().map(r => r.amount_mln_usd);

    case 'remittances_pct_gdp':
      return getRemittancesHistory().map(r => r.pct_gdp);

    case 'export':
      return getTradeHistory().map(r => r.export_mln_usd);

    case 'import':
      return getTradeHistory().map(r => r.import_mln_usd);

    case 'trade_balance':
      return getTradeHistory().map(r => r.balance_mln_usd);

    default:
      return [];
  }
}

// ─── Вспомогательные аналитические функции ───────────────────────────────────

/**
 * getYearRange(from, to) — срез данных по диапазону лет
 */
function getYearRange(data, from, to) {
  return data.filter(r => r.year >= from && r.year <= to);
}

/**
 * getLastYear() — данные последнего доступного года (2024)
 */
function getLastYear() {
  const gdp    = getGDPHistory();
  const infl   = getInflationHistory();
  const fx     = readData('exchange_rates_history.json');
  const remit  = getRemittancesHistory();
  const trade  = getTradeHistory();

  const last = arr => arr[arr.length - 1] || null;

  return {
    year:              2024,
    gdp_growth:        last(gdp)?.gdp_growth    ?? 8.0,
    gdp_bln_somoni:    last(gdp)?.gdp_bln_somoni ?? 128.0,
    gdp_per_capita_usd: last(gdp)?.gdp_per_capita_usd ?? 1300,
    inflation:         last(infl)?.cpi                                          ?? 105.0, // 100-based
    food_inflation:    last(infl)?.food_inflation                               ?? 5.5,
    usd_tjs:           last(fx)?.usd_tjs         ?? 10.92,
    eur_tjs:           last(fx)?.eur_tjs         ?? 11.85,
    rub_tjs:           last(fx)?.rub_tjs         ?? 0.118,
    remittances_mln:   last(remit)?.amount_mln_usd ?? 3100,
    remittances_pct_gdp: last(remit)?.pct_gdp    ?? 35.0,
    export_mln:        last(trade)?.export_mln_usd ?? 1950,
    import_mln:        last(trade)?.import_mln_usd ?? 4850,
    trade_balance:     last(trade)?.balance_mln_usd ?? -2900,
  };
}

/**
 * getCrisisContext(scenario) — исторический контекст похожих шоков
 * Используется стресс-тестом для сравнения с прошлым
 */
function getCrisisContext(scenario) {
  const gdp   = getGDPHistory();
  const fx    = readData('exchange_rates_history.json');
  const remit = getRemittancesHistory();

  switch (scenario) {
    case 'oil': {
      // Нефтяной шок 2015–2016: нефть упала с ~$110 до ~$30
      const y2015 = gdp.find(r => r.year === 2015);
      const y2016 = gdp.find(r => r.year === 2016);
      const fx2015 = fx.find(r => r.year === 2015);
      const fx2016 = fx.find(r => r.year === 2016);
      const r2015  = remit.find(r => r.year === 2015);
      const r2016  = remit.find(r => r.year === 2016);
      return {
        period:      '2015–2016',
        event:       'Падение нефтяных цен с $110 до $30/барр.',
        gdp_impact:  [y2015?.gdp_growth, y2016?.gdp_growth],
        fx_impact:   [fx2015?.usd_tjs, fx2016?.usd_tjs],
        remit_impact:[r2015?.amount_mln_usd, r2016?.amount_mln_usd],
        lesson:      'ВВП замедлился до 6.0–6.9%, курс TJS ослаб с 6.52 до 7.84 (-20%), переводы сократились на 16%',
      };
    }

    case 'remittances': {
      // COVID 2020: переводы упали на 17%
      const y2019 = gdp.find(r => r.year === 2019);
      const y2020 = gdp.find(r => r.year === 2020);
      const r2019 = remit.find(r => r.year === 2019);
      const r2020 = remit.find(r => r.year === 2020);
      return {
        period:      '2019–2020',
        event:       'COVID-19: закрытие границ, массовое возвращение мигрантов',
        gdp_impact:  [y2019?.gdp_growth, y2020?.gdp_growth],
        remit_before: r2019?.amount_mln_usd,
        remit_after:  r2020?.amount_mln_usd,
        remit_drop_pct: r2019 && r2020
          ? Math.round(((r2020.amount_mln_usd - r2019.amount_mln_usd) / r2019.amount_mln_usd) * 100)
          : -17,
        lesson:      'Рост ВВП снизился с 7.5% до 4.5%, переводы упали на $394 млн (-16.8%)',
      };
    }

    case 'crop': {
      // Засуха 2018: продовольственная инфляция снизилась, но уязвимость высокая
      const infl = getInflationHistory();
      const i2017 = infl.find(r => r.year === 2017);
      const i2018 = infl.find(r => r.year === 2018);
      return {
        period:   '2017–2018',
        event:    'Волатильность продовольственных цен',
        cpi_2017: i2017?.cpi,
        cpi_2018: i2018?.cpi,
        food_2017: i2017?.food_inflation,
        food_2018: i2018?.food_inflation,
        lesson:   'Продовольственная инфляция снизилась с 8.2% до 4.5% при стабильной монетарной политике НБТ',
      };
    }

    case 'hydro': {
      // Гидроэнергетика — нет прямого кризиса, но контекст экспорта
      const trade = getTradeHistory();
      const t2020 = trade.find(r => r.year === 2020);
      const t2021 = trade.find(r => r.year === 2021);
      return {
        period:        '2020–2021',
        event:         'Восстановление экспорта после COVID',
        export_2020:   t2020?.export_mln_usd,
        export_2021:   t2021?.export_mln_usd,
        export_growth: t2020 && t2021
          ? Math.round(((t2021.export_mln_usd - t2020.export_mln_usd) / t2020.export_mln_usd) * 100)
          : 37,
        lesson: 'Экспорт вырос на 37% в 2021 при восстановлении мирового спроса',
      };
    }

    default:
      return null;
  }
}

/**
 * getVARData() — подготовленные данные для VAR-модели (4 ряда × 10 лет)
 */
function getVARData() {
  return {
    gdp:           getDataForForecasting('gdp_growth'),
    inflation:     getDataForForecasting('food_inflation'),
    exchange_rate: getDataForForecasting('usd_tjs'),
    remittances:   getDataForForecasting('remittances').map(v => v / 1000), // в млрд USD
    years:         getGDPHistory().map(r => r.year),
  };
}

/**
 * getCorrelationData() — матрица для корреляционного анализа
 * Возвращает именованные числовые ряды
 */
function getCorrelationData() {
  const years = getGDPHistory().map(r => r.year);
  return {
    years,
    series: {
      gdp_growth:    getDataForForecasting('gdp_growth'),
      inflation:     getDataForForecasting('food_inflation'),
      usd_tjs:       getDataForForecasting('usd_tjs'),
      remittances:   getDataForForecasting('remittances').map(v => v / 1000),
      export:        getDataForForecasting('export').map(v => v / 1000),
      trade_balance: getDataForForecasting('trade_balance').map(v => v / 1000),
    },
    labels: {
      gdp_growth:    'Рост ВВП (%)',
      inflation:     'Продинфляция (%)',
      usd_tjs:       'USD/TJS',
      remittances:   'Переводы (млрд $)',
      export:        'Экспорт (млрд $)',
      trade_balance: 'Торг. баланс (млрд $)',
    },
  };
}

/**
 * getCGECalibration() — параметры для калибровки CGE модели на реальных данных
 */
function getCGECalibration() {
  const last    = getLastYear();
  const remit   = getRemittancesHistory();
  const trade   = getTradeHistory();
  const gdp     = getGDPHistory();

  // Среднее значение remit/GDP за 10 лет
  const avgRemitPct = remit.reduce((s, r) => s + r.pct_gdp, 0) / remit.length;

  // Среднее соотношение экспорт/ВВП (приблизительно, в USD)
  const avgGDPusd = gdp.reduce((s, r) => s + r.gdp_per_capita_usd, 0) / gdp.length * 10; // ~10 млн чел
  const avgExportPct = trade.reduce((s, r) => s + (r.export_mln_usd / (avgGDPusd || 1)) * 100, 0) / trade.length;
  const avgImportPct = trade.reduce((s, r) => s + (r.import_mln_usd / (avgGDPusd || 1)) * 100, 0) / trade.length;

  return {
    remit_gdp:   Math.round(avgRemitPct) / 100,       // доля переводов в ВВП
    export_gdp:  Math.round(avgExportPct * 10) / 1000,// доля экспорта
    import_gdp:  Math.round(avgImportPct * 10) / 1000,// доля импорта
    last_year:   last,
    calibrated_at: new Date().toISOString(),
  };
}

// ─── Экспорт ─────────────────────────────────────────────────────────────────

module.exports = {
  getGDPHistory,
  getInflationHistory,
  getExchangeRateHistory,
  getRemittancesHistory,
  getTradeHistory,
  getAllHistory,
  getDataForForecasting,
  getLastYear,
  getCrisisContext,
  getVARData,
  getCorrelationData,
  getCGECalibration,
  getYearRange,
};
