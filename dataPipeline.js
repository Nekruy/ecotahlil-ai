/**
 * dataPipeline.js — Data Science pipeline для макроэкономики Таджикистана
 * Модули: корреляции, nowcasting ВВП, ETL, дайджест, индекс здоровья
 * Чистый Node.js, без внешних зависимостей
 */

'use strict';

const fs   = require('fs');
const path = require('path');

const PIPELINE_FILE   = path.join(__dirname, 'pipeline_data.json');
const DIGEST_FILE     = path.join(__dirname, 'morning_digest.json');
const LOG_FILE        = path.join(__dirname, 'pipeline_log.json');
const DASHBOARD_CACHE = path.join(__dirname, 'dashboard_cache.json');

// ─── Утилиты ─────────────────────────────────────────────────────────────────

function readJSON(file, fallback = null) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {}
  return fallback;
}

function writeJSON(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
    return true;
  } catch (e) {
    console.error(`[pipeline] Ошибка записи ${path.basename(file)}:`, e.message);
    return false;
  }
}

function pipelineLog(level, source, message, details = {}) {
  const log = readJSON(LOG_FILE, []);
  const entry = {
    ts:      new Date().toISOString(),
    level,   // 'info' | 'warn' | 'error'
    source,
    message,
    ...details,
  };
  log.push(entry);
  // Хранить последние 500 записей
  const trimmed = log.slice(-500);
  writeJSON(LOG_FILE, trimmed);
  const prefix = level === 'error' ? '❌' : level === 'warn' ? '⚠️' : '✅';
  console.log(`[pipeline] ${prefix} [${source}] ${message}`);
}

// ─── МОДУЛЬ 1: Корреляционный анализ ─────────────────────────────────────────

/**
 * Вычисляет коэффициент Пирсона для двух массивов чисел.
 * Массивы должны быть одной длины, NaN/null пропускаются попарно.
 */
function pearson(xArr, yArr) {
  const pairs = xArr.map((x, i) => [x, yArr[i]])
    .filter(([x, y]) => x != null && y != null && !isNaN(x) && !isNaN(y));

  const n = pairs.length;
  if (n < 3) return null; // недостаточно данных

  const xs = pairs.map(p => p[0]);
  const ys = pairs.map(p => p[1]);

  const meanX = xs.reduce((a, b) => a + b, 0) / n;
  const meanY = ys.reduce((a, b) => a + b, 0) / n;

  let num = 0, denX = 0, denY = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i] - meanX;
    const dy = ys[i] - meanY;
    num  += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }

  const den = Math.sqrt(denX * denY);
  if (den === 0) return 0;
  return Math.round((num / den) * 1000) / 1000;
}

/**
 * Интерпретация коэффициента Пирсона.
 */
function interpretCorrelation(r) {
  if (r === null) return { label: 'Нет данных', color: '#9ca3af', strength: 'none' };
  const abs = Math.abs(r);
  if (abs >= 0.7) return r > 0
    ? { label: 'Сильная положительная', color: '#15803d', strength: 'strong_pos' }
    : { label: 'Сильная отрицательная', color: '#b91c1c', strength: 'strong_neg' };
  if (abs >= 0.3) return r > 0
    ? { label: 'Умеренная положительная', color: '#0284c7', strength: 'moderate_pos' }
    : { label: 'Умеренная отрицательная', color: '#ea580c', strength: 'moderate_neg' };
  return { label: 'Слабая связь', color: '#6b7280', strength: 'weak' };
}

/**
 * Выравнивание двух временных рядов по датам.
 * Каждый ряд — массив { date: 'YYYY-MM-DD', value: number }
 */
function alignSeries(seriesA, seriesB) {
  const mapB = new Map(seriesB.map(d => [d.date, d.value]));
  const aligned = seriesA
    .filter(d => mapB.has(d.date))
    .map(d => ({ date: d.date, a: d.value, b: mapB.get(d.date) }));
  return {
    xs: aligned.map(d => d.a),
    ys: aligned.map(d => d.b),
    n:  aligned.length,
  };
}

/**
 * correlationMatrix(datasets)
 *
 * @param {Object} datasets — { name: [{ date, value }] }
 * @returns матрица корреляций с интерпретацией
 */
function correlationMatrix(datasets) {
  const keys = Object.keys(datasets);
  const matrix = {};
  const pairs  = [];

  for (const kA of keys) {
    matrix[kA] = {};
    for (const kB of keys) {
      if (kA === kB) {
        matrix[kA][kB] = { r: 1, ...interpretCorrelation(1) };
        continue;
      }
      const { xs, ys, n } = alignSeries(datasets[kA], datasets[kB]);
      const r = pearson(xs, ys);
      const interp = interpretCorrelation(r);
      matrix[kA][kB] = { r, n, ...interp };

      // Уникальные пары для списка
      if (kA < kB && r !== null) {
        pairs.push({ a: kA, b: kB, r, n, ...interp });
      }
    }
  }

  // Топ-5 сильных корреляций (по |r|)
  pairs.sort((a, b) => Math.abs(b.r) - Math.abs(a.r));
  const topPairs = pairs.slice(0, 5);

  return {
    keys,
    matrix,
    topPairs,
    computed: new Date().toISOString(),
  };
}

// ─── МОДУЛЬ 2: Nowcasting ВВП ─────────────────────────────────────────────────

/**
 * nowcastGDP(highFrequencyData)
 *
 * Оценивает рост ВВП до публикации официальной статистики.
 * Метод: взвешенная сумма нормализованных индикаторов.
 *
 * Веса (по МВФ для Таджикистана):
 *   переводы 30%, торговля 25%, курс 25%, цены 20%
 *
 * @param {Object} hf — {
 *   remittances:  { current, prev }   // переводы ($млн)
 *   trade:        { current, prev }   // товарооборот ($млн)
 *   fxRate:       { current, prev }   // TJS/USD
 *   cpi:          { current, prev }   // индекс потребцен
 *   oil:          { current, prev }   // цена нефти
 *   aluminum:     { current, prev }   // цена алюминия
 * }
 */
function nowcastGDP(highFrequencyData) {
  const hf = highFrequencyData || {};

  const WEIGHTS = {
    remittances: 0.30,
    trade:       0.25,
    fxRate:      0.25,
    prices:      0.20,
  };

  // Вычисляем вклад каждого индикатора (YoY % изменение)
  function yoy(cur, prev) {
    if (!cur || !prev || prev === 0) return null;
    return ((cur - prev) / Math.abs(prev)) * 100;
  }

  const components = {};

  // Переводы мигрантов: рост переводов → рост потребления → рост ВВП
  const remGrowth = yoy(hf.remittances?.current, hf.remittances?.prev);
  components.remittances = {
    growth:      remGrowth,
    weight:      WEIGHTS.remittances,
    contribution: remGrowth !== null ? remGrowth * WEIGHTS.remittances : null,
    label:       'Переводы мигрантов',
    note:        '~25-30% ВВП Таджикистана',
  };

  // Торговля: рост экспорта/торгооборота → рост ВВП
  const tradeGrowth = yoy(hf.trade?.current, hf.trade?.prev);
  components.trade = {
    growth:       tradeGrowth,
    weight:       WEIGHTS.trade,
    contribution: tradeGrowth !== null ? tradeGrowth * WEIGHTS.trade : null,
    label:        'Объём торговли',
    note:         'Экспорт + импорт $млн',
  };

  // Курс: укрепление TJS (рост TJS/USD) → снижение инфляции → рост потребления
  // Укрепление сомони (меньше сомони за USD) — позитив для ВВП
  const fxGrowth = hf.fxRate?.current && hf.fxRate?.prev
    ? ((hf.fxRate.prev - hf.fxRate.current) / hf.fxRate.prev) * 100  // инвертируем
    : null;
  components.fxRate = {
    growth:       fxGrowth,
    weight:       WEIGHTS.fxRate,
    contribution: fxGrowth !== null ? fxGrowth * WEIGHTS.fxRate : null,
    label:        'Стабильность курса TJS',
    note:         'Укрепление TJS позитивно для реального ВВП',
  };

  // Цены: снижение инфляции → рост реального ВВП
  // Рост CPI = плохо → инвертируем
  const cpiGrowth = yoy(hf.cpi?.current, hf.cpi?.prev);
  components.prices = {
    growth:       cpiGrowth !== null ? -cpiGrowth : null, // инвертируем
    weight:       WEIGHTS.prices,
    contribution: cpiGrowth !== null ? (-cpiGrowth) * WEIGHTS.prices : null,
    label:        'Инфляция (инв.)',
    note:         'Снижение инфляции повышает реальный ВВП',
  };

  // Считаем итоговый nowcast
  const contributions = Object.values(components)
    .map(c => c.contribution)
    .filter(c => c !== null);

  // Корректируем вес под доступные данные
  const availableWeight = Object.values(components)
    .filter(c => c.contribution !== null)
    .reduce((s, c) => s + c.weight, 0);

  let gdpEstimate = null;
  if (contributions.length > 0 && availableWeight > 0) {
    const rawSum = contributions.reduce((a, b) => a + b, 0);
    // Нормализуем под типичный диапазон ВВП Таджикистана (5-10% роста)
    gdpEstimate = Math.round((rawSum / availableWeight) * 10) / 10;
    // Зажимаем в реалистичный диапазон
    gdpEstimate = Math.max(-15, Math.min(20, gdpEstimate));
  }

  // Учитываем внешние commodity факторы как корректировку
  let commodityAdjustment = 0;
  if (hf.oil?.current && hf.oil?.prev) {
    const oilChange = ((hf.oil.current - hf.oil.prev) / hf.oil.prev) * 100;
    commodityAdjustment += oilChange * 0.08; // нефть → переводы из РФ
  }
  if (hf.aluminum?.current && hf.aluminum?.prev) {
    const aluChange = ((hf.aluminum.current - hf.aluminum.prev) / hf.aluminum.prev) * 100;
    commodityAdjustment += aluChange * 0.05; // алюминий → ТАЛКО
  }
  commodityAdjustment = Math.round(commodityAdjustment * 10) / 10;

  const finalEstimate = gdpEstimate !== null
    ? Math.round((gdpEstimate + commodityAdjustment) * 10) / 10
    : null;

  // Уровень уверенности — больше доступных данных = выше уверенность
  const confidence = Math.round((availableWeight / 1.0) * 100);

  return {
    gdpGrowthEstimate: finalEstimate,
    gdpGrowthBase:     gdpEstimate,
    commodityAdjustment,
    confidence,
    dataPoints:        contributions.length,
    components,
    interpretation:    interpretGDP(finalEstimate),
    method:            'Взвешенная сумма HF-индикаторов (МВФ-подход)',
    computed:          new Date().toISOString(),
  };
}

function interpretGDP(estimate) {
  if (estimate === null) return { label: 'Нет данных', color: '#6b7280' };
  if (estimate > 7)  return { label: 'Сильный рост', color: '#15803d' };
  if (estimate > 4)  return { label: 'Умеренный рост', color: '#0284c7' };
  if (estimate > 0)  return { label: 'Слабый рост', color: '#d97706' };
  if (estimate > -2) return { label: 'Стагнация', color: '#ea580c' };
  return { label: 'Рецессия', color: '#b91c1c' };
}

// ─── МОДУЛЬ 5: Индекс экономического здоровья ────────────────────────────────
// (объявлен до ETL, т.к. ETL его использует)

/**
 * economicHealthIndex(indicators)
 *
 * @param {Object} ind — {
 *   inflation:    число (% инфляция)
 *   fxStdDev:     число (std отклонение TJS/USD за 30 дней)
 *   fxCurrent:    число (текущий TJS/USD)
 *   fxPrev:       число (TJS/USD месяц назад)
 *   remittancesGrowth: число (% YoY изменение переводов)
 *   foodPriceIdx: число (индекс продовольственных цен, 100 = база)
 *   oilPrice:     число
 *   oilPrev:      число
 *   aluminumPrice: число
 *   aluminumPrev:  число
 *   wheatPrice:    число
 *   wheatPrev:     число
 * }
 */
function economicHealthIndex(indicators = {}) {
  const scores = {};

  // 1. Инфляция (вес 25%): 100 = <5%, 0 = >20%, линейно
  const inf = indicators.inflation;
  scores.inflation = {
    weight: 0.25,
    label:  'Инфляция',
    value:  inf,
    score:  inf == null ? 50
      : inf <= 5  ? 100
      : inf >= 20 ? 0
      : Math.round(100 - ((inf - 5) / 15) * 100),
  };

  // 2. Стабильность курса TJS (вес 20%)
  // Используем % изменение за месяц: 0% = 100 баллов, ≥10% = 0
  const fxChange = indicators.fxCurrent && indicators.fxPrev
    ? Math.abs((indicators.fxCurrent - indicators.fxPrev) / indicators.fxPrev) * 100
    : null;
  scores.fxStability = {
    weight:   0.20,
    label:    'Стабильность курса',
    value:    fxChange,
    valueStr: fxChange != null ? fxChange.toFixed(2) + '% изм./мес.' : null,
    score:    fxChange == null ? 50
      : fxChange <= 0.5 ? 100
      : fxChange >= 10  ? 0
      : Math.round(100 - (fxChange / 10) * 100),
  };

  // 3. Переводы мигрантов (вес 20%): рост +10% = 100, падение −30% = 0
  const remGrowth = indicators.remittancesGrowth;
  scores.remittances = {
    weight:  0.20,
    label:   'Переводы мигрантов',
    value:   remGrowth,
    score:   remGrowth == null ? 50
      : remGrowth >= 10  ? 100
      : remGrowth <= -30 ? 0
      : Math.round(50 + (remGrowth / 40) * 50),
  };

  // 4. Цены на продукты (вес 20%): foodPriceIdx 100 = хорошо, 130+ = плохо
  const fpi = indicators.foodPriceIdx;
  scores.foodPrices = {
    weight: 0.20,
    label:  'Цены на продовольствие',
    value:  fpi,
    score:  fpi == null ? 50
      : fpi <= 100 ? 100
      : fpi >= 130 ? 0
      : Math.round(100 - ((fpi - 100) / 30) * 100),
  };

  // 5. Внешние товарные рынки (вес 15%)
  // Oil: рост → хорошо для переводов из РФ (но плохо для инфляции)
  // Aluminum: рост → хорошо для ТАЛКО
  // Wheat: рост → плохо для продбезопасности
  // Итого: нефть (+), алюминий (+), пшеница (-)
  let commodityScore = 50;
  let commodityParts = 0;

  if (indicators.oilPrice && indicators.oilPrev) {
    const oilChg = (indicators.oilPrice - indicators.oilPrev) / indicators.oilPrev;
    commodityScore += oilChg * 200; // умеренное влияние
    commodityParts++;
  }
  if (indicators.aluminumPrice && indicators.aluminumPrev) {
    const aluChg = (indicators.aluminumPrice - indicators.aluminumPrev) / indicators.aluminumPrev;
    commodityScore += aluChg * 150;
    commodityParts++;
  }
  if (indicators.wheatPrice && indicators.wheatPrev) {
    const wheatChg = (indicators.wheatPrice - indicators.wheatPrev) / indicators.wheatPrev;
    commodityScore -= wheatChg * 200; // пшеница — негатив при росте
    commodityParts++;
  }

  scores.commodities = {
    weight: 0.15,
    label:  'Товарные рынки',
    score:  Math.round(Math.max(0, Math.min(100, commodityScore))),
  };

  // Итоговый индекс
  const total = Object.values(scores).reduce(
    (sum, s) => sum + s.score * s.weight, 0
  );
  const index = Math.round(total);

  // Уровень
  let level, levelColor, levelIcon;
  if (index >= 70) {
    level = 'Здоровая';        levelColor = '#15803d'; levelIcon = '✅';
  } else if (index >= 50) {
    level = 'Умеренный риск';  levelColor = '#0284c7'; levelIcon = '⚠️';
  } else if (index >= 30) {
    level = 'Уязвимая';        levelColor = '#ea580c'; levelIcon = '🔶';
  } else {
    level = 'Кризис';          levelColor = '#b91c1c'; levelIcon = '🚨';
  }

  return {
    index,
    level,
    levelColor,
    levelIcon,
    scores,
    computed: new Date().toISOString(),
  };
}

// ─── МОДУЛЬ 4: Утренний дайджест ─────────────────────────────────────────────

/**
 * generateMorningDigest(pipelineData)
 * Формирует структурированный дайджест последних изменений.
 */
function generateMorningDigest(pipelineData = {}) {
  const pd  = pipelineData;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  // Алерты — критические изменения
  const alerts = [];

  // Инфляция > 10%
  if (pd.inflation > 10) {
    alerts.push({
      type:    'inflation',
      level:   'warning',
      icon:    '📈',
      message: `Инфляция ${pd.inflation?.toFixed(1)}% — выше порога 10%`,
    });
  }

  // Аномалия курса
  if (pd.fxChange && Math.abs(pd.fxChange) > 3) {
    alerts.push({
      type:    'fx',
      level:   pd.fxChange > 0 ? 'danger' : 'info',
      icon:    '💱',
      message: `Курс TJS/USD изменился на ${pd.fxChange > 0 ? '+' : ''}${pd.fxChange?.toFixed(2)}% за месяц`,
    });
  }

  // Падение нефти > 5%
  if (pd.oilChangePct && pd.oilChangePct < -5) {
    alerts.push({
      type:    'oil',
      level:   'warning',
      icon:    '🛢️',
      message: `Нефть упала на ${Math.abs(pd.oilChangePct).toFixed(1)}% — риск сокращения переводов из России`,
    });
  }

  // Рост пшеницы > 5%
  if (pd.wheatChangePct && pd.wheatChangePct > 5) {
    alerts.push({
      type:    'wheat',
      level:   'warning',
      icon:    '🌾',
      message: `Пшеница выросла на ${pd.wheatChangePct.toFixed(1)}% — продовольственная инфляция`,
    });
  }

  // Топ-5 изменений
  const changes = [];
  const addChange = (label, val, prev, unit, impact) => {
    if (val == null || prev == null || prev === 0) return;
    const pct = ((val - prev) / Math.abs(prev)) * 100;
    changes.push({ label, value: val, prev, pct: Math.round(pct * 10) / 10, unit, impact });
  };

  addChange('Нефть WTI',     pd.oilPrice,       pd.oilPrev,       '$/барр.', 'Переводы мигрантов');
  addChange('Алюминий',      pd.aluminumPrice,   pd.aluminumPrev,  '$/т',     'Экспорт ТАЛКО');
  addChange('Пшеница',       pd.wheatPrice,      pd.wheatPrev,     '$/буш.',  'Цены на хлеб');
  addChange('Курс USD/TJS',  pd.fxCurrent,       pd.fxPrev,        'TJS',     'Инфляция импорта');
  addChange('Инфляция',      pd.inflation,       pd.inflationPrev, '%',       'Покупательная способность');

  changes.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));
  const topChanges = changes.slice(0, 5);

  // Рекомендации для руководства
  const recommendations = buildRecommendations(pd, alerts);

  const digest = {
    date:         today,
    generatedAt:  now.toISOString(),
    alerts,
    gdpNowcast:   pd.gdpNowcast || null,
    healthIndex:  pd.healthIndex || null,
    topChanges,
    correlations: pd.topCorrelations || [],
    recommendations,
    dataSources:  pd.sources || [],
  };

  writeJSON(DIGEST_FILE, digest);
  pipelineLog('info', 'digest', `Дайджест сформирован: ${alerts.length} алертов, ${topChanges.length} изменений`);
  return digest;
}

function buildRecommendations(pd, alerts) {
  const recs = [];

  // Рекомендация 1: на основе нефти и переводов
  if (pd.oilChangePct !== undefined) {
    if (pd.oilChangePct < -5) {
      recs.push({
        priority: 'высокий',
        icon: '🏦',
        title: 'Валютная политика НБТ',
        text: `Снижение нефти на ${Math.abs(pd.oilChangePct).toFixed(1)}% создаёт риск сокращения переводов из России. Рекомендуется усилить валютные интервенции НБТ для поддержания курса TJS.`,
      });
    } else if (pd.oilChangePct > 5) {
      recs.push({
        priority: 'низкий',
        icon: '📈',
        title: 'Благоприятная конъюнктура',
        text: `Рост нефти на ${pd.oilChangePct.toFixed(1)}% укрепляет рубль, что увеличит объём переводов мигрантов. Рекомендуется пополнить валютные резервы.`,
      });
    }
  }

  // Рекомендация 2: на основе инфляции
  if (pd.inflation > 10) {
    recs.push({
      priority: 'высокий',
      icon: '📊',
      title: 'Денежно-кредитная политика',
      text: `Инфляция ${pd.inflation.toFixed(1)}% превышает целевой ориентир НБТ. Рекомендуется рассмотреть повышение ставки рефинансирования для сдерживания роста цен.`,
    });
  } else if (pd.inflation < 4) {
    recs.push({
      priority: 'средний',
      icon: '📉',
      title: 'Стимулирующая политика',
      text: 'Низкая инфляция создаёт пространство для смягчения денежно-кредитной политики и стимулирования кредитования реального сектора.',
    });
  }

  // Рекомендация 3: на основе пшеницы
  if (pd.wheatChangePct > 5) {
    recs.push({
      priority: 'высокий',
      icon: '🌾',
      title: 'Продовольственная безопасность',
      text: `Мировые цены на пшеницу выросли на ${pd.wheatChangePct.toFixed(1)}%. Рекомендуется субсидировать импорт зерна и активировать государственные резервы для стабилизации цен на хлеб.`,
    });
  }

  // Рекомендация 4: на основе алюминия
  if (pd.aluminumChangePct > 3) {
    recs.push({
      priority: 'средний',
      icon: '🏭',
      title: 'Поддержка ТАЛКО',
      text: `Рост цен на алюминий на ${pd.aluminumChangePct.toFixed(1)}% повышает экспортную выручку ТАЛКО. Рекомендуется обеспечить бесперебойную работу Нурекской ГЭС для максимизации производства.`,
    });
  }

  // Дополнительная стандартная рекомендация если нет специфических
  if (recs.length < 2) {
    recs.push({
      priority: 'средний',
      icon: '🔍',
      title: 'Мониторинг внешних рисков',
      text: 'Рекомендуется усилить мониторинг геополитических рисков и их влияния на переводы мигрантов из России, которые составляют около 25-30% ВВП страны.',
    });
  }

  return recs.slice(0, 3);
}

// ─── МОДУЛЬ 3: Ночной ETL-пайплайн ──────────────────────────────────────────

let _pipelineTimer = null;

/**
 * Один прогон ETL-пайплайна.
 */
async function runNightlyPipeline() {
  const started = new Date().toISOString();
  pipelineLog('info', 'etl', 'Запуск ETL-пайплайна...');

  const {
    fetchNBT, fetchWorldBank,
    fetchOilPrice, fetchAluminumPrice, fetchWheatPrice,
    getRatesHistory,
  } = require('./dataCollector');

  const results = {};
  const sources = [];

  // Загружаем источники параллельно
  const [nbtR, wbGdpR, wbCpiR, oilR, aluR, wheatR] = await Promise.allSettled([
    fetchNBT(),
    fetchWorldBank('GDP'),
    fetchWorldBank('CPI'),
    fetchOilPrice(),
    fetchAluminumPrice(),
    fetchWheatPrice(),
  ]);

  // НБТ
  if (nbtR.status === 'fulfilled') {
    results.nbt = nbtR.value;
    sources.push('nbt.tj');
    pipelineLog('info', 'etl:nbt', 'НБТ загружен');
  } else {
    pipelineLog('error', 'etl:nbt', nbtR.reason?.message || 'ошибка');
  }

  // ВВП
  if (wbGdpR.status === 'fulfilled') {
    results.wbGdp = wbGdpR.value;
    sources.push('worldbank:GDP');
    pipelineLog('info', 'etl:wb', 'ВВП загружен');
  } else {
    pipelineLog('warn', 'etl:wb', 'ВВП: ' + (wbGdpR.reason?.message || 'ошибка'));
  }

  // Инфляция
  if (wbCpiR.status === 'fulfilled') {
    results.wbCpi = wbCpiR.value;
    sources.push('worldbank:CPI');
  } else {
    pipelineLog('warn', 'etl:wb', 'CPI: ' + (wbCpiR.reason?.message || 'ошибка'));
  }

  // Товарные рынки
  if (oilR.status === 'fulfilled')   { results.oil      = oilR.value;   sources.push('yahoo:CL=F'); }
  if (aluR.status === 'fulfilled')   { results.aluminum = aluR.value;   sources.push('yahoo:ALI=F'); }
  if (wheatR.status === 'fulfilled') { results.wheat    = wheatR.value; sources.push('yahoo:ZW=F'); }

  // Строим датасеты для корреляционного анализа
  const ratesHistory = getRatesHistory();
  const datasets = buildDatasets(results, ratesHistory);

  // Корреляционная матрица
  const corr = correlationMatrix(datasets);
  results.correlations = corr;

  // Данные для nowcast
  const hfData = buildHFData(results, ratesHistory);
  const nowcast = nowcastGDP(hfData);
  results.nowcast = nowcast;

  // Данные для индекса здоровья
  const healthIndicators = buildHealthIndicators(results, ratesHistory);
  const health = economicHealthIndex(healthIndicators);
  results.healthIndex = health;

  // Сохраняем pipeline_data.json
  const pipelineData = {
    runAt:        started,
    completedAt:  new Date().toISOString(),
    sources,
    ...extractPipelineMetrics(results),
    correlations: corr,
    nowcast,
    healthIndex:  health,
    topCorrelations: corr.topPairs?.slice(0, 3) || [],
  };

  writeJSON(PIPELINE_FILE, pipelineData);

  // Обновляем dashboard_cache.json из данных пайплайна
  try {
    const nbtRates = results.nbt?.rates || {};
    const cpiSeries = results.wbCpi?.series?.filter(s => s.value !== null) || [];
    const lastCpi   = cpiSeries.length > 0 ? cpiSeries[cpiSeries.length - 1] : null;

    const dashCache = {
      cpi:              lastCpi?.value ?? null,
      cpi_year:         lastCpi?.year ?? null,
      usd_tjs:          nbtRates.USD?.rate ?? null,
      eur_tjs:          nbtRates.EUR?.rate ?? null,
      rub_tjs:          nbtRates.RUB?.rate ?? null,
      nbt_date:         results.nbt?.date ?? null,
      oil_price:        results.oil?.price ?? null,
      oil_change_pct:   results.oil?.changePct ?? null,
      wheat_price:      results.wheat?.price ?? null,
      wheat_change_pct: results.wheat?.changePct ?? null,
      aluminum_price:   results.aluminum?.price ?? null,
      al_change_pct:    results.aluminum?.changePct ?? null,
      last_updated:     new Date().toISOString(),
      source:           sources.join(', '),
    };
    writeJSON(DASHBOARD_CACHE, dashCache);
    pipelineLog('info', 'etl', 'dashboard_cache.json обновлён');
  } catch (e) {
    pipelineLog('warn', 'etl', 'Не удалось обновить dashboard_cache: ' + e.message);
  }

  // Генерируем дайджест
  const digest = generateMorningDigest(pipelineData);

  pipelineLog('info', 'etl', `Пайплайн завершён. Источников: ${sources.length}. ВВП nowcast: ${nowcast.gdpGrowthEstimate}%`);
  return { pipelineData, digest };
}

function buildDatasets(results, ratesHistory) {
  const datasets = {};

  // Курс USD/TJS (из истории НБТ)
  if (ratesHistory.length > 0) {
    datasets['USD/TJS'] = ratesHistory
      .filter(r => r.USD)
      .map(r => ({ date: r.date, value: r.USD }));
  }

  // Нефть
  if (results.oil?.history30) {
    datasets['Нефть'] = results.oil.history30;
  }

  // Алюминий
  if (results.aluminum?.history30) {
    datasets['Алюминий'] = results.aluminum.history30;
  }

  // Пшеница
  if (results.wheat?.history30) {
    datasets['Пшеница'] = results.wheat.history30;
  }

  // ВВП (годовые данные — менее репрезентативны для краткосрочных корреляций)
  if (results.wbGdp?.series?.length) {
    datasets['ВВП'] = results.wbGdp.series.map(s => ({
      date:  String(s.year) + '-01-01',
      value: s.value,
    }));
  }

  // Инфляция
  if (results.wbCpi?.series?.length) {
    datasets['Инфляция'] = results.wbCpi.series.map(s => ({
      date:  String(s.year) + '-01-01',
      value: s.value,
    }));
  }

  return datasets;
}

function buildHFData(results, ratesHistory) {
  const usdRates = ratesHistory.filter(r => r.USD).sort((a, b) => a.date.localeCompare(b.date));
  const current  = usdRates[usdRates.length - 1];
  const prev     = usdRates[Math.max(0, usdRates.length - 31)]; // ~месяц назад

  const cpiSeries = results.wbCpi?.series || [];
  const lastCpi   = cpiSeries[cpiSeries.length - 1]?.value;
  const prevCpi   = cpiSeries[cpiSeries.length - 2]?.value;

  return {
    fxRate:    { current: current?.USD,        prev: prev?.USD },
    cpi:       { current: lastCpi,              prev: prevCpi },
    oil:       { current: results.oil?.price,   prev: results.oil?.prevClose },
    aluminum:  { current: results.aluminum?.price, prev: results.aluminum?.prevClose },
    // Переводы и торговля — используем прокси через NBT если нет прямых данных
    remittances: { current: null, prev: null },
    trade:       { current: null, prev: null },
  };
}

function buildHealthIndicators(results, ratesHistory) {
  const usdRates  = ratesHistory.filter(r => r.USD).sort((a, b) => a.date.localeCompare(b.date));
  const currRate  = usdRates[usdRates.length - 1]?.USD;
  const prevRate  = usdRates[Math.max(0, usdRates.length - 31)]?.USD;
  const cpiSeries = results.wbCpi?.series || [];
  const lastCpi   = cpiSeries[cpiSeries.length - 1]?.value;

  return {
    inflation:         lastCpi,
    fxCurrent:         currRate,
    fxPrev:            prevRate,
    oilPrice:          results.oil?.price,
    oilPrev:           results.oil?.prevClose,
    aluminumPrice:     results.aluminum?.price,
    aluminumPrev:      results.aluminum?.prevClose,
    wheatPrice:        results.wheat?.price,
    wheatPrev:         results.wheat?.prevClose,
    remittancesGrowth: null,  // заполняется из внешних источников если доступны
    foodPriceIdx:      null,
  };
}

function extractPipelineMetrics(results) {
  const usd = results.nbt?.rates?.USD?.rate;
  const rub = results.nbt?.rates?.RUB?.rate;

  const oilChangePct = results.oil?.changePct;
  const aluChangePct = results.aluminum?.changePct;
  const wheatChangePct = results.wheat?.changePct;

  const cpiSeries = results.wbCpi?.series || [];
  const inflation  = cpiSeries[cpiSeries.length - 1]?.value;
  const inflPrev   = cpiSeries[cpiSeries.length - 2]?.value;

  return {
    fxCurrent:        usd,
    rubRate:          rub,
    oilPrice:         results.oil?.price,
    oilPrev:          results.oil?.prevClose,
    oilChangePct,
    aluminumPrice:    results.aluminum?.price,
    aluminumPrev:     results.aluminum?.prevClose,
    aluminumChangePct: aluChangePct,
    wheatPrice:       results.wheat?.price,
    wheatPrev:        results.wheat?.prevClose,
    wheatChangePct,
    inflation,
    inflationPrev:    inflPrev,
  };
}

/**
 * Планировщик: запускает пайплайн каждую ночь в 02:00.
 * Также поддерживает немедленный запуск для тестирования.
 */
function schedulePipeline({ runNow = false } = {}) {
  if (_pipelineTimer) {
    clearTimeout(_pipelineTimer);
    _pipelineTimer = null;
  }

  function scheduleNext() {
    const now    = new Date();
    const target = new Date(now);
    target.setHours(2, 0, 0, 0);
    if (target <= now) target.setDate(target.getDate() + 1);
    const ms = target - now;
    const h  = Math.floor(ms / 3600000);
    const m  = Math.floor((ms % 3600000) / 60000);
    pipelineLog('info', 'scheduler', `Следующий запуск ETL через ${h}ч ${m}мин (${target.toLocaleString('ru-RU')})`);

    _pipelineTimer = setTimeout(async () => {
      try {
        await runNightlyPipeline();
      } catch (e) {
        pipelineLog('error', 'scheduler', 'Ошибка ETL: ' + e.message);
      }
      scheduleNext(); // запланировать следующий
    }, ms);
  }

  if (runNow) {
    pipelineLog('info', 'scheduler', 'Немедленный запуск ETL (runNow=true)');
    runNightlyPipeline().catch(e =>
      pipelineLog('error', 'scheduler', 'Ошибка немедленного ETL: ' + e.message)
    );
  }

  scheduleNext();
}

// ─── Геттеры кэшированных данных ─────────────────────────────────────────────

function getPipelineData() {
  return readJSON(PIPELINE_FILE, null);
}

function getMorningDigest() {
  return readJSON(DIGEST_FILE, null);
}

function getPipelineLog(limit = 50) {
  const log = readJSON(LOG_FILE, []);
  return log.slice(-limit).reverse();
}

// ─── Экспорт ──────────────────────────────────────────────────────────────────

module.exports = {
  // Модуль 1
  correlationMatrix,
  pearson,
  // Модуль 2
  nowcastGDP,
  // Модуль 3
  runNightlyPipeline,
  schedulePipeline,
  // Модуль 4
  generateMorningDigest,
  // Модуль 5
  economicHealthIndex,
  // Геттеры
  getPipelineData,
  getMorningDigest,
  getPipelineLog,
};
