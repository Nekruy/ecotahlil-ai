/**
 * stressTest.js — Модуль стресс-тестирования экономики Таджикистана
 * Чистый JavaScript, без внешних зависимостей.
 *
 * Базовые данные (2023–2024):
 *   ВВП: $11.5 млрд, рост +7.7%
 *   Инфляция: ~5.8%
 *   Курс TJS/USD: ~10.9
 *   Переводы мигрантов: ~$3.45 млрд (~30% ВВП)
 *   Резервы НБТ: ~$3.8 млрд
 *   Экспорт электроэнергии: ~$300 млн/год
 *   ТАЛКО: ~15% экспорта страны
 */

'use strict';

// ─── Вспомогательные функции ────────────────────────────────────────────────

function riskLevel(gdpChange) {
  const abs = Math.abs(gdpChange);
  if (abs < 2)  return 'низкий';
  if (abs < 5)  return 'умеренный';
  if (abs < 10) return 'высокий';
  return 'критический';
}

function riskColor(level) {
  return { низкий: 'green', умеренный: 'gold', высокий: 'red', критический: 'critical' }[level] || 'gray';
}

function round2(v) { return Math.round(v * 100) / 100; }

function buildTimeline(q1, q2, q3, q4, key) {
  return [
    { quarter: 'Q1', label: '1-й квартал', [key]: round2(q1) },
    { quarter: 'Q2', label: '2-й квартал', [key]: round2(q2) },
    { quarter: 'Q3', label: '3-й квартал', [key]: round2(q3) },
    { quarter: 'Q4', label: '4-й квартал', [key]: round2(q4) },
  ];
}

// ─── СЦЕНАРИЙ 1: Падение нефти ───────────────────────────────────────────────
/**
 * @param {number} oilPrice — цена нефти в долларах (20, 30, 40, 50)
 * Базовая цена нефти: $70/барр.
 * Коэффициенты из кризиса 2015–2016.
 */
function scenarioOilPrice(oilPrice) {
  const BASE_OIL = 70;
  const oilDrop = (BASE_OIL - oilPrice) / BASE_OIL; // доля падения (0..1)

  // Замедление российской экономики
  const russiaGdpSlowdown = oilDrop * 0.35; // 35% эластичность РФ к нефти

  // Переводы сокращаются с коэффициентом -0.8 от падения нефти
  const remittancesChange = round2(-oilDrop * 0.8 * 100);          // %

  // Дефицит валюты → курс TJS падает
  // Переводы = 30% ВВП; каждый 1% сокращения переводов → 0.25% ослабление курса
  const exchangeRateChange = round2(remittancesChange * 0.25);      // % (отрицательно = ослабление TJS)

  // Инфляция: курс TJS → импорт дорожает; pass-through ~0.4
  const inflationChange = round2(Math.abs(exchangeRateChange) * 0.4);

  // ВВП: прямой эффект через переводы (мультипликатор 1.5) + курс
  const gdpChange = round2(remittancesChange * 0.30 * 1.5 - inflationChange * 0.1);

  // Резервы: НБТ тратит для защиты курса
  const reservesChange = round2(remittancesChange * 0.4);           // %

  const risk = riskLevel(gdpChange);

  // Рекомендации
  const recs = [
    'Ввести режим управляемого плавания TJS для предотвращения резкой девальвации',
    'Активировать линию рефинансирования НБТ для коммерческих банков',
    'Усилить диверсификацию трудовых миграционных коридоров (Казахстан, ОАЭ, ЕС)',
  ];
  if (oilPrice <= 30) {
    recs.push('Запросить поддержку МВФ и Всемирного банка в рамках экстренного финансирования');
    recs.push('Ввести временные субсидии на продукты первой необходимости');
  }
  if (oilPrice <= 40) {
    recs.push('Создать резервный фонд компенсации выпадающих переводов');
    recs.push('Форсировать программу импортозамещения в агросекторе');
  }
  recs.push('Проводить еженедельный мониторинг входящих переводов через банковские каналы');

  // Поквартальная динамика (кризис нарастает к Q2–Q3, начинает стабилизироваться к Q4)
  const timeline = [
    { quarter: 'Q1', label: '1-й квартал', gdpChange: round2(gdpChange * 0.3), inflationChange: round2(inflationChange * 0.5), exchangeRateChange: round2(exchangeRateChange * 0.4), remittancesChange: round2(remittancesChange * 0.5) },
    { quarter: 'Q2', label: '2-й квартал', gdpChange: round2(gdpChange * 0.8), inflationChange: round2(inflationChange * 0.9), exchangeRateChange: round2(exchangeRateChange * 0.8), remittancesChange: round2(remittancesChange * 0.8) },
    { quarter: 'Q3', label: '3-й квартал', gdpChange: round2(gdpChange * 1.0), inflationChange: round2(inflationChange * 1.0), exchangeRateChange: round2(exchangeRateChange * 1.0), remittancesChange: round2(remittancesChange * 1.0) },
    { quarter: 'Q4', label: '4-й квартал', gdpChange: round2(gdpChange * 0.85), inflationChange: round2(inflationChange * 0.8), exchangeRateChange: round2(exchangeRateChange * 0.9), remittancesChange: round2(remittancesChange * 0.85) },
  ];

  return {
    scenario: 'oil',
    scenarioName: 'Падение нефти',
    inputParam: { oilPrice, baseOilPrice: BASE_OIL },
    gdpChange: round2(gdpChange),
    inflationChange: round2(inflationChange),
    exchangeRateChange: round2(exchangeRateChange),
    remittancesChange: round2(remittancesChange),
    reservesChange: round2(reservesChange),
    electricityDeficit: 0,
    riskLevel: risk,
    riskColor: riskColor(risk),
    regionalImpact: {
      'Душанбе':  round2(gdpChange * 0.8),
      'Согд':     round2(gdpChange * 1.1),
      'Хатлон':   round2(gdpChange * 1.2),
      'ГБАО':     round2(gdpChange * 0.9),
      'РРП':      round2(gdpChange * 1.1),
    },
    recommendations: recs,
    timeline,
  };
}

// ─── СЦЕНАРИЙ 2: Ограничение переводов из России ─────────────────────────────
/**
 * @param {number} restrictionPct — процент ограничения переводов (25, 50, 75, 100)
 */
function scenarioRemittances(restrictionPct) {
  const share = restrictionPct / 100; // 0..1

  // Переводы = 30% ВВП → прямое сокращение
  const remittancesChange = round2(-restrictionPct);               // %

  // Дефицит валюты: переводы составляют значительную часть притока $
  // 1% сокращения переводов → ~0.35% ослабление TJS (более чувствительно, чем косвенный канал нефти)
  const exchangeRateChange = round2(-restrictionPct * 0.35);

  // Импорт дорожает → инфляция (pass-through 0.45)
  const inflationChange = round2(Math.abs(exchangeRateChange) * 0.45);

  // Резервы: НБТ тратит $ для сглаживания курса
  const reservesChange = round2(-restrictionPct * 0.5);

  // ВВП: мультипликатор переводов ~1.5, смягчается торговым балансом
  const gdpChange = round2(-(share * 30) * 1.5 * 0.25 - inflationChange * 0.15);

  const risk = riskLevel(gdpChange);

  const recs = [
    'Срочно диверсифицировать каналы входящих переводов: Wise, Western Union через нейтральные страны',
    'Активировать двусторонние переговоры с ЦБ РФ по специальным коридорам для Таджикистана',
    'Ввести стимулы для легализации неформальных каналов (хавала)',
  ];
  if (restrictionPct >= 50) {
    recs.push('Запросить чрезвычайную кредитную линию МВФ (Rapid Financing Instrument)');
    recs.push('Ввести режим экономии валютных резервов: приоритет — импорт продовольствия и топлива');
  }
  if (restrictionPct >= 75) {
    recs.push('Рассмотреть введение временного контроля за движением капитала');
    recs.push('Экстренная программа занятости для возвращающихся мигрантов');
    recs.push('Расширить программу социальной поддержки домохозяйств, зависящих от переводов');
  }
  recs.push('Наладить систему мониторинга еженедельных переводов через НБТ в режиме реального времени');

  const timeline = [
    { quarter: 'Q1', label: '1-й квартал', gdpChange: round2(gdpChange * 0.4), inflationChange: round2(inflationChange * 0.5), exchangeRateChange: round2(exchangeRateChange * 0.5), remittancesChange: round2(remittancesChange * 0.7), reservesChange: round2(reservesChange * 0.4) },
    { quarter: 'Q2', label: '2-й квартал', gdpChange: round2(gdpChange * 0.85), inflationChange: round2(inflationChange * 0.85), exchangeRateChange: round2(exchangeRateChange * 0.9), remittancesChange: round2(remittancesChange * 1.0), reservesChange: round2(reservesChange * 0.7) },
    { quarter: 'Q3', label: '3-й квартал', gdpChange: round2(gdpChange * 1.0), inflationChange: round2(inflationChange * 1.0), exchangeRateChange: round2(exchangeRateChange * 1.0), remittancesChange: round2(remittancesChange * 1.0), reservesChange: round2(reservesChange * 1.0) },
    { quarter: 'Q4', label: '4-й квартал', gdpChange: round2(gdpChange * 0.9), inflationChange: round2(inflationChange * 0.9), exchangeRateChange: round2(exchangeRateChange * 0.95), remittancesChange: round2(remittancesChange * 1.0), reservesChange: round2(reservesChange * 1.0) },
  ];

  return {
    scenario: 'remittances',
    scenarioName: 'Ограничение переводов',
    inputParam: { restrictionPct },
    gdpChange,
    inflationChange,
    exchangeRateChange,
    remittancesChange,
    reservesChange,
    electricityDeficit: 0,
    riskLevel: risk,
    riskColor: riskColor(risk),
    regionalImpact: {
      'Душанбе':  round2(gdpChange * 0.9),
      'Согд':     round2(gdpChange * 1.2),  // высокая зависимость от переводов
      'Хатлон':   round2(gdpChange * 1.3),  // наибольшая доля мигрантов
      'ГБАО':     round2(gdpChange * 0.8),
      'РРП':      round2(gdpChange * 1.1),
    },
    recommendations: recs,
    timeline,
  };
}

// ─── СЦЕНАРИЙ 3: Неурожай ────────────────────────────────────────────────────
/**
 * @param {number} cropFailurePct — падение урожая в % (20, 30, 50)
 * Региональный расчёт по 5 регионам.
 */
function scenarioCropFailure(cropFailurePct) {
  const share = cropFailurePct / 100;

  // Рост импорта зерна (эластичность ~0.7)
  const grainImportIncrease = round2(share * 70);                   // % рост импорта зерна

  // Спрос на валюту → ослабление TJS
  // Импорт зерна ≈ $150–200 млн в год; рост импорта давит на курс
  const exchangeRateChange = round2(-share * 12);                    // %

  // Продовольственная инфляция (хлеб, мука): эластичность 0.8
  const foodInflationChange = round2(share * 0.8 * 25);             // п.п. продовольственной инфляции

  // Общая инфляция (продовольствие ~45% корзины)
  const inflationChange = round2(foodInflationChange * 0.45 + Math.abs(exchangeRateChange) * 0.2);

  // ВВП: с/х ≈ 20% ВВП Таджикистана
  const gdpChange = round2(-(share * 20 * 0.6));                     // с учётом частичной компенсации импортом

  const reservesChange = round2(-grainImportIncrease * 0.1);        // валюта на импорт
  const remittancesChange = 0;

  const risk = riskLevel(gdpChange);

  // Региональный эффект (доля с/х в регионе)
  const regionalWeights = {
    'Душанбе': 0.2,   // меньше с/х
    'Согд':    0.8,   // крупный аграрный регион
    'Хатлон':  1.3,   // главный с/х регион
    'ГБАО':    0.6,   // горное земледелие
    'РРП':     0.9,   // смешанный
  };

  const recs = [
    'Сформировать государственный резерв зерна на 6 месяцев (не менее 200 тыс. т)',
    'Открыть специальные валютные аукционы НБТ для импортёров продовольствия',
    'Ввести временные таможенные льготы на импорт зерна, муки и растительного масла',
  ];
  if (cropFailurePct >= 30) {
    recs.push('Запросить продовольственную помощь ВПП ООН и ФАО');
    recs.push('Ввести ценовое регулирование на хлеб и муку в розничной торговле');
  }
  if (cropFailurePct >= 50) {
    recs.push('Объявить режим чрезвычайной продовольственной ситуации');
    recs.push('Ввести адресные продовольственные субсидии для уязвимых домохозяйств (600 тыс. семей)');
    recs.push('Мобилизовать ирригационные ресурсы для спасения оставшегося урожая');
  }
  recs.push('Ускорить реализацию программ страхования урожая и аграрного кредитования');

  // Сезонная динамика: пик кризиса в Q3 (сбор урожая), ослабление к Q4–Q1
  const timeline = [
    { quarter: 'Q1', label: '1-й квартал', gdpChange: round2(gdpChange * 0.2), inflationChange: round2(inflationChange * 0.4), exchangeRateChange: round2(exchangeRateChange * 0.3), foodInflationChange: round2(foodInflationChange * 0.3) },
    { quarter: 'Q2', label: '2-й квартал', gdpChange: round2(gdpChange * 0.5), inflationChange: round2(inflationChange * 0.7), exchangeRateChange: round2(exchangeRateChange * 0.6), foodInflationChange: round2(foodInflationChange * 0.6) },
    { quarter: 'Q3', label: '3-й квартал', gdpChange: round2(gdpChange * 1.0), inflationChange: round2(inflationChange * 1.0), exchangeRateChange: round2(exchangeRateChange * 1.0), foodInflationChange: round2(foodInflationChange * 1.0) },
    { quarter: 'Q4', label: '4-й квартал', gdpChange: round2(gdpChange * 0.7), inflationChange: round2(inflationChange * 0.85), exchangeRateChange: round2(exchangeRateChange * 0.7), foodInflationChange: round2(foodInflationChange * 0.75) },
  ];

  return {
    scenario: 'crop',
    scenarioName: 'Неурожай',
    inputParam: { cropFailurePct, grainImportIncrease },
    gdpChange,
    inflationChange,
    exchangeRateChange,
    remittancesChange,
    reservesChange,
    electricityDeficit: 0,
    foodInflationChange,
    riskLevel: risk,
    riskColor: riskColor(risk),
    regionalImpact: Object.fromEntries(
      Object.entries(regionalWeights).map(([r, w]) => [r, round2(gdpChange * w)])
    ),
    recommendations: recs,
    timeline,
  };
}

// ─── СЦЕНАРИЙ 4: Гидроэнергетический кризис ─────────────────────────────────
/**
 * @param {number} waterDropPct — падение уровня воды в % (10, 20, 30, 50)
 *
 * Коэффициенты:
 *   Каждые 10% падения воды → 8% сокращения электроэнергии
 *   Каждые 8% сокращения э/э → 1.5% падения ВВП
 *   Экспорт э/э = $300 млн/год
 *   ТАЛКО = 15% экспорта страны
 */
function scenarioHydropower(waterDropPct) {
  const share = waterDropPct / 100;

  // Выработка электроэнергии
  const electricityDeficit = round2(waterDropPct * 0.8);            // %

  // Сокращение экспорта электроэнергии ($300 млн в год)
  const exportElectricityCut = round2(electricityDeficit / 100 * 300); // $млн

  // ТАЛКО (алюминий): крупнейший потребитель э/э; при нехватке снижает производство
  // ТАЛКО потребляет ~40% электроэнергии страны
  const talcoProductionCut = round2(Math.min(electricityDeficit / 40, 1) * 100); // % сокращение ТАЛКО

  // Промышленность: останавливается пропорционально дефициту
  const industryStopPct = round2(electricityDeficit * 0.6);          // % падение промышленности

  // ВВП: 1.5% за каждые 10% падения воды (т.е. 8% э/э)
  // + ТАЛКО (вклад в экспорт 15% = ~$1.7 млрд экспорта * 15% = $255 млн)
  const gdpFromElectricity = round2(-(waterDropPct / 10) * 1.5);
  const gdpFromTALCO       = round2(-(talcoProductionCut / 100) * 2.2); // ТАЛКО ~2.2% ВВП
  const gdpChange          = round2(gdpFromElectricity + gdpFromTALCO);

  // Инфляция: дефицит э/э → рост себестоимости → инфляция издержек
  const inflationChange    = round2(electricityDeficit * 0.15 + 1.0);

  // Курс TJS: сокращение экспорта ($э/э + ТАЛКО) давит на валюту
  const exportLoss = exportElectricityCut + (talcoProductionCut / 100) * 255;
  const exchangeRateChange = round2(-(exportLoss / 3800) * 100 * 0.8); // % от резервов

  const reservesChange     = round2(exchangeRateChange * 0.5);
  const remittancesChange  = round2(gdpChange * 0.15); // вторичный эффект на занятость мигрантов

  // Безработица
  const unemploymentChange = round2(electricityDeficit * 0.2 + talcoProductionCut * 0.1);

  const risk = riskLevel(gdpChange);

  // Региональный эффект
  const regionalImpact = {
    'Душанбе': round2(gdpChange * 0.9 + (-industryStopPct * 0.05)),   // промышленные отключения
    'Согд':    round2(gdpChange * 0.85),                               // промышленность, Кайраккум
    'Хатлон':  round2(gdpChange * 1.1 + (-share * 5)),                // нехватка воды для орошения
    'ГБАО':    round2(gdpChange * 1.4),                                // наиболее уязвим (малые ГЭС)
    'РРП':     round2(gdpChange * 1.0),
  };

  const recs = [
    'Ввести приоритетное распределение электроэнергии: население → медицина → с/х → промышленность',
    'Запустить переговоры об экстренных поставках электроэнергии из Центральноазиатского кольца',
    'Ввести режим экономии э/э в госструктурах (–30%) и промышленности (–20%)',
  ];
  if (waterDropPct >= 20) {
    recs.push('Переключить ТАЛКО на режим минимального производства для сохранения занятости');
    recs.push('Активировать соглашения о параллельной работе с энергосистемами Узбекистана и Кыргызстана');
  }
  if (waterDropPct >= 30) {
    recs.push('Сформировать компенсационный фонд для населения ГБАО и Хатлона ($50 млн)');
    recs.push('Запросить техническую помощь АБР по модернизации системы управления водохранилищами');
    recs.push('Ввести субсидированные тарифы на топливо для автономных генераторов');
  }
  if (waterDropPct >= 50) {
    recs.push('Объявить режим энергетической чрезвычайной ситуации на национальном уровне');
    recs.push('Провести переговоры с МВФ о чрезвычайном кредите для компенсации выпадающего экспорта');
    recs.push('Рассмотреть временную приостановку экспорта э/э для обеспечения внутренних нужд');
  }
  recs.push('Ускорить строительство малых ГЭС и солнечных станций для диверсификации генерации');

  // Поквартальная динамика (гидрокризис нарастает в Q1–Q2 — маловодный период)
  const timeline = [
    { quarter: 'Q1', label: '1-й квартал', gdpChange: round2(gdpChange * 0.5), inflationChange: round2(inflationChange * 0.6), electricityDeficit: round2(electricityDeficit * 0.7), exchangeRateChange: round2(exchangeRateChange * 0.4), unemploymentChange: round2(unemploymentChange * 0.4) },
    { quarter: 'Q2', label: '2-й квартал', gdpChange: round2(gdpChange * 0.9), inflationChange: round2(inflationChange * 0.9), electricityDeficit: round2(electricityDeficit * 1.0), exchangeRateChange: round2(exchangeRateChange * 0.8), unemploymentChange: round2(unemploymentChange * 0.7) },
    { quarter: 'Q3', label: '3-й квартал', gdpChange: round2(gdpChange * 1.0), inflationChange: round2(inflationChange * 1.0), electricityDeficit: round2(electricityDeficit * 0.8), exchangeRateChange: round2(exchangeRateChange * 1.0), unemploymentChange: round2(unemploymentChange * 1.0) },
    { quarter: 'Q4', label: '4-й квартал', gdpChange: round2(gdpChange * 0.7), inflationChange: round2(inflationChange * 0.8), electricityDeficit: round2(electricityDeficit * 0.5), exchangeRateChange: round2(exchangeRateChange * 0.7), unemploymentChange: round2(unemploymentChange * 0.8) },
  ];

  return {
    scenario: 'hydro',
    scenarioName: 'Гидроэнергетический кризис',
    inputParam: { waterDropPct, electricityDeficit, exportElectricityCutMln: round2(exportElectricityCut), talcoProductionCut },
    gdpChange,
    inflationChange,
    exchangeRateChange,
    remittancesChange,
    reservesChange,
    electricityDeficit,
    unemploymentChange,
    industryStopPct,
    riskLevel: risk,
    riskColor: riskColor(risk),
    regionalImpact,
    recommendations: recs,
    timeline,
  };
}

// ─── Публичный API ───────────────────────────────────────────────────────────

/**
 * Запуск стресс-теста по сценарию.
 * @param {string} scenario — 'oil' | 'remittances' | 'crop' | 'hydro'
 * @param {object} params   — параметры сценария
 * @returns {object}        — результат теста
 */
function runStressTest(scenario, params) {
  switch (scenario) {
    case 'oil':
      return scenarioOilPrice(Number(params.oilPrice));
    case 'remittances':
      return scenarioRemittances(Number(params.restrictionPct));
    case 'crop':
      return scenarioCropFailure(Number(params.cropFailurePct));
    case 'hydro':
      return scenarioHydropower(Number(params.waterDropPct));
    default:
      throw new Error(`Неизвестный сценарий: ${scenario}`);
  }
}

module.exports = { runStressTest, scenarioOilPrice, scenarioRemittances, scenarioCropFailure, scenarioHydropower };
