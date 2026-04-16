'use strict';

// ═══════════════════════════════════════════════════════════════════════════════
//  CGE — Модель общего равновесия Таджикистана
//  Базируется на матрице социальных счетов (SAM) 2019 года
//  Перекалибрована с учётом прогноза МЭРиТ РТ 2025–2027
// ═══════════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

// ─── SAM 2019: секторальные данные ──────────────────────────────────────────
//
// Источники: Агентство по статистике РТ, МВФ Article IV 2019,
//            Всемирный банк TAJIKISTAN: Fostering Inclusive Growth (2020)
//
// Поля:
//   name           — название сектора
//   gdp_share      — доля в ВВП (%), сумма = 100
//   alpha          — доля труда в добавленной стоимости (функция Кобба-Дугласа)
//   export_share   — доля экспорта в выпуске сектора
//   import_share   — доля импорта в совокупном спросе сектора
//   remit_sens     — чувствительность к переводам мигрантов (0–1)
//   tax_rate       — эффективная налоговая нагрузка
//   gov_demand     — доля государственного спроса в выпуске сектора

const SECTORS = [
  {
    name: 'Сельское хозяйство',
    gdp_share:    25.0,
    alpha:         0.72,
    export_share:  0.12,
    import_share:  0.04,
    remit_sens:    0.35,
    tax_rate:      0.05,
    gov_demand:    0.05,
  },
  {
    name: 'Горнодобыча',
    gdp_share:    5.0,
    alpha:         0.40,
    export_share:  0.65,
    import_share:  0.08,
    remit_sens:    0.08,
    tax_rate:      0.15,
    gov_demand:    0.05,
  },
  {
    name: 'Алюминий',
    gdp_share:    7.0,
    alpha:         0.35,
    export_share:  0.88,
    import_share:  0.12,
    remit_sens:    0.05,
    tax_rate:      0.10,
    gov_demand:    0.02,
  },
  {
    name: 'Текстиль',
    gdp_share:    3.0,
    alpha:         0.62,
    export_share:  0.55,
    import_share:  0.10,
    remit_sens:    0.15,
    tax_rate:      0.08,
    gov_demand:    0.03,
  },
  {
    name: 'Пищевая промышленность',
    gdp_share:    6.0,
    alpha:         0.55,
    export_share:  0.08,
    import_share:  0.20,
    remit_sens:    0.40,
    tax_rate:      0.10,
    gov_demand:    0.08,
  },
  {
    name: 'Строительство',
    gdp_share:    8.0,
    alpha:         0.52,
    export_share:  0.02,
    import_share:  0.22,
    remit_sens:    0.45,
    tax_rate:      0.12,
    gov_demand:    0.25,
  },
  {
    name: 'Энергетика',
    gdp_share:    6.0,
    alpha:         0.30,
    export_share:  0.15,
    import_share:  0.10,
    remit_sens:    0.10,
    tax_rate:      0.12,
    gov_demand:    0.15,
  },
  {
    name: 'Торговля',
    gdp_share:    15.0,
    alpha:         0.65,
    export_share:  0.04,
    import_share:  0.08,
    remit_sens:    0.42,
    tax_rate:      0.10,
    gov_demand:    0.05,
  },
  {
    name: 'Транспорт',
    gdp_share:    8.0,
    alpha:         0.58,
    export_share:  0.12,
    import_share:  0.06,
    remit_sens:    0.28,
    tax_rate:      0.10,
    gov_demand:    0.12,
  },
  {
    name: 'Услуги',
    gdp_share:    17.0,
    alpha:         0.62,
    export_share:  0.05,
    import_share:  0.07,
    remit_sens:    0.38,
    tax_rate:      0.12,
    gov_demand:    0.20,
  },
];

// ─── Макроэкономические параметры Таджикистана ───────────────────────────────
// Базовые структурные пропорции: SAM 2019 + пересчёт на 10-летних средних МЭРиТ
const MACRO = {
  remit_gdp:   0.32,   // денежные переводы / ВВП ≈ 32% (10-летнее среднее МЭРиТ 2015–2024)
  gov_gdp:     0.30,   // госрасходы / ВВП ≈ 30% (SAM 2019)
  export_gdp:  0.22,   // экспорт / ВВП ≈ 22% (SAM 2019)
  import_gdp:  0.42,   // импорт / ВВП ≈ 42% (SAM 2019)
  multiplier:  1.25,   // фискальный мультипликатор (открытая малая экономика)
  fx_pass:     0.35,   // коэффициент переноса курса на цены (НБТ оценка)
  calibration: {
    source:       'SAM 2019 + МЭРиТ РТ 1997–2024',
    remit_basis:  'НБТ, среднее 2015–2024 (27–51% ВВП, пик 2022)',
    trade_basis:  'Агентство по статистике РТ + МЭРиТ модель',
    updated_at:   '2025-04',
  },
};

// ─── Функция Кобба-Дугласа ───────────────────────────────────────────────────
//
//   Y = A × L^α × K^(1-α)
//
//   alpha   — доля труда (0 < α < 1)
//   labor   — индекс занятости (нормализован к 1)
//   capital — индекс капитала (нормализован к 1)
//   tfp     — совокупная производительность факторов (Total Factor Productivity)
//   Возвращает объём выпуска

function cobbDouglas(alpha, labor, capital, tfp) {
  if (tfp === undefined) tfp = 1.0;
  const L = Math.max(labor,   0.001);
  const K = Math.max(capital, 0.001);
  return tfp * Math.pow(L, alpha) * Math.pow(K, 1 - alpha);
}

// ─── Влияние шока на отдельный сектор (log-линеаризованная CGE) ─────────────
//
// Используется log-линеаризация вокруг базового равновесия SAM 2019.
// Возвращает % изменение выпуска сектора.

function _sectorImpact(sector, tax, wage, expPx, remit, gov, fx_app) {
  const { alpha, export_share, import_share, remit_sens, tax_rate, gov_demand } = sector;

  // ── Канал 1: Изменение налогов ──────────────────────────────────────────
  // Рост налогов удорожает капитал → инвестиции сокращаются → dK < 0
  // Капиталоёмкие сектора (низкий α) теряют больше.
  // Одновременно небольшое снижение спроса на труд.
  const dY_tax = -(alpha * 0.08 + (1 - alpha) * 0.35) * tax;

  // ── Канал 2: Изменение заработных плат ─────────────────────────────────
  // Предложение: рост зарплат сокращает занятость (эластичность ≈ -0.4,
  //   но в короткой перспективе капитал фиксирован → net ≈ -0.18 × α)
  const dY_wage_supply = -alpha * 0.18 * wage;
  // Спрос: рост доходов домохозяйств → внутренний спрос на потребит. товары.
  // Трудоёмкие сектора выигрывают сильнее через передаточный механизм.
  const dY_wage_demand = remit_sens * (alpha > 0.5 ? 0.5 : 0.3) * 0.15 * wage;
  const dY_wage = dY_wage_supply + dY_wage_demand;

  // ── Канал 3: Изменение мировых цен на экспорт ──────────────────────────
  // Прямой: экспортёры получают больше валютной выручки → наращивают выпуск.
  const dY_exp_direct = export_share * 0.70 * expPx;
  // Курсовой: рост выручки → укрепление TJS (fx_app > 0).
  //   Экспортёры теряют ценовую конкурентоспособность.
  //   Импортёры выигрывают от подешевевших ресурсов.
  const dY_exp_fx = -export_share * fx_app * 0.28 + import_share * fx_app * 0.35;
  const dY_exp = dY_exp_direct + dY_exp_fx;

  // ── Канал 4: Изменение денежных переводов мигрантов ────────────────────
  // Потребительский спрос: переводы → доходы домохозяйств → спрос на товары.
  // Эластичность спроса по переводам × chuvstvitelnost' × dolya_perevodov_v_VVP
  const dY_remit_demand = remit_sens * MACRO.remit_gdp * 0.35 * remit;
  // Курсовой: переводы → приток валюты → укрепление TJS → потери экспортёров.
  const dY_remit_fx = -export_share * fx_app * 0.15;
  const dY_remit = dY_remit_demand + dY_remit_fx;

  // ── Канал 5: Изменение государственных расходов ─────────────────────────
  // Прямой спрос государства: госзаказы, инфраструктура, субсидии.
  // С учётом мультипликатора Кейнса для малой открытой экономики.
  const dY_gov = gov_demand * MACRO.gov_gdp * MACRO.multiplier * gov;

  return dY_tax + dY_wage + dY_exp + dY_remit + dY_gov;
}

// ─── Интерпретация результатов ──────────────────────────────────────────────
function _generateInterpretation(shock, gdp, sectors, winners, losers, price, fx_app) {
  const { tax_change: tax = 0, wage_change: wage = 0,
    export_price_change: expPx = 0,
    remittances_change: remit = 0,
    government_spending_change: gov = 0 } = shock;

  const gdpSign   = gdp >= 0 ? 'рост' : 'падение';
  const gdpAbs    = Math.abs(gdp).toFixed(2);
  const priceSign = price >= 0 ? 'ускорение инфляции' : 'дефляционное давление';
  const priceAbs  = Math.abs(price).toFixed(2);

  const lines = [];

  // ── Общая оценка ─────────────────────────────────────────────────────────
  lines.push(`<b>Общая оценка.</b> Моделирование показывает ${gdpSign} ВВП на ${gdpAbs}%. ` +
    `Уровень цен изменится на ${price >= 0 ? '+' : ''}${price.toFixed(2)}% ` +
    `(${priceSign} на ${priceAbs} п.п.).`);

  // ── Анализ каждого ненулевого шока ───────────────────────────────────────
  if (Math.abs(tax) > 0.01) {
    if (tax > 0) {
      lines.push(`<b>Налоговая нагрузка (+${tax}%).</b> Рост налогов удорожает капитал и снижает рентабельность инвестиций. ` +
        `Капиталоёмкие сектора (алюминий, энергетика, горнодобыча) пострадают сильнее. ` +
        `Бюджет получит дополнительные доходы, однако долгосрочный ущерб для ВВП может превысить краткосрочный фискальный выигрыш.`);
    } else {
      lines.push(`<b>Снижение налогов (${tax}%).</b> Уменьшение налоговой нагрузки стимулирует инвестиции и деловую активность. ` +
        `Рост инвестиций поддержит выпуск прежде всего в капиталоёмких секторах. ` +
        `Краткосрочный дефицит бюджета требует компенсирующих мер или внешнего финансирования.`);
    }
  }

  if (Math.abs(wage) > 0.01) {
    if (wage > 0) {
      lines.push(`<b>Рост заработных плат (+${wage}%).</b> Повышение зарплат увеличивает издержки производства, ` +
        `снижая конкурентоспособность трудоёмких секторов (сельское хозяйство, торговля, текстиль). ` +
        `Однако рост располагаемых доходов домохозяйств поддерживает внутренний потребительский спрос, ` +
        `частично компенсируя шок предложения для ориентированных на внутренний рынок секторов.`);
    } else {
      lines.push(`<b>Снижение заработных плат (${wage}%).</b> Падение зарплат снижает потребительский спрос и уровень жизни. ` +
        `Формальное снижение издержек слабо стимулирует экспортные сектора без соответствующего роста производительности.`);
    }
  }

  if (Math.abs(expPx) > 0.01) {
    const fxStr = fx_app > 0.5
      ? ` Укрепление сомони на ~${fx_app.toFixed(1)}% сдерживает выигрыш экспортёров и сжимает маржу несырьевых секторов.`
      : '';
    if (expPx > 0) {
      lines.push(`<b>Рост мировых экспортных цен (+${expPx}%).</b> Алюминий и горнодобывающая отрасль получают наибольший выигрыш ` +
        `через рост валютной выручки. Рост экспортных доходов укрепляет платёжный баланс.${fxStr}`);
    } else {
      lines.push(`<b>Падение мировых экспортных цен (${expPx}%).</b> Алюминий (ТАЛКО) и горнодобыча несут наибольшие потери. ` +
        `Ослабление TJS из-за сокращения валютной выручки вызовет импортируемую инфляцию.`);
    }
  }

  if (Math.abs(remit) > 0.01) {
    if (remit > 0) {
      lines.push(`<b>Рост денежных переводов (+${remit}%).</b> Переводы мигрантов составляют ~28% ВВП Таджикистана — ` +
        `один из крупнейших показателей в мире. Рост переводов стимулирует потребительские расходы, ` +
        `строительство и розничную торговлю. Укрепление сомони снижает стоимость импорта, ` +
        `однако сжимает экспортных конкурентоспособность промышленных секторов.`);
    } else {
      lines.push(`<b>Падение денежных переводов (${remit}%).</b> Сокращение трансфертов из России оказывает значительное ` +
        `негативное воздействие на потребительский спрос и строительный рынок. ` +
        `Давление на TJS усилит инфляционные риски. Рекомендуется активация резервных фондов ` +
        `и программ социальной поддержки.`);
    }
  }

  if (Math.abs(gov) > 0.01) {
    if (gov > 0) {
      lines.push(`<b>Увеличение госрасходов (+${gov}%).</b> Наибольший эффект — в строительстве, энергетике и услугах ` +
        `через прямой государственный спрос. Мультипликатор для Таджикистана оценивается в 1.25 ` +
        `с учётом высокой доли импорта (42% ВВП), что ограничивает внутренние вторичные эффекты. ` +
        `При дефицитном финансировании возможен рост госдолга.`);
    } else {
      lines.push(`<b>Сокращение госрасходов (${gov}%).</b> Строительство и энергетика теряют государственный спрос. ` +
        `Рекомендуется сохранить финансирование социальных программ, сокращая административные расходы.`);
    }
  }

  // ── Победители и проигравшие ─────────────────────────────────────────────
  if (winners.length > 0) {
    lines.push(`<b>Сектора-выигравшие:</b> ${winners.join(', ')}. ` +
      `Рекомендуется приоритетная поддержка этих секторов для закрепления позитивных эффектов.`);
  }
  if (losers.length > 0) {
    lines.push(`<b>Сектора-проигравшие:</b> ${losers.join(', ')}. ` +
      `Требуется адресная политика поддержки: субсидии, налоговые льготы или программы переобучения.`);
  }

  // ── Общие рекомендации ───────────────────────────────────────────────────
  if (gdp < -1.0) {
    lines.push(`<b>Вывод.</b> Совокупный шок несёт значительный риск для роста экономики. ` +
      `НБТ следует рассмотреть смягчение денежно-кредитной политики, Министерству финансов — ` +
      `антициклические расходы в рамках фискального пространства.`);
  } else if (gdp > 1.0) {
    lines.push(`<b>Вывод.</b> Шок оказывает положительное воздействие. ` +
      `Рекомендуется использовать дополнительные доходы для укрепления резервов и ` +
      `инвестиций в инфраструктуру, снижающую зависимость от внешних шоков.`);
  } else {
    lines.push(`<b>Вывод.</b> Эффект умеренный. Секторальные дисбалансы требуют ` +
      `точечных мер поддержки проигравших секторов при сохранении общего макроэкономического курса.`);
  }

  return lines.join(' ');
}

// ─── Основная функция симуляции CGE ─────────────────────────────────────────
//
// Аргумент shock — объект с одним или несколькими полями:
//   tax_change                  — изменение налоговой ставки, %
//   wage_change                 — изменение заработных плат, %
//   export_price_change         — изменение мировых цен на экспорт, %
//   remittances_change          — изменение денежных переводов, %
//   government_spending_change  — изменение государственных расходов, %
//
// Возвращает объект с агрегированными показателями и детализацией по секторам.

function cgeSimulate(shock) {
  const tax  = shock.tax_change                  || 0;
  const wage = shock.wage_change                 || 0;
  const expPx = shock.export_price_change        || 0;
  const remit = shock.remittances_change         || 0;
  const gov   = shock.government_spending_change || 0;

  // ── Эффект обменного курса ────────────────────────────────────────────────
  // Рост экспортных цен и переводов → приток иностранной валюты
  // → укрепление сомони (fx_app > 0 означает укрепление, %).
  const fx_app = expPx * MACRO.export_gdp * 0.35 + remit * MACRO.remit_gdp * 0.20;

  // ── Средняя доля труда по экономике ──────────────────────────────────────
  const avg_alpha = SECTORS.reduce((s, sec) => s + sec.alpha * sec.gdp_share, 0) / 100;

  // ── Расчёт по секторам ────────────────────────────────────────────────────
  const sector_impacts = {};
  let gdp_change   = 0;
  let emp_change   = 0;
  let ex_sum       = 0;
  let im_sum       = 0;
  let govrev_sum   = 0;

  for (const s of SECTORS) {
    const dY = _sectorImpact(s, tax, wage, expPx, remit, gov, fx_app);
    sector_impacts[s.name] = parseFloat(dY.toFixed(2));

    const w = s.gdp_share / 100;

    // Взвешенный ВВП
    gdp_change += w * dY;

    // Занятость: реакция труда через предложение + общий выпуск
    const dL = -0.38 * wage - 0.10 * tax + dY * 0.55;
    emp_change += w * s.alpha * dL;

    // Экспорт: реакция выпуска + прямой ценовой эффект + курс
    // dEx — % изменение экспорта сектора i
    const dEx = s.export_share * (dY * 0.55 + expPx * 0.50 - fx_app * 0.22);
    ex_sum += w * dEx;

    // Импорт: эффект дохода (зарплаты + переводы) + курс
    // income_boost — рост располагаемых доходов в % к базовому уровню
    const income_boost = wage * avg_alpha * 0.4 + remit * MACRO.remit_gdp;
    const dIm = s.import_share * (dY * 0.45 + income_boost * 0.15 - fx_app * 0.28);
    im_sum += w * dIm;

    // Гос. доходы от изменения выпуска (налоговая база)
    govrev_sum += w * s.tax_rate * dY;
  }

  // Прямой эффект изменения ставки на доходы бюджета
  govrev_sum += tax * 0.72;
  // Госрасходы: небольшой эффект изъятия (снижение налоговой базы от перегрева)
  govrev_sum -= gov * MACRO.gov_gdp * 0.04;

  // ── Уровень цен ──────────────────────────────────────────────────────────
  // Cost-push: зарплаты переносятся на цены (доля прибл. avg_alpha)
  const price_wage  = avg_alpha * wage * 0.55;
  // Дефляция от укрепления TJS (перенос курса на внутренние цены)
  const price_fx    = -fx_app * MACRO.fx_pass;
  // Demand-pull: переводы и госрасходы создают спрос
  const price_dem   = (Math.abs(remit) * MACRO.remit_gdp * 0.55 + Math.abs(gov) * MACRO.gov_gdp * 0.40) * 0.32 * Math.sign(remit + gov || 1);
  const price_level_change = price_wage + price_fx + price_dem;

  // ── Доходы домохозяйств ──────────────────────────────────────────────────
  // Три компонента: трудовые доходы (зарплаты) + переводы + эффект занятости
  const hh_wage  = avg_alpha * wage;                // изменение зарплат × доля труда в ВВП
  const hh_remit = MACRO.remit_gdp * remit * 0.80;  // переводы ≈ 28% доходов домохозяйств
  const hh_emp   = emp_change * 0.50;               // занятость → трудовые доходы
  const household_income_change = parseFloat((hh_wage * 0.45 + hh_remit + hh_emp).toFixed(2));

  // ── Победители и проигравшие ─────────────────────────────────────────────
  const sorted = Object.entries(sector_impacts).sort((a, b) => b[1] - a[1]);
  const winners = sorted.filter(([, v]) => v > 0.2).map(([k]) => k);
  const losers  = sorted.filter(([, v]) => v < -0.2).map(([k]) => k);

  return {
    gdp_change:               parseFloat(gdp_change.toFixed(2)),
    household_income_change,
    employment_change:        parseFloat(emp_change.toFixed(2)),
    price_level_change:       parseFloat(price_level_change.toFixed(2)),
    export_change:            parseFloat(ex_sum.toFixed(2)),
    import_change:            parseFloat(im_sum.toFixed(2)),
    government_revenue_change: parseFloat(govrev_sum.toFixed(2)),
    sector_impacts,
    winners,
    losers,
    interpretation: _generateInterpretation(shock, gdp_change, sector_impacts, winners, losers, price_level_change, fx_app),
    // Метаданные
    meta: {
      fx_appreciation: parseFloat(fx_app.toFixed(2)),
      sectors_count:   SECTORS.length,
      sam_year:        2019,
      data_source:     'SAM 2019 + МЭРиТ РТ 1997–2024',
    },
  };
}

// ─── Перекалибровка CGE из прогноза МЭРиТ ───────────────────────────────────

/**
 * calibrateFromForecast() — пересчитывает доли секторов из реальных данных МЭРиТ.
 * Обновляет SECTORS[Сельское хозяйство] и SECTORS[Алюминий], MACRO.export_gdp.
 * Источник: ministry_forecast_2025_2027.json
 */
function calibrateFromForecast() {
  const FORECAST_FILE = path.join(__dirname, 'data', 'ministry_forecast_2025_2027.json');
  try {
    if (!fs.existsSync(FORECAST_FILE)) return { calibrated: false, reason: 'файл не найден' };

    const fc = JSON.parse(fs.readFileSync(FORECAST_FILE, 'utf8')).official_forecast;
    const gdp24  = fc.gdp_mln_somoni?.[2024];
    const agr24  = fc.agriculture_mln?.[2024];
    const ind24  = fc.industry_mln?.[2024];
    const exp24u = fc.export_mln_usd?.[2024];
    const gdp25  = fc.gdp_mln_somoni?.[2025];

    if (!gdp24) return { calibrated: false, reason: 'нет данных ВВП 2024' };

    // Доля сельского хозяйства в ВВП
    if (agr24) {
      const agrShare = Math.round(agr24 / gdp24 * 1000) / 10; // в %
      const agIdx = SECTORS.findIndex(s => s.name === 'Сельское хозяйство');
      if (agIdx >= 0) SECTORS[agIdx].gdp_share = agrShare;
    }

    // Доля алюминия: алюминий = ~7% экспорта; экспорт/ВВП (в сомони, курс ~10.5)
    if (exp24u) {
      const exportGdpShare = Math.round(exp24u * 10.5 / gdp24 * 1000) / 10;
      MACRO.export_gdp = Math.round(exportGdpShare) / 100;
      // Алюминий ≈ 6% экспорта страны (ТАЛКО данные 2024: $161 млн / $2615 млн)
      const alShare = exp24u > 0 ? Math.round(161 / exp24u * gdp24 / gdp24 * 100 * 10) / 10 : 7.0;
      const alIdx = SECTORS.findIndex(s => s.name === 'Алюминий');
      if (alIdx >= 0) SECTORS[alIdx].gdp_share = Math.max(3, Math.min(15, alShare));
    }

    // Нормализуем gdp_share чтобы сумма = 100
    const total = SECTORS.reduce((s, sec) => s + sec.gdp_share, 0);
    if (Math.abs(total - 100) > 1) {
      const factor = 100 / total;
      SECTORS.forEach(s => { s.gdp_share = Math.round(s.gdp_share * factor * 10) / 10; });
    }

    return {
      calibrated:            true,
      base_year_gdp:         gdp24,
      forecast_gdp_2025:     gdp25,
      agriculture_share:     agr24 ? Math.round(agr24 / gdp24 * 1000) / 10 : null,
      export_gdp_ratio:      MACRO.export_gdp,
      sectors_recalibrated:  true,
      source:                'SAM 2019 + МЭРиТ 1997–2024 + Прогноз 2025–2027',
    };
  } catch (e) {
    return { calibrated: false, reason: e.message };
  }
}

// Перекалибровка при загрузке модуля
const _calibration = calibrateFromForecast();

// ─── Обёртка cgeSimulate с полем calibration ────────────────────────────────

const _cgeSimulateBase = cgeSimulate;
function cgeSimulateWithCalibration(shock) {
  const result = _cgeSimulateBase(shock);
  return {
    ...result,
    calibration: {
      source:                'SAM 2019 + МЭРиТ 1997–2024 + Прогноз 2025–2027',
      base_year_gdp:         _calibration.base_year_gdp         ?? 146475,
      forecast_gdp_2025:     _calibration.forecast_gdp_2025     ?? 166065,
      sectors_recalibrated:  _calibration.calibrated             ?? false,
      calibrated_at:         new Date().toISOString(),
    },
  };
}

module.exports = { cgeSimulate: cgeSimulateWithCalibration, cobbDouglas, SECTORS, MACRO, calibrateFromForecast };
