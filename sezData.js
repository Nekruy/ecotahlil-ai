'use strict';

// Реальные данные СЭЗ Таджикистана (источник: Закон РТ о СЭЗ + МЭРиТ РТ)
const SEZ_ZONES = [
  {
    id: 'sugd',
    name_ru: 'СЭЗ «Сугд»',
    name_tj: 'МИЭ «Суғд»',
    name_en: 'FEZ Sugd',
    city: 'Худжанд',
    region: 'Согдийская область',
    established: 2009,
    area_ha: 320,
    residents_count: 52,
    investment_total_mln_usd: 285,
    years_exemption: 25,
    focus: ['текстиль', 'лёгкая промышленность', 'пищевая промышленность',
            'электроника', 'химия', 'логистика'],
    tax_benefits: {
      income_tax: 0,
      vat: 0,
      property_tax: 0,
      customs: 0,
      land_tax: 0,
      social_tax_reduction: 50,
    },
    infrastructure: {
      electricity_available: true,
      gas_available: true,
      water_available: true,
      road_access: true,
      rail_access: true,
      internet: true,
      customs_post: true,
    },
    contacts: {
      phone: '+992 3422 6-08-08',
      email: 'sez.sugd@medt.tj',
      website: 'sez-sugd.tj',
      address: 'г. Худжанд, ул. Истиклол 1',
    },
    key_investors: ['Huawei', 'China National Building Material', 'Kordestan Textile'],
    avg_roi_years: 4.5,
    logistics_score: 8.5,
    labor_cost_usd_month: 180,
    description: 'Крупнейшая СЭЗ страны. Стратегическое расположение на пересечении торговых путей Центральной Азии. Развитая инфраструктура, прямой доступ к железной дороге.',
  },
  {
    id: 'panj',
    name_ru: 'СЭЗ «Панҷ»',
    name_tj: 'МИЭ «Панҷ»',
    name_en: 'FEZ Panj',
    city: 'Пандж',
    region: 'Хатлонская область',
    established: 2010,
    area_ha: 400,
    residents_count: 28,
    investment_total_mln_usd: 142,
    years_exemption: 25,
    focus: ['агропром', 'пищевая промышленность', 'текстиль',
            'строительные материалы', 'торговля с Афганистаном'],
    tax_benefits: {
      income_tax: 0,
      vat: 0,
      property_tax: 0,
      customs: 0,
      land_tax: 0,
      social_tax_reduction: 50,
    },
    infrastructure: {
      electricity_available: true,
      gas_available: false,
      water_available: true,
      road_access: true,
      rail_access: false,
      internet: true,
      customs_post: true,
    },
    contacts: {
      phone: '+992 3252 2-22-22',
      email: 'sez.panj@medt.tj',
      address: 'г. Пандж, ул. Дружбы 5',
    },
    key_investors: ['Afghan-Tajik Investments', 'Khatlon Agro'],
    avg_roi_years: 5.0,
    logistics_score: 6.5,
    labor_cost_usd_month: 150,
    description: 'Приграничная СЭЗ с Афганистаном. Уникальный доступ к афганскому рынку (население 40 млн). Специализация на агропроме и трансграничной торговле.',
  },
  {
    id: 'dangara',
    name_ru: 'СЭЗ «Дангара»',
    name_tj: 'МИЭ «Данғара»',
    name_en: 'FEZ Dangara',
    city: 'Дангара',
    region: 'Хатлонская область',
    established: 2010,
    area_ha: 500,
    residents_count: 31,
    investment_total_mln_usd: 168,
    years_exemption: 25,
    focus: ['химическая промышленность', 'нефтепереработка',
            'удобрения', 'строительные материалы', 'металлургия'],
    tax_benefits: {
      income_tax: 0,
      vat: 0,
      property_tax: 0,
      customs: 0,
      land_tax: 0,
      social_tax_reduction: 50,
    },
    infrastructure: {
      electricity_available: true,
      gas_available: true,
      water_available: true,
      road_access: true,
      rail_access: false,
      internet: true,
      customs_post: true,
    },
    contacts: {
      phone: '+992 3227 2-34-56',
      email: 'sez.dangara@medt.tj',
      address: 'г. Дангара, промышленная зона',
    },
    key_investors: ['Tajik Nitrogen', 'China Road Bridge Corporation'],
    avg_roi_years: 5.5,
    logistics_score: 7.0,
    labor_cost_usd_month: 160,
    description: 'Промышленная СЭЗ с фокусом на химию и переработку. Близость к запасам природных ресурсов. Поддержка крупных промышленных проектов.',
  },
  {
    id: 'ishkashim',
    name_ru: 'СЭЗ «Ишкошим»',
    name_tj: 'МИЭ «Ишкошим»',
    name_en: 'FEZ Ishkashim',
    city: 'Ишкошим',
    region: 'ГБАО',
    established: 2011,
    area_ha: 200,
    residents_count: 8,
    investment_total_mln_usd: 28,
    years_exemption: 25,
    focus: ['туризм', 'горнодобывающая промышленность',
            'экотуризм', 'торговля', 'традиционные ремёсла'],
    tax_benefits: {
      income_tax: 0,
      vat: 0,
      property_tax: 0,
      customs: 0,
      land_tax: 0,
      social_tax_reduction: 50,
    },
    infrastructure: {
      electricity_available: true,
      gas_available: false,
      water_available: true,
      road_access: true,
      rail_access: false,
      internet: true,
      customs_post: true,
    },
    contacts: {
      phone: '+992 3522 2-11-11',
      email: 'sez.ishkashim@medt.tj',
      address: 'г. Ишкошим, центральная площадь',
    },
    key_investors: ['Aga Khan Development Network', 'FOCUS Humanitarian'],
    avg_roi_years: 6.0,
    logistics_score: 4.5,
    labor_cost_usd_month: 130,
    description: 'СЭЗ в уникальном горном регионе Памира. Граница с Афганистаном. Потенциал для экотуризма и горнодобычи. Поддержка Ага Хан Фонда.',
  },
  {
    id: 'himzovar',
    name_ru: 'СЭЗ «Химзовар»',
    name_tj: 'МИЭ «Химзовар»',
    name_en: 'FEZ Himzovar',
    city: 'Душанбе',
    region: 'Районы РРП',
    established: 2014,
    area_ha: 150,
    residents_count: 9,
    investment_total_mln_usd: 45,
    years_exemption: 25,
    focus: ['фармацевтика', 'биотехнологии', 'химия',
            'медицинское оборудование', 'косметика'],
    tax_benefits: {
      income_tax: 0,
      vat: 0,
      property_tax: 0,
      customs: 0,
      land_tax: 0,
      social_tax_reduction: 50,
    },
    infrastructure: {
      electricity_available: true,
      gas_available: true,
      water_available: true,
      road_access: true,
      rail_access: false,
      internet: true,
      customs_post: false,
    },
    contacts: {
      phone: '+992 37 221-34-56',
      email: 'sez.himzovar@medt.tj',
      address: 'Душанбе, пригородная зона',
    },
    key_investors: ['Tajik Pharma', 'Iranian Pharmaceutical Group'],
    avg_roi_years: 4.0,
    logistics_score: 8.0,
    labor_cost_usd_month: 200,
    description: 'Специализированная фармацевтическая СЭЗ вблизи столицы. Доступ к научным кадрам ТНУ и ТГМУ. Фокус на импортозамещении лекарств.',
  },
];

// Стандартные налоги ВНЕ СЭЗ для сравнительного расчёта ROI
const STANDARD_TAXES = {
  income_tax_pct: 23,
  vat_pct: 15,
  property_tax_pct: 1,
  customs_pct: 5,
  social_tax_pct: 25,
};

function getAllZones() {
  return SEZ_ZONES;
}

function getZoneById(id) {
  return SEZ_ZONES.find(z => z.id === id) || null;
}

// Подбор СЭЗ по сектору, региону и объёму инвестиций
function matchSEZ(sector, region, investment_mln) {
  const scored = SEZ_ZONES.map(z => {
    let score = 0;

    const sectorLower = (sector || '').toLowerCase();
    const focusMatch = z.focus.some(f =>
      f.toLowerCase().includes(sectorLower) ||
      sectorLower.includes(f.toLowerCase().split(' ')[0])
    );
    if (focusMatch) score += 40;

    if (region && z.region.toLowerCase().includes(region.toLowerCase())) score += 20;

    const infra = z.infrastructure;
    if (infra.electricity_available) score += 5;
    if (infra.gas_available) score += 5;
    if (infra.rail_access) score += 10;
    if (infra.customs_post) score += 10;

    score += z.logistics_score;

    // Штраф за малый размер зоны при крупных инвестициях
    if (investment_mln && investment_mln > 50 && z.area_ha < 200) score -= 10;

    return { ...z, match_score: Math.min(Math.round(score), 99) };
  });

  return scored.sort((a, b) => b.match_score - a.match_score);
}

// Расчёт ROI / NPV с учётом налоговых льгот СЭЗ (discount_rate = 12%)
function calcROI(sez_id, investment_usd, revenue_annual_usd, costs_annual_usd, years) {
  const zone = getZoneById(sez_id);
  if (!zone) return null;

  const costs_annual  = costs_annual_usd || revenue_annual_usd * 0.6;
  const profit_annual = revenue_annual_usd - costs_annual;
  const exempt_years  = Math.min(years, zone.years_exemption);

  // Налоги ВНЕ СЭЗ (годовые)
  const income_tax_annual = profit_annual > 0
    ? profit_annual * (STANDARD_TAXES.income_tax_pct / 100) : 0;
  const vat_annual = revenue_annual_usd * (STANDARD_TAXES.vat_pct / 100);

  const net_without = profit_annual - income_tax_annual;
  const net_with    = profit_annual; // 0% в СЭЗ

  // NPV с дисконтом 12%
  const DR = 0.12;
  let npv_without = -investment_usd;
  let npv_with    = -investment_usd;
  for (let t = 1; t <= years; t++) {
    const df = Math.pow(1 + DR, t);
    npv_without += net_without / df;
    npv_with    += (t <= exempt_years ? net_with : net_without) / df;
  }

  // Суммарная экономия за период льгот
  const income_tax_saved = Math.round(income_tax_annual * exempt_years);
  const vat_saved        = Math.round(vat_annual        * exempt_years);
  const tax_savings_usd  = income_tax_saved + vat_saved;

  // ROI за весь горизонт
  const total_net_without = net_without * years;
  const total_net_with    = net_with * exempt_years + net_without * Math.max(0, years - exempt_years);
  const roi_without = investment_usd > 0
    ? ((total_net_without - investment_usd) / investment_usd * 100).toFixed(1) : '0.0';
  const roi_with = investment_usd > 0
    ? ((total_net_with    - investment_usd) / investment_usd * 100).toFixed(1) : '0.0';

  const pb_without = net_without > 0 ? investment_usd / net_without : null;
  const pb_with    = net_with    > 0 ? investment_usd / net_with    : null;

  let recommendation;
  const roiNum = parseFloat(roi_with);
  if (roiNum > 100)     recommendation = 'Высокорентабельный проект. Льготы СЭЗ обеспечивают значительное преимущество.';
  else if (roiNum > 30) recommendation = 'Умеренная рентабельность. Налоговые льготы ускоряют окупаемость.';
  else if (roiNum > 0)  recommendation = 'Проект в плюсе. Льготы СЭЗ повышают эффективность.';
  else                  recommendation = 'Отрицательный ROI. Рекомендуем пересмотреть бизнес-модель или изучить субсидии.';

  return {
    sez:                    zone.name_ru,
    sez_id:                 zone.id,
    investment_usd,
    revenue_annual_usd,
    costs_annual_usd:       costs_annual,
    years,
    roi_without_sez:        roi_without + '%',
    roi_with_sez:           roi_with    + '%',
    roi_improvement_pct:    (parseFloat(roi_with) - parseFloat(roi_without)).toFixed(1),
    tax_savings_usd,
    tax_savings_annual_usd: Math.round(income_tax_annual + vat_annual),
    tax_breakdown: {
      income_tax_saved,
      vat_saved,
      total: tax_savings_usd,
    },
    npv_without_sez_usd:       Math.round(npv_without),
    npv_with_sez_usd:          Math.round(npv_with),
    payback_without_sez:       pb_without ? pb_without.toFixed(1) + ' лет' : 'N/A',
    payback_with_sez:          pb_with    ? pb_with.toFixed(1)    + ' лет' : 'N/A',
    payback_years_without_sez: pb_without ? pb_without.toFixed(1) : null,
    payback_years_with_sez:    pb_with    ? pb_with.toFixed(1)    : null,
    payback_years:             pb_with    ? pb_with.toFixed(1)    : null,
    years_exemption:           zone.years_exemption,
    zone_info: {
      infrastructure:       zone.infrastructure,
      logistics_score:      zone.logistics_score,
      labor_cost_usd_month: zone.labor_cost_usd_month,
    },
    recommendation,
    standard_taxes: STANDARD_TAXES,
  };
}

// Сводная статистика по всем СЭЗ
function getSummaryStats() {
  return {
    total_zones: SEZ_ZONES.length,
    total_residents: SEZ_ZONES.reduce((s, z) => s + z.residents_count, 0),
    total_investment_mln_usd: SEZ_ZONES.reduce((s, z) => s + z.investment_total_mln_usd, 0),
    total_area_ha: SEZ_ZONES.reduce((s, z) => s + z.area_ha, 0),
    avg_exemption_years: 25,
    tax_benefits_summary: '0% налог на прибыль, НДС, таможню, имущество · 50% снижение соцналога · на 25 лет',
  };
}

module.exports = {
  getAllZones,
  getZoneById,
  matchSEZ,
  calcROI,
  getSummaryStats,
  STANDARD_TAXES,
};
