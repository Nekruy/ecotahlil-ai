'use strict';
const fs   = require('fs');
const path = require('path');

function r2(v) {
  const n = parseFloat(v);
  return (!isNaN(n) && isFinite(n)) ? Math.round(n * 100) / 100 : null;
}

// ── Читаем CSV в массив объектов ─────────────────────────────────────────────
function readCSV(filepath) {
  const lines = fs.readFileSync(filepath, 'utf8').replace(/^﻿/, '').trim().split('\n');
  const header = lines[0].split(',');
  return lines.slice(1).map(line => {
    const parts = line.split(',');
    const obj = {};
    header.forEach((h, i) => { obj[h.trim()] = parts[i]?.trim() ?? ''; });
    return obj;
  });
}

// ── Читаем long-format CSV в map: indicator → {year → {value, yoy}} ─────────
function readLongCSV(filepath) {
  const lines = fs.readFileSync(filepath, 'utf8').replace(/^﻿/, '').trim().split('\n');
  // header: year,indicator,unit,value,yoy_pct
  const result = {}; // indicator → year → {value, yoy}
  lines.slice(1).forEach(line => {
    const parts = line.split(',');
    const year  = parseInt(parts[0]);
    const ind   = parts[1]?.trim();
    const val   = parseFloat(parts[3]);
    const yoy   = parseFloat(parts[4]);
    if (!ind || isNaN(year)) return;
    if (!result[ind]) result[ind] = {};
    if (!result[ind][year]) result[ind][year] = { value: isNaN(val) ? null : val, yoy: isNaN(yoy) ? null : yoy };
  });
  return result;
}

async function main() {

  // ══════════════════════════════════════════════════════════════════════════
  // [1/3] ИСТОЧНИК: data/final/master_dataset.csv (GDP 1997–2035, 39 лет)
  // ══════════════════════════════════════════════════════════════════════════
  console.log('\n[1/3] Читаю master_dataset.csv (GDP 1997-2035)...');
  const masterRows = readCSV('data/final/master_dataset.csv');

  const gdp_extended = masterRows.map(r => {
    const gdpMln  = r2(r['gdpmpt']);
    const growIdx = r2(r['ggdpt']);     // 108.35 → рост 8.35%
    const cpiIdx  = r2(r['ипц_в_том_числе_декдек_факт']); // 104.2 → 4.2%
    return {
      year:               parseInt(r.year),
      gdp_mln_somoni:     gdpMln,
      gdp_bln_somoni:     gdpMln != null ? r2(gdpMln / 1000) : null,
      gdp_growth_pct:     growIdx != null ? r2(growIdx - 100) : null,
      gdp_deflator:       r2(r['defgdpt']),
      gdp_per_capita_usd: r2(r['gdppcust']),
      cpi_index:          cpiIdx,
      cpi_growth_pct:     cpiIdx != null ? r2(cpiIdx - 100) : null,
    };
  }).filter(r => r.gdp_mln_somoni != null && r.year >= 1997);

  console.log(`   ✅ GDP: ${gdp_extended.length} лет (${gdp_extended[0].year}–${gdp_extended[gdp_extended.length-1].year})`);
  console.log('   Последние 5 лет:');
  gdp_extended.slice(-5).forEach(r =>
    console.log(`      ${r.year}: ВВП=${r.gdp_mln_somoni} млн | рост=${r.gdp_growth_pct}% | инфл=${r.cpi_growth_pct}%`)
  );

  // ══════════════════════════════════════════════════════════════════════════
  // [2/3] ИСТОЧНИК: data/cleaned/cge_model__Итог_Модел_CGE.csv (2022–2032)
  // ══════════════════════════════════════════════════════════════════════════
  console.log('\n[2/3] Читаю CGE cleaned CSV (2022-2032)...');
  const cgeData = readLongCSV('data/cleaned/cge_model__Итог_Модел_CGE.csv');

  // Маппинг индикаторов (таджикские названия → поля)
  const IND_GDP    = 'Маљмўи мањсулоти дохилї';
  const IND_INFL   = 'Таваррум';
  const IND_USD    = 'Ќурби миёнасолона (1 $ нисбат ба сомонї)*';
  const IND_EXP    = 'аз он: - содирот';
  const IND_IMP    = '-воридот';
  const IND_POP    = 'Шумораи ањолї (ба њисоби миёнаи солона)';
  const IND_IND    = 'Њаљми мањсулоти саноатї';
  const IND_AGR    = 'Њаљми мањсулоти кишоварзї';
  const IND_INV    = 'Маблаѓгузорї ба сармояи асосї аз њисоби њамаи манбаъњои маблаѓгузорї';

  const cgeYears = [2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032];

  const cge_forecast = cgeYears.map(yr => {
    const get = (ind, field='value') => cgeData[ind]?.[yr]?.[field] ?? null;
    const usdRate = get(IND_USD);
    return {
      year:            yr,
      gdp_mln_somoni:  r2(get(IND_GDP)),
      gdp_growth_pct:  r2((get(IND_GDP, 'yoy') ?? 0) - 100) || null,
      inflation_pct:   r2(((get(IND_INFL, 'yoy') ?? 100)) - 100) || null,
      usd_tjs_rate:    r2(usdRate),
      export_mln_usd:  r2(get(IND_EXP)),
      import_mln_usd:  r2(get(IND_IMP)),
      trade_balance:   (get(IND_EXP) != null && get(IND_IMP) != null)
                         ? r2(get(IND_EXP) - get(IND_IMP)) : null,
      population_thou: r2(get(IND_POP)),
      industry_mln:    r2(get(IND_IND)),
      agriculture_mln: r2(get(IND_AGR)),
      investment_mln:  r2(get(IND_INV)),
      source: 'CGE Model МЭРиТ',
    };
  }).filter(r => r.gdp_mln_somoni != null);

  console.log(`   ✅ CGE: ${cge_forecast.length} лет (2022–2032)`);
  cge_forecast.forEach(r =>
    console.log(`      ${r.year}: ВВП=${r.gdp_mln_somoni} | курс=${r.usd_tjs_rate} | экспорт=${r.export_mln_usd} | импорт=${r.import_mln_usd}`)
  );

  // ══════════════════════════════════════════════════════════════════════════
  // [3/3] СБОРКА И СОХРАНЕНИЕ
  // ══════════════════════════════════════════════════════════════════════════
  console.log('\n[3/3] Собираю new_ministry_data.json...');

  // Тайм-серии для моделей (только ненулевые)
  const histRows  = gdp_extended.filter(r => r.year <= 2024);
  const timeseries = {
    years:          histRows.map(r => r.year),
    gdp_mln:        histRows.map(r => r.gdp_mln_somoni),
    gdp_bln:        histRows.map(r => r.gdp_bln_somoni),
    gdp_growth:     histRows.map(r => r.gdp_growth_pct),
    inflation:      histRows.map(r => r.cpi_growth_pct),
    gdp_per_capita: histRows.map(r => r.gdp_per_capita_usd),
    deflator:       histRows.map(r => r.gdp_deflator),
  };

  // Курс USD/TJS из CGE (2022–2032)
  const usd_tjs_series = cge_forecast
    .filter(r => r.usd_tjs_rate != null)
    .map(r => ({ year: r.year, rate: r.usd_tjs_rate, source: 'CGE Model МЭРиТ' }));

  const result = {
    meta: {
      source: 'МЭРиТ РТ — Model_GDP_2035.xlsx + CGE_Model_2025_2032.xlsx',
      imported_at: new Date().toISOString(),
      files: ['Model_GDP_2035.xlsx', 'CGE_Model_2025_2032.xlsx'],
      processed_via: ['data/final/master_dataset.csv', 'data/cleaned/cge_model__Итог_Модел_CGE.csv'],
      gdp_period: `${gdp_extended[0].year}–${gdp_extended[gdp_extended.length-1].year}`,
      gdp_years_count: gdp_extended.length,
    },
    gdp_extended,
    cge_forecast,
    timeseries,
    usd_tjs_series,
  };

  fs.writeFileSync('data/new_ministry_data.json', JSON.stringify(result, null, 2));
  const sizeMB = (fs.statSync('data/new_ministry_data.json').size / 1024).toFixed(0);
  console.log(`   ✅ new_ministry_data.json сохранён (${sizeMB} KB)`);

  // ── Обновляем unified_dataset.json ────────────────────────────────────────
  console.log('\nОбновляю unified_dataset.json...');
  const ud = JSON.parse(fs.readFileSync('data/unified_dataset.json', 'utf8'));

  const existingYears = new Set(ud.annual.map(r => r.year));
  const cgeByYear = Object.fromEntries(cge_forecast.map(r => [r.year, r]));

  // 1. Обогащаем уже существующие записи (1995–2024) данными из master + CGE
  ud.annual = ud.annual.map(rec => {
    const ext = gdp_extended.find(r => r.year === rec.year);
    const cge = cgeByYear[rec.year];
    if (ext) {
      // Обновляем/дополняем из нового Excel (более свежий источник)
      rec.gdp_mln_somoni    = ext.gdp_mln_somoni  ?? rec.gdp_mln_somoni;
      rec.gdp_bln_somoni    = ext.gdp_bln_somoni  ?? rec.gdp_bln_somoni;
      rec.gdp_growth_mert   = ext.gdp_growth_pct  ?? rec.gdp_growth_mert;
      rec.gdp_per_capita_usd = ext.gdp_per_capita_usd ?? rec.gdp_per_capita_usd;
      rec.gdp_deflator      = ext.gdp_deflator    ?? rec.gdp_deflator;
      rec.inflation_mert    = ext.cpi_growth_pct  ?? rec.inflation_mert;
    }
    if (cge) {
      rec.usd_tjs_rate    = cge.usd_tjs_rate    ?? rec.usd_tjs_rate;
      rec.export_mln_usd  = cge.export_mln_usd  ?? rec.export_mln_usd;
      rec.import_mln_usd  = cge.import_mln_usd  ?? rec.import_mln_usd;
      rec.industry_mln    = cge.industry_mln    ?? rec.industry_mln;
      rec.agriculture_mln = cge.agriculture_mln ?? rec.agriculture_mln;
    }
    return rec;
  });

  // 2. Добавляем новые годы (2025–2035) из master_dataset + CGE
  const newYears = gdp_extended.filter(r => r.year >= 2025 && !existingYears.has(r.year));
  newYears.forEach(ext => {
    const cge = cgeByYear[ext.year] || {};
    ud.annual.push({
      year:               ext.year,
      gdp_mln_somoni:     ext.gdp_mln_somoni,
      gdp_bln_somoni:     ext.gdp_bln_somoni,
      gdp_growth_mert:    ext.gdp_growth_pct,
      gdp_growth_imf:     null,
      gdp_usd_bln:        null,
      gdp_per_capita_usd: ext.gdp_per_capita_usd,
      inflation_mert:     ext.cpi_growth_pct,
      inflation_imf:      null,
      inflation_wb:       null,
      export_mln_usd:     cge.export_mln_usd    ?? null,
      import_mln_usd:     cge.import_mln_usd    ?? null,
      trade_balance:      cge.trade_balance     ?? null,
      usd_tjs_rate:       cge.usd_tjs_rate      ?? null,
      industry_mln:       cge.industry_mln      ?? null,
      agriculture_mln:    cge.agriculture_mln   ?? null,
      investment_mln:     cge.investment_mln    ?? null,
      population_thou:    cge.population_thou   ?? null,
      gdp_deflator:       ext.gdp_deflator,
      source:             'МЭРиТ CGE+GDP 2035',
    });
  });

  ud.annual.sort((a, b) => a.year - b.year);
  ud.meta.version    = '2.0';
  ud.meta.updated_at = new Date().toISOString();
  ud.meta.quality.annual_records = ud.annual.length;
  ud.meta.quality.annual_period  =
    ud.annual[0].year + '–' + ud.annual[ud.annual.length - 1].year;
  ud.cge_forecast = cge_forecast;

  fs.writeFileSync('data/unified_dataset.json', JSON.stringify(ud, null, 2));

  const allYears = ud.annual.map(r => r.year);
  console.log(`   ✅ unified_dataset обновлён: ${ud.annual.length} записей, ${Math.min(...allYears)}–${Math.max(...allYears)}`);
  console.log(`   Новых записей: ${newYears.length} (${newYears.map(r=>r.year).join(', ')})`);

  // ── Итоговый отчёт ────────────────────────────────────────────────────────
  const gdpSeries  = ud.annual.filter(r => r.year <= 2024 && r.gdp_growth_mert != null).map(r => r.gdp_growth_mert);
  const inflSeries = ud.annual.filter(r => r.year <= 2024 && r.inflation_mert  != null).map(r => r.inflation_mert);
  const usdSeries  = ud.daily_rates?.map(r => r.usd_tjs) ?? [];

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║        ДАТАСЕТ СОЗДАН УСПЕШНО            ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log('║ GDP 1997–2035:', gdp_extended.length, 'лет');
  console.log('║ CGE 2022–2032:', cge_forecast.length, 'лет');
  console.log('║ unified_dataset:', ud.annual.length, 'записей (1995–2035)');
  console.log('╚══════════════════════════════════════════╝');
  console.log('\n📊 ДАННЫЕ ДЛЯ МОДЕЛЕЙ (исторические до 2024):');
  console.log(`ARIMA ВВП:      ${gdpSeries.length} точек ${gdpSeries.length >= 20 ? '✅' : '⚠️'}`);
  console.log(`ARIMA Инфляция: ${inflSeries.length} точек ${inflSeries.length >= 20 ? '✅' : '⚠️'}`);
  console.log(`GARCH USD/TJS:  ${usdSeries.length} точек ${usdSeries.length >= 100 ? '✅' : '⚠️'}`);
  console.log(`VAR совместных: ${Math.min(gdpSeries.length, inflSeries.length)} точек`);
}

main().catch(console.error);
