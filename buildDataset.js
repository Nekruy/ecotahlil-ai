'use strict';
const fs   = require('fs');
const path = require('path');
const https = require('https');

// ── Вспомогательные функции ───────────────────

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: {'User-Agent':'EcotahlilAI/2.0'} }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try { resolve(JSON.parse(d)); }
        catch(e) { reject(new Error('JSON parse error: ' + url)); }
      });
    }).on('error', reject)
      .setTimeout(15000, function(){ this.destroy(); reject(new Error('timeout: '+url)); });
  });
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: {'User-Agent':'EcotahlilAI/2.0'} }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(d));
    }).on('error', reject)
      .setTimeout(15000, function(){ this.destroy(); reject(new Error('timeout')); });
  });
}

// ── ИСТОЧНИК 1: МЭРиТ локальные данные ──────────

async function loadMinistryData() {
  console.log('\n[1/6] Загружаю данные МЭРиТ (локально)...');
  const result = { gdp: [], inflation: [], trade: [], industry: [], agriculture: [] };

  try {
    const m = JSON.parse(fs.readFileSync('data/ministry_gdp_model.json','utf8'));

    result.gdp = (m.gdp || []).map(r => ({
      year: r.year,
      gdp_mln_somoni: r.gdp_mln_somoni,
      gdp_bln_somoni: r.gdp_bln_somoni,
      gdp_growth_pct: r.gdp_growth,
      gdp_deflator: r.gdp_deflator,
      gdp_per_capita_usd: r.gdp_per_capita_usd,
      population_thou: r.population_thou,
      source: 'МЭРиТ РТ'
    }));

    result.inflation = (m.inflation || []).map(r => ({
      year: r.year,
      cpi: r.cpi,
      cpi_growth_pct: r.cpi_growth_pct,
      source: 'МЭРиТ РТ'
    }));

    result.trade = (m.trade || []).map(r => ({
      year: r.year,
      export_mln_usd: r.export_mln_usd,
      import_mln_usd: r.import_mln_usd,
      balance_mln_usd: r.balance_mln_usd,
      source: 'МЭРиТ РТ'
    }));

    result.industry    = (m.industry    || []).map(r => ({...r, source:'МЭРиТ РТ'}));
    result.agriculture = (m.agriculture || []).map(r => ({...r, source:'МЭРиТ РТ'}));

    console.log('   ✅ ВВП:', result.gdp.length, 'лет | Инфляция:', result.inflation.length, '| Торговля:', result.trade.length);
  } catch(e) {
    console.log('   ❌ МЭРиТ:', e.message);
  }
  return result;
}

// ── ИСТОЧНИК 2: МВФ API ──────────────────────────

async function loadIMFData() {
  console.log('\n[2/6] Загружаю данные МВФ (API)...');
  const result = {};

  const indicators = {
    'NGDP_RPCH': 'gdp_growth_imf',
    'PCPIPCH':   'inflation_imf',
    'BCA_NGDPD': 'current_account_gdp',
    'GGXWDG_NGDP': 'gov_debt_gdp',
    'LUR':       'unemployment',
    'NGDPDPC':   'gdp_per_capita_usd',
  };

  for (const [code, name] of Object.entries(indicators)) {
    try {
      const url = 'https://www.imf.org/external/datamapper/api/v1/' + code + '/TJK';
      const data = await fetchJSON(url);
      const values = data?.values?.[code]?.TJK || {};
      result[name] = Object.entries(values)
        .filter(([yr, val]) => val !== null && !isNaN(val))
        .map(([yr, val]) => ({ year: parseInt(yr), value: parseFloat(val), source: 'МВФ' }))
        .sort((a,b) => a.year - b.year);
      console.log('   ✅ МВФ', code, ':', result[name].length, 'точек');
    } catch(e) {
      console.log('   ⚠️  МВФ', code, ':', e.message);
      result[name] = [];
    }
    await new Promise(r => setTimeout(r, 300));
  }
  return result;
}

// ── ИСТОЧНИК 3: Мировой банк API ─────────────────

async function loadWorldBankData() {
  console.log('\n[3/6] Загружаю данные Мирового банка (API)...');
  const result = {};

  const indicators = {
    'FP.CPI.TOTL.ZG':  'inflation_wb',
    'NY.GDP.MKTP.CD':  'gdp_usd',
    'NY.GDP.PCAP.CD':  'gdp_per_capita_wb',
    'BX.TRF.PWKR.CD.DT': 'remittances_usd',
    'NE.EXP.GNFS.ZS':  'exports_pct_gdp',
    'NE.IMP.GNFS.ZS':  'imports_pct_gdp',
    'SL.UEM.TOTL.ZS':  'unemployment_wb',
    'SP.POP.TOTL':     'population',
  };

  for (const [code, name] of Object.entries(indicators)) {
    try {
      const url = 'https://api.worldbank.org/v2/country/TJ/indicator/' + code +
        '?format=json&per_page=50&mrv=30';
      const data = await fetchJSON(url);
      const series = (data?.[1] || [])
        .filter(r => r.value !== null)
        .map(r => ({ year: parseInt(r.date), value: r.value, source: 'Мировой банк' }))
        .sort((a,b) => a.year - b.year);
      result[name] = series;
      console.log('   ✅ WB', code, ':', series.length, 'точек');
    } catch(e) {
      console.log('   ⚠️  WB', code, ':', e.message);
      result[name] = [];
    }
    await new Promise(r => setTimeout(r, 200));
  }
  return result;
}

// ── ИСТОЧНИК 4: НБТ курсы ────────────────────────

async function loadNBTRates() {
  console.log('\n[4/6] Загружаю курсы НБТ...');

  let existingRates = [];
  try {
    existingRates = JSON.parse(fs.readFileSync('data/rates_timeseries.json','utf8'));
    const realRates = existingRates.filter(r => r.source === 'nbt');
    console.log('   Локально:', existingRates.length, 'записей | Реальных НБТ:', realRates.length);
  } catch(e) {}

  try {
    const xml = await fetchText('https://nbt.tj/export/nbt.xml');
    const usdMatch = xml.match(/<CharCode>USD<\/CharCode>[\s\S]*?<Value>([\d.]+)<\/Value>/);
    const eurMatch = xml.match(/<CharCode>EUR<\/CharCode>[\s\S]*?<Value>([\d.]+)<\/Value>/);
    const rubMatch = xml.match(/<CharCode>RUB<\/CharCode>[\s\S]*?<Value>([\d.]+)<\/Value>/);

    if (usdMatch) {
      const today = new Date().toISOString().slice(0,10);
      const newRate = {
        date: today,
        usd: parseFloat(usdMatch[1]),
        eur: eurMatch ? parseFloat(eurMatch[1]) : null,
        rub: rubMatch ? parseFloat(rubMatch[1]) : null,
        source: 'nbt'
      };

      const exists = existingRates.find(r => r.date === today);
      if (!exists) {
        existingRates.push(newRate);
        existingRates.sort((a,b) => a.date.localeCompare(b.date));
        fs.writeFileSync('data/rates_timeseries.json', JSON.stringify(existingRates, null, 2));
      }
      console.log('   ✅ НБТ live: USD=' + newRate.usd + ' EUR=' + newRate.eur + ' RUB=' + newRate.rub);
    }
  } catch(e) {
    console.log('   ⚠️  НБТ API недоступен:', e.message);
  }

  return existingRates;
}

// ── ИСТОЧНИК 5: ОФИЦ. ПРОГНОЗ МЭРиТ 2025-2027 ───

async function loadForecastData() {
  console.log('\n[5/6] Загружаю официальный прогноз МЭРиТ...');
  try {
    const f = JSON.parse(fs.readFileSync('data/ministry_forecast_2025_2027.json','utf8'));
    const gdp = f.official_forecast?.gdp_mln_somoni;
    console.log('   ✅ Прогноз МЭРиТ: ВВП 2025='+gdp?.[2025]+' 2026='+gdp?.[2026]+' 2027='+gdp?.[2027]);
    return f;
  } catch(e) {
    console.log('   ❌', e.message);
    return null;
  }
}

// ── СБОРКА ЕДИНОГО ДАТАСЕТА ──────────────────────

async function buildUnifiedDataset() {
  console.log('\n[6/6] Собираю единый датасет...');

  const [ministry, imf, wb, rates, forecast] = await Promise.all([
    loadMinistryData(),
    loadIMFData(),
    loadWorldBankData(),
    loadNBTRates(),
    loadForecastData(),
  ]);

  const years = [...new Set([
    ...ministry.gdp.map(r=>r.year),
    ...(imf.gdp_growth_imf||[]).map(r=>r.year),
    ...(wb.gdp_usd||[]).map(r=>r.year),
  ])].sort();

  const unified_annual = years.map(year => {
    const mert    = ministry.gdp.find(r=>r.year===year) || {};
    const mertInfl= ministry.inflation.find(r=>r.year===year) || {};
    const mertTr  = ministry.trade.find(r=>r.year===year) || {};
    const imfGDP  = (imf.gdp_growth_imf||[]).find(r=>r.year===year);
    const imfInfl = (imf.inflation_imf||[]).find(r=>r.year===year);
    const imfPC   = (imf.gdp_per_capita_usd||[]).find(r=>r.year===year);
    const wbGDP   = (wb.gdp_usd||[]).find(r=>r.year===year);
    const wbInfl  = (wb.inflation_wb||[]).find(r=>r.year===year);
    const wbRemit = (wb.remittances_usd||[]).find(r=>r.year===year);
    const wbPop   = (wb.population||[]).find(r=>r.year===year);

    return {
      year,
      gdp_mln_somoni:      mert.gdp_mln_somoni    || null,
      gdp_bln_somoni:      mert.gdp_bln_somoni    || null,
      gdp_growth_mert:     mert.gdp_growth_pct    || null,
      gdp_growth_imf:      imfGDP?.value          || null,
      gdp_usd_bln:         wbGDP ? wbGDP.value/1e9 : null,
      gdp_per_capita_usd:  mert.gdp_per_capita_usd || imfPC?.value || null,
      inflation_mert:      mertInfl.cpi_growth_pct || null,
      inflation_imf:       imfInfl?.value          || null,
      inflation_wb:        wbInfl?.value           || null,
      export_mln_usd:      mertTr.export_mln_usd  || null,
      import_mln_usd:      mertTr.import_mln_usd  || null,
      trade_balance:       mertTr.balance_mln_usd || null,
      remittances_mln_usd: wbRemit ? wbRemit.value/1e6 : null,
      population_thou:     mert.population_thou   || (wbPop ? wbPop.value/1e3 : null),
      gdp_deflator:        mert.gdp_deflator      || null,
    };
  });

  const daily_rates = rates.map(r => ({
    date: r.date,
    usd_tjs: r.usd,
    eur_tjs: r.eur,
    rub_tjs: r.rub,
    source: r.source || 'synthetic'
  })).filter(r => r.usd_tjs);

  const official_forecast = forecast?.official_forecast || null;

  const quality = {
    annual_records: unified_annual.length,
    annual_period: years[0] + '—' + years[years.length-1],
    daily_rates_count: daily_rates.length,
    real_nbt_count: daily_rates.filter(r=>r.source==='nbt').length,
    gdp_completeness: Math.round(unified_annual.filter(r=>r.gdp_mln_somoni).length / unified_annual.length * 100) + '%',
    inflation_completeness: Math.round(unified_annual.filter(r=>r.inflation_mert||r.inflation_imf).length / unified_annual.length * 100) + '%',
    sources: ['МЭРиТ РТ', 'МВФ', 'Мировой банк', 'НБТ РТ'],
  };

  const dataset = {
    meta: {
      name: 'EcotahlilAI Unified Dataset',
      version: '1.0',
      built_at: new Date().toISOString(),
      quality,
    },
    annual: unified_annual,
    daily_rates,
    official_forecast,
  };

  fs.writeFileSync('data/unified_dataset.json', JSON.stringify(dataset, null, 2));

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║        ДАТАСЕТ СОЗДАН УСПЕШНО            ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log('║ Годовых записей:', unified_annual.length, '(' + years[0] + '—' + years[years.length-1] + ')');
  console.log('║ Дневных курсов:', daily_rates.length, '| Реальных НБТ:', daily_rates.filter(r=>r.source==='nbt').length);
  console.log('║ Полнота ВВП:', quality.gdp_completeness);
  console.log('║ Полнота инфляции:', quality.inflation_completeness);
  console.log('║ Источники:', quality.sources.join(', '));
  console.log('╚══════════════════════════════════════════╝');

  console.log('\n📊 ДАННЫЕ ДЛЯ МОДЕЛЕЙ:');
  const gdpSeries  = unified_annual.filter(r=>r.gdp_growth_mert||r.gdp_growth_imf).map(r=>r.gdp_growth_mert||r.gdp_growth_imf);
  const inflSeries = unified_annual.filter(r=>r.inflation_mert||r.inflation_imf).map(r=>r.inflation_mert||r.inflation_imf);
  const usdSeries  = daily_rates.map(r=>r.usd_tjs);
  console.log('ARIMA ВВП:',       gdpSeries.length,  'точек', gdpSeries.length  >= 20 ? '✅ ДОСТАТОЧНО' : '⚠️ МАЛО (нужно 20+)');
  console.log('ARIMA Инфляция:',  inflSeries.length, 'точек', inflSeries.length >= 20 ? '✅'            : '⚠️');
  console.log('GARCH USD/TJS:',   usdSeries.length,  'точек', usdSeries.length  >= 100? '✅'            : '⚠️ МАЛО (нужно 100+)');
  console.log('VAR (4 ряда):',    Math.min(gdpSeries.length, inflSeries.length), 'совместных точек');

  return dataset;
}

buildUnifiedDataset().catch(console.error);
