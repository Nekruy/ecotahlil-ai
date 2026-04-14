'use strict';

/**
 * importMinistryData.js — Импорт реальных данных из модели МЭРиТ
 *
 * Читает: data/Model_GDP_-8_04_2025.xlsx
 * Записывает: data/ministry_gdp_model.json
 *
 * Запуск: node importMinistryData.js
 */

const XLSX = require('xlsx');
const fs   = require('fs');
const path = require('path');

const SRC  = path.join(__dirname, 'data', 'Model_GDP_-8_04_2025.xlsx');
const DEST = path.join(__dirname, 'data', 'ministry_gdp_model.json');

// ─── Вспомогательные функции ──────────────────────────────────────────────────

function round2(v) {
  return v != null && !isNaN(v) ? Math.round(v * 100) / 100 : null;
}

function round0(v) {
  return v != null && !isNaN(v) ? Math.round(v) : null;
}

/** Возвращает строку листа по переменному имени (col 0 или col 1) */
function findRow(data, varName, col = 0) {
  return data.find(row => row[col] && String(row[col]).trim() === varName);
}

/** Строит массив {year, value} из строки данных и массива лет */
function buildSeries(row, years, colOffset = 2) {
  return years.map((yr, i) => {
    const v = row ? row[colOffset + i] : null;
    return { year: yr, value: v != null && !isNaN(v) ? Number(v) : null };
  });
}

// ─── Чтение Excel ─────────────────────────────────────────────────────────────

console.log(`Читаю файл: ${SRC}`);
const wb = XLSX.readFile(SRC);

// ─── Лист GDP ─────────────────────────────────────────────────────────────────

const gdpWS   = wb.Sheets['GDP'];
const gdpData = XLSX.utils.sheet_to_json(gdpWS, { header: 1, defval: null });

const gdpYears = gdpData[0].slice(2, 30).filter(y => typeof y === 'number'); // 1997–2024
const N = gdpYears.length; // 28

// Индексы строк (0-based) в листе GDP:
const rowGDPmp     = gdpData[1];   // ВВП в рыночных ценах, млн. сомони
const rowGrowth    = gdpData[2];   // Реальный рост ВВП, % (значение 100 + %)
const rowDeflator  = gdpData[3];   // Дефлятор ВВП, %
const rowPerCapita = gdpData[5];   // ВВП на душу населения, долл. США
const rowCPI_dec   = gdpData[7];   // ИПЦ (дек/дек)
const rowPopulation = gdpData[9];  // Численность населения, тыс. чел.

const gdpSeries = gdpYears.map((yr, i) => {
  const col = 2 + i;
  const gdpMln    = rowGDPmp     ? Number(rowGDPmp[col])     : null;
  const growthRaw = rowGrowth    ? Number(rowGrowth[col])    : null;
  const deflator  = rowDeflator  ? Number(rowDeflator[col])  : null;
  const pcUSD     = rowPerCapita ? Number(rowPerCapita[col]) : null;
  const pop       = rowPopulation ? Number(rowPopulation[col]) : null;

  return {
    year:             yr,
    gdp_mln_somoni:   round2(gdpMln),
    gdp_bln_somoni:   round2(gdpMln != null ? gdpMln / 1000 : null),
    gdp_growth:       round2(growthRaw != null ? growthRaw - 100 : null),
    gdp_deflator:     round2(deflator),
    gdp_per_capita_usd: round2(pcUSD),
    population_thou:  round2(pop),
  };
});

// ─── Инфляция ─────────────────────────────────────────────────────────────────

const inflSeries = gdpYears.map((yr, i) => {
  const col = 2 + i;
  const cpi_dec = rowCPI_dec ? Number(rowCPI_dec[col]) : null;
  return {
    year: yr,
    cpi:  round2(cpi_dec),        // ИПЦ дек/дек (100-based, н-р 107.4 = 7.4%)
    cpi_growth_pct: round2(cpi_dec != null ? cpi_dec - 100 : null),
  };
});

// ─── Лист Agriculture ────────────────────────────────────────────────────────

const agWS   = wb.Sheets['Agriculture'];
const agData = XLSX.utils.sheet_to_json(agWS, { header: 1, defval: null });

const agYears   = agData[0].slice(2, 30).filter(y => typeof y === 'number');
const rowAgTotal = agData[1]; // СЕЛЬСКОЕ ХОЗЯЙСТВО, млн. сомони
const rowAgGrow  = agData[2]; // темп роста %
const rowCrop    = agData[4]; // РАСТЕНИЕВОДСТВО
const rowLivest  = agData[7]; // ЖИВОТНОВОДСТВО

const agSeries = agYears.map((yr, i) => {
  const col = 2 + i;
  const total  = rowAgTotal ? Number(rowAgTotal[col]) : null;
  const growth = rowAgGrow  ? Number(rowAgGrow[col])  : null;
  const crop   = rowCrop    ? Number(rowCrop[col])    : null;
  const livest = rowLivest  ? Number(rowLivest[col])  : null;

  return {
    year:            yr,
    value_mln_somoni: round2(total),
    growth_pct:      round2(growth != null ? growth - 100 : null),
    crop_mln_somoni: round2(crop),
    livestock_mln_somoni: round2(livest),
  };
}).filter(r => r.value_mln_somoni != null);

// ─── Лист Industry ───────────────────────────────────────────────────────────

const indWS   = wb.Sheets['Industry'];
const indData = XLSX.utils.sheet_to_json(indWS, { header: 1, defval: null });

// В листе Industry: col 0 — пусто или "Реальные темпы роста", col 1 — Var.Name, col 2 — название, col 3..30 — 1997..2023
const indYears = indData[0].slice(3, 31).filter(y => typeof y === 'number'); // 1997–2023

function findIndRow(varName) {
  return indData.find(row => row[1] && String(row[1]).trim() === varName);
}

const rowIndTotal   = findIndRow('INDCDE,t');   // Промышленное производство
const rowMining     = findIndRow('INDC,t');     // Добыча полезных ископаемых
const rowManuf      = findIndRow('INDDJ, t');   // Обрабатывающая промышленность (первое вхождение)
const rowEnergy     = findIndRow('INDE, t');    // Электроэнергия
const rowIndGrowth  = findIndRow('gINDCDE,t');  // Темп роста промышленности %

const indSeries = indYears.map((yr, i) => {
  const col = 3 + i;
  const total  = rowIndTotal  ? Number(rowIndTotal[col])  : null;
  const mining = rowMining    ? Number(rowMining[col])    : null;
  const manuf  = rowManuf     ? Number(rowManuf[col])     : null;
  const energy = rowEnergy    ? Number(rowEnergy[col])    : null;
  const grow   = rowIndGrowth ? Number(rowIndGrowth[col]) : null;

  return {
    year:             yr,
    value_mln_somoni: round2(total),
    growth_pct:       round2(grow != null ? grow - 100 : null),
    mining_mln:       round2(mining),
    manufacturing_mln: round2(manuf),
    energy_mln:       round2(energy),
  };
}).filter(r => r.value_mln_somoni != null);

// ─── Лист Trade ───────────────────────────────────────────────────────────────

const trWS   = wb.Sheets['Trade'];
const trData = XLSX.utils.sheet_to_json(trWS, { header: 1, defval: null });

const trYears  = trData[0].slice(2, 30).filter(y => typeof y === 'number');
const rowExport = trData[2];   // EXUSt — Экспорт товаров и услуг, млн. долл.
const rowImport = trData[40];  // IMUSt — Импорт товаров и услуг, млн. долл.

const tradeSeries = trYears.map((yr, i) => {
  const col = 2 + i;
  const exp = rowExport ? Number(rowExport[col]) : null;
  const imp = rowImport ? Number(rowImport[col]) : null;
  const bal = (exp != null && imp != null) ? exp - imp : null;

  return {
    year:           yr,
    export_mln_usd: round2(exp),
    import_mln_usd: round2(imp),
    balance_mln_usd: round2(bal),
  };
}).filter(r => r.export_mln_usd != null || r.import_mln_usd != null);

// ─── Лист Monetary ────────────────────────────────────────────────────────────

const monWS   = wb.Sheets['Monetary'];
const monData = XLSX.utils.sheet_to_json(monWS, { header: 1, defval: null });

const monYears   = monData[0].slice(2, 30).filter(y => typeof y === 'number');
const rowCPIav   = monData[2];   // dCPIAVt — Инфляция ср.год
const rowCPIFood = monData[4];   // dCPIAVFood — продовольственная инфляция
const rowCPInFood = monData[5];  // dCPIAVnonFood — непродовольственная
const rowCPIServ = monData[6];   // dCPIAVServices — услуги

const monetarySeries = monYears.map((yr, i) => {
  const col = 2 + i;
  return {
    year:                yr,
    cpi_avg_annual:      round2(Number(rowCPIav   ? rowCPIav[col]   : null)),
    cpi_food:            round2(Number(rowCPIFood ? rowCPIFood[col] : null)),
    cpi_nonfood:         round2(Number(rowCPInFood ? rowCPInFood[col] : null)),
    cpi_services:        round2(Number(rowCPIServ ? rowCPIServ[col] : null)),
  };
}).filter(r => r.cpi_avg_annual != null);

// ─── Сборка итогового JSON ────────────────────────────────────────────────────

const result = {
  meta: {
    source:       'Модель ВВП МЭРиТ РТ, файл Model_GDP_-8_04_2025.xlsx',
    period:       '1997–2024',
    imported_at:  new Date().toISOString(),
    note:         'Все показатели роста — в % (7.5 означает 7.5%, не 107.5). ИПЦ: 100-base (107.4 = 7.4% инфляция).',
  },
  gdp:        gdpSeries,
  inflation:  inflSeries,
  agriculture: agSeries,
  industry:   indSeries,
  trade:      tradeSeries,
  monetary:   monetarySeries,
};

fs.writeFileSync(DEST, JSON.stringify(result, null, 2), 'utf8');
console.log(`\n✓ Сохранено: ${DEST}`);
console.log(`  ВВП:         ${gdpSeries.length} записей (${gdpYears[0]}–${gdpYears[gdpYears.length-1]})`);
console.log(`  Инфляция:    ${inflSeries.length} записей`);
console.log(`  С/х:         ${agSeries.length} записей`);
console.log(`  Промышл.:    ${indSeries.length} записей`);
console.log(`  Торговля:    ${tradeSeries.length} записей`);
console.log(`  Монетарные:  ${monetarySeries.length} записей`);

// Быстрая проверка — последние данные
const last = gdpSeries[gdpSeries.length - 1];
console.log(`\nПоследняя запись ВВП (${last.year}):`);
console.log(`  ВВП:        ${last.gdp_mln_somoni} млн сомони (${last.gdp_bln_somoni} млрд)`);
console.log(`  Рост:       ${last.gdp_growth}%`);
console.log(`  На душу:    $${last.gdp_per_capita_usd}`);
console.log(`  Население:  ${last.population_thou} тыс. чел.`);
