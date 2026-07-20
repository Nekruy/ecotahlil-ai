'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/exportSogd.js — выгрузка импортированных данных Согда в один JSON
//  для внешней визуализации. Только реально импортированные числа.
//  Единая логика сборки — в territoryData.js (используется и живым дашбордом).
//
//  Запуск:  node schemas/exportSogd.js   →  schemas/sogd_export.json
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');
const { buildTerritoryData } = require('./territoryData');

const OUT_FILE = path.join(__dirname, 'sogd_export.json');

function run() {
  const d = buildTerritoryData('sogd');
  const out = {
    territory:   d.territory,
    source:      d.source,
    years:       d.years,
    tablesCount: d.filledTables,
    tables:      d.tables,
  };
  fs.writeFileSync(OUT_FILE, JSON.stringify(out, null, 2), 'utf8');

  const size = fs.statSync(OUT_FILE).size;
  const indTotal = d.tables.reduce((a, t) => a + t.indicators.length, 0);
  console.log('Записано:', path.relative(process.cwd(), OUT_FILE));
  console.log('Таблиц:', d.filledTables, '| показателей всего:', indTotal, '| размер:', (size / 1024).toFixed(1) + ' КБ (' + size + ' байт)');
  return out;
}

run();
