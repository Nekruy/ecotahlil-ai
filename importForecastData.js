'use strict';

/**
 * importForecastData.js — Импорт официального прогноза МЭРиТ 2025–2027
 * Источник: data/Forecast_2025_2027.xlsx (Лист 1)
 *
 * Структура столбцов (0-based):
 *   col 3 (D) = 2023 факт
 *   col 5 (F) = 2024 оценка
 *   col 7 (H) = 2025 прогноз
 *   col 9 (J) = 2026 прогноз
 *   col 11(L) = 2027 прогноз
 */

const XLSX = require('xlsx');
const fs   = require('fs');
const path = require('path');

const XLSX_FILE   = path.join(__dirname, 'data', 'Forecast_2025_2027.xlsx');
const OUTPUT_FILE = path.join(__dirname, 'data', 'ministry_forecast_2025_2027.json');

// Столбцы значений (0-based)
const COL = { y2023: 3, y2024: 5, y2025: 7, y2026: 9, y2027: 11 };

// Строки (0-based = row_number - 1)
const ROWS = {
  population:   4,   // Строка 5:  население тыс. чел
  gdp_mln:      5,   // Строка 6:  ВВП млн сомони
  gdp_per_cap:  7,   // Строка 8:  ВВП на душу сомони
  industry_mln: 11,  // Строка 12: промышленность млн сомони
  agri_mln:     13,  // Строка 14: сельское хозяйство млн сомони
  export_usd:   24,  // Строка 25: экспорт млн долл
  import_usd:   25,  // Строка 26: импорт млн долл
  electricity:  27,  // Строка 28: электроэнергия млн кВт/с
  aluminum:     29,  // Строка 30: алюминий тыс. тонн
  wheat:        74,  // Строка 75: пшеница тыс. тонн
};

function getVal(ws, rowIdx, colIdx) {
  const cell = ws[XLSX.utils.encode_cell({ r: rowIdx, c: colIdx })];
  if (!cell || cell.v === undefined || cell.v === null || cell.v === '') return null;
  const v = parseFloat(String(cell.v).replace(',', '.'));
  return isFinite(v) ? Math.round(v * 10) / 10 : null;
}

function buildSeries(ws, rowIdx) {
  return {
    2023: getVal(ws, rowIdx, COL.y2023),
    2024: getVal(ws, rowIdx, COL.y2024),
    2025: getVal(ws, rowIdx, COL.y2025),
    2026: getVal(ws, rowIdx, COL.y2026),
    2027: getVal(ws, rowIdx, COL.y2027),
  };
}

async function importForecastData() {
  if (!fs.existsSync(XLSX_FILE)) {
    throw new Error(`Файл не найден: ${XLSX_FILE}\nСначала скопируйте: cp "Downloads/..." ./data/Forecast_2025_2027.xlsx`);
  }

  console.log(`[importForecastData] Читаю ${XLSX_FILE}...`);
  const wb = XLSX.readFile(XLSX_FILE);
  const ws = wb.Sheets[wb.SheetNames[0]]; // Лист 1

  // Извлекаем данные
  const population_thou  = buildSeries(ws, ROWS.population);
  const gdp_mln_somoni   = buildSeries(ws, ROWS.gdp_mln);
  const gdp_per_cap_som  = buildSeries(ws, ROWS.gdp_per_cap);
  const industry_mln     = buildSeries(ws, ROWS.industry_mln);
  const agriculture_mln  = buildSeries(ws, ROWS.agri_mln);
  const export_mln_usd   = buildSeries(ws, ROWS.export_usd);
  const import_mln_usd   = buildSeries(ws, ROWS.import_usd);
  const electricity_gwh  = buildSeries(ws, ROWS.electricity);
  const aluminum_thou_t  = buildSeries(ws, ROWS.aluminum);
  const wheat_thou_t     = buildSeries(ws, ROWS.wheat);

  // Вычисляем производные показатели
  const trade_balance_usd = {};
  for (const y of [2023, 2024, 2025, 2026, 2027]) {
    const exp = export_mln_usd[y], imp = import_mln_usd[y];
    trade_balance_usd[y] = (exp != null && imp != null) ? Math.round((exp - imp) * 10) / 10 : null;
  }

  // Доли в ВВП (2024)
  const gdp24  = gdp_mln_somoni[2024];
  const ind24  = industry_mln[2024];
  const agr24  = agriculture_mln[2024];
  const exp24u = export_mln_usd[2024];

  const shares = {
    industry_share_2024:     gdp24 && ind24 ? Math.round(ind24 / gdp24 * 1000) / 10 : null,
    agriculture_share_2024:  gdp24 && agr24 ? Math.round(agr24 / gdp24 * 1000) / 10 : null,
    // Экспорт / ВВП (в сомони): приближённо используем курс ~10.5 USD/TJS
    export_gdp_ratio_2024:   gdp24 && exp24u ? Math.round(exp24u * 10.5 / gdp24 * 1000) / 10 : null,
  };

  const result = {
    meta: {
      source:      'МЭРиТ РТ — Официальный прогноз 2025–2027',
      file:        'data/Forecast_2025_2027.xlsx',
      sheet:       wb.SheetNames[0],
      imported_at: new Date().toISOString(),
    },
    official_forecast: {
      population_thou,
      gdp_mln_somoni,
      gdp_per_cap_som,
      industry_mln,
      agriculture_mln,
      export_mln_usd,
      import_mln_usd,
      trade_balance_usd,
      electricity_gwh,
      aluminum_thou_t,
      wheat_thou_t,
    },
    shares,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(result, null, 2), 'utf8');
  console.log(`[importForecastData] Сохранено в ${OUTPUT_FILE}`);

  // Вывод ключевых значений
  console.log('\n── Ключевые показатели ──────────────────────────────');
  console.log('ВВП млн сомони:   ', gdp_mln_somoni);
  console.log('Население тыс:     ', population_thou);
  console.log('Промышленность:    ', industry_mln);
  console.log('Сельхоз:           ', agriculture_mln);
  console.log('Экспорт млн USD:   ', export_mln_usd);
  console.log('Импорт млн USD:    ', import_mln_usd);
  console.log('Электроэнергия:    ', electricity_gwh);
  console.log('Алюминий тыс.т:    ', aluminum_thou_t);
  console.log('Пшеница тыс.т:     ', wheat_thou_t);
  console.log('Доли (2024):       ', shares);

  return result;
}

// Запуск напрямую: node importForecastData.js
if (require.main === module) {
  importForecastData()
    .then(() => console.log('\n✓ Импорт завершён успешно'))
    .catch(e => { console.error('✗ Ошибка:', e.message); process.exit(1); });
}

module.exports = { importForecastData };
