'use strict';
/**
 * import_dushanbe_prices.js
 * Парсит файл нх(2).xlsx и сохраняет структурированные данные цен
 * в data/dushanbe_prices.json
 *
 * Формат выходного файла:
 * {
 *   meta: { source, importedAt, sheets, totalRecords },
 *   products: { [product]: { unit, observations: [{date, price}] } },
 *   snapshots: [ { date, label, prices: {product: price} } ]
 * }
 */

const XLSX = require('xlsx');
const fs   = require('fs');
const path = require('path');

const INPUT_FILE  = 'C:\\Users\\user\\Downloads\\нх (2).xlsx';
const OUTPUT_FILE = path.join(__dirname, 'data', 'dushanbe_prices.json');

// ─── Словарь нормализации названий товаров (тадж. → en key) ─────────────────
const PRODUCT_MAP = {
  'Картошка':                         'potato',
  'Лўбиё':                            'beans',
  'Мош':                              'mung_beans',
  'Нахўд':                            'chickpeas',
  'Орди навъи 1 (ватанї)':            'flour_domestic',
  'Орди навъи 1 (Ќазоќ.)':           'flour_kazakh',
  'Орди навъи 1 (Ќазоќистон)':       'flour_kazakh',
  'Гази моеъ':                        'lpg',
  'Шир':                              'milk',
  'Тухм':                             'eggs',
  'Чойи кабуд':                       'green_tea',
  'Чойи сиёњ':                        'black_tea',
  'Гўшти мурѓ':                       'chicken',
  'Помидор':                          'tomato',
  'Равѓани растанї':                  'vegetable_oil',
  'Гўшти гов':                        'beef',
  'Гўшти гўсфанд':                    'mutton',
  'Карам':                            'cabbage',
  'Шакар':                            'sugar',
  'Биринљ':                           'rice',
  'Бодиринг':                         'cucumber',
  'Себ':                              'apple',
  'Сўзишвории дизелї':                'diesel',
  'Бензин АИ-95':                     'gasoline_95',
  'Бензин АИ-92':                     'gasoline_92',
  'Сабзї ':                           'carrot',
  'Сабзї':                            'carrot',
  'Бехпиёз':                          'onion',
  'Макарон':                          'pasta',
};

const UNIT_MAP = {
  'кг': 'kg', 'литр': 'liter', '50 кг': '50kg', '10 дона': '10pcs',
};

// ─── Хелперы ─────────────────────────────────────────────────────────────────

/** Excel serial date → YYYY-MM-DD */
function excelDateToISO(serial) {
  if (typeof serial !== 'number' || serial < 40000 || serial > 50000) return null;
  const base = new Date(1900, 0, 0); // Excel epoch: 0 = Jan 0 1900
  const d = new Date(base.getTime() + (serial - 1) * 86400000);
  return d.toISOString().slice(0, 10);
}

/** Очистить строку от лишних пробелов */
function clean(s) {
  return typeof s === 'string' ? s.trim().replace(/\s+/g, ' ') : s;
}

/** Нормализовать имя товара */
function normalizeProduct(name) {
  const c = clean(name);
  return PRODUCT_MAP[c] || null;
}

// ─── Парсинг листа с горизонтальным форматом (дата = столбец) ────────────────

/**
 * Лист с форматом:
 * строка 0: заголовок
 * строка 1: ["№", "Номгўи мањсулот", "Воњиди ченак", "Нархи миёна", ...]
 * строка 2: [null, null, null, DATE1, DATE2, DATE3, ...]
 * строка 3+: [num, product, unit, price1, price2, price3, ...]
 */
function parseMultiDateSheet(rows, sheetName) {
  const observations = [];

  // Найти строку с датами (Excel serial numbers 40000–50000)
  let dateRow = -1;
  let headerRow = -1;
  for (let i = 0; i < Math.min(5, rows.length); i++) {
    const r = rows[i];
    if (!r) continue;
    const hasDate = r.some(v => typeof v === 'number' && v > 40000 && v < 50000);
    if (hasDate) { dateRow = i; break; }
    if (r[1] && typeof r[1] === 'string' && r[1].includes('Номгўи')) headerRow = i;
  }

  if (dateRow === -1) return observations;

  // Столбцы с датами
  const dateCols = [];
  const dr = rows[dateRow];
  for (let c = 3; c < dr.length; c++) {
    const iso = excelDateToISO(dr[c]);
    if (iso) dateCols.push({ col: c, date: iso });
  }

  if (dateCols.length === 0) return observations;

  // Строки с товарами
  for (let i = dateRow + 1; i < rows.length; i++) {
    const row = rows[i];
    if (!row || row.length < 4) continue;

    const productRaw = row[1];
    if (!productRaw || typeof productRaw !== 'string') continue;

    const productKey = normalizeProduct(productRaw);
    if (!productKey) continue;

    const unit = UNIT_MAP[clean(row[2])] || clean(row[2]) || 'unit';

    for (const { col, date } of dateCols) {
      const price = row[col];
      if (typeof price === 'number' && price > 0) {
        observations.push({ date, product: productKey, unit, price, sheet: sheetName });
      }
    }
  }

  return observations;
}

// ─── Парсинг листа с двумя датами и разницей ─────────────────────────────────
/**
 * Формат: №, product, unit, price_current, price_prev, diff_som, diff_pct
 * Даты берём из строки 2 (может быть текст или serial)
 */
function parseTwoDateSheet(rows, sheetName) {
  const observations = [];

  let dateRow = -1;
  for (let i = 0; i < Math.min(5, rows.length); i++) {
    const r = rows[i];
    if (!r) continue;
    const hasDate = r.some(v => typeof v === 'number' && v > 40000 && v < 50000);
    if (hasDate) { dateRow = i; break; }
  }

  // Если нет serial dates, пробуем найти текстовые даты в заголовке
  let date1 = null, date2 = null;
  if (dateRow !== -1) {
    const dr = rows[dateRow];
    const dates = dr.filter(v => typeof v === 'number' && v > 40000 && v < 50000)
                    .map(excelDateToISO).filter(Boolean);
    if (dates.length >= 2) { [date1, date2] = dates; }
    else if (dates.length === 1) { date1 = dates[0]; }
  }

  // Заголовок в строке 0
  const titleStr = rows[0]?.[0] || '';
  if (!date1) {
    // Попробуем извлечь год из заголовка
    const yearMatch = titleStr.match(/20\d\d/g);
    if (yearMatch) date1 = yearMatch[yearMatch.length - 1] + '-12-31';
  }

  const startRow = dateRow !== -1 ? dateRow + 1 : 3;
  for (let i = startRow; i < rows.length; i++) {
    const row = rows[i];
    if (!row || row.length < 4) continue;
    if (typeof row[0] !== 'number' && !(typeof row[0] === 'string' && /^\d+/.test(row[0]))) continue;

    const productRaw = row[1];
    if (!productRaw || typeof productRaw !== 'string') continue;

    const productKey = normalizeProduct(productRaw);
    if (!productKey) continue;

    const unit = UNIT_MAP[clean(row[2])] || 'unit';

    // price1 (current) — колонка 3 или после dateRow
    if (date1 && typeof row[3] === 'number' && row[3] > 0) {
      observations.push({ date: date1, product: productKey, unit, price: row[3], sheet: sheetName });
    }
    if (date2 && typeof row[4] === 'number' && row[4] > 0) {
      observations.push({ date: date2, product: productKey, unit, price: row[4], sheet: sheetName });
    }
  }

  return observations;
}

// ─── Главная функция ──────────────────────────────────────────────────────────

function importPrices() {
  console.log('📂 Чтение файла:', INPUT_FILE);
  const wb = XLSX.readFile(INPUT_FILE);
  console.log('Листы:', wb.SheetNames.join(', '));

  const allObservations = [];

  for (const sheetName of wb.SheetNames) {
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });

    console.log(`\n📊 Лист "${sheetName}": ${rows.length} строк`);

    // Пробуем оба парсера
    const obs1 = parseMultiDateSheet(rows, sheetName);
    const obs2 = parseTwoDateSheet(rows, sheetName);

    // Берём более богатый результат
    const obs = obs1.length >= obs2.length ? obs1 : obs2;
    console.log(`   Извлечено наблюдений: ${obs.length} (multi: ${obs1.length}, two-date: ${obs2.length})`);

    allObservations.push(...obs);
  }

  console.log(`\n✅ Итого наблюдений: ${allObservations.length}`);

  // ─── Структуризация по товарам ───────────────────────────────────────────
  const productsMap = {};
  for (const obs of allObservations) {
    if (!productsMap[obs.product]) {
      productsMap[obs.product] = { unit: obs.unit, observations: [] };
    }
    productsMap[obs.product].observations.push({ date: obs.date, price: obs.price });
  }

  // Сортировка и дедупликация
  for (const key of Object.keys(productsMap)) {
    const seen = new Map();
    for (const o of productsMap[key].observations) {
      if (!seen.has(o.date) || seen.get(o.date).price !== o.price) {
        seen.set(o.date + '_' + o.price, o);
      }
    }
    productsMap[key].observations = [...seen.values()]
      .sort((a, b) => a.date.localeCompare(b.date));
  }

  // ─── Снапшоты (каждая уникальная дата) ──────────────────────────────────
  const dateIndex = {};
  for (const obs of allObservations) {
    if (!dateIndex[obs.date]) dateIndex[obs.date] = { date: obs.date, prices: {} };
    dateIndex[obs.date].prices[obs.product] = obs.price;
  }
  const snapshots = Object.values(dateIndex).sort((a, b) => a.date.localeCompare(b.date));

  // ─── Сводная статистика по продуктам ─────────────────────────────────────
  const summary = {};
  for (const [prod, data] of Object.entries(productsMap)) {
    const prices = data.observations.map(o => o.price);
    const first  = data.observations[0];
    const last   = data.observations[data.observations.length - 1];
    summary[prod] = {
      unit:       data.unit,
      count:      prices.length,
      dateRange:  [first?.date, last?.date],
      priceRange: [Math.min(...prices), Math.max(...prices)],
      firstPrice: first?.price,
      lastPrice:  last?.price,
      totalChange: last && first ? ((last.price / first.price - 1) * 100).toFixed(1) + '%' : null,
    };
  }

  const output = {
    meta: {
      source:       'Раёсати сиёсати савдо ва хизматрасонї — г. Душанбе',
      sourceFile:   'нх (2).xlsx',
      importedAt:   new Date().toISOString(),
      sheets:       wb.SheetNames,
      totalRecords: allObservations.length,
      uniqueDates:  snapshots.length,
      products:     Object.keys(productsMap).length,
    },
    summary,
    products: productsMap,
    snapshots,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2), 'utf8');
  console.log(`\n💾 Сохранено: ${OUTPUT_FILE}`);

  // ─── Вывод итоговой статистики ───────────────────────────────────────────
  console.log('\n📈 СВОДКА ПО ТОВАРАМ:');
  console.log('Товар           | Точек | Период               | Изм-е');
  console.log('─'.repeat(65));
  for (const [prod, s] of Object.entries(summary).sort((a, b) => b[1].count - a[1].count)) {
    const name = prod.padEnd(16);
    const cnt  = String(s.count).padStart(5);
    const range = `${s.dateRange[0]} → ${s.dateRange[1]}`.padEnd(22);
    const chg  = s.totalChange || '?';
    console.log(`${name} | ${cnt} | ${range} | ${chg}`);
  }

  console.log('\n📅 Уникальных дат:', snapshots.length);
  console.log('Первая дата:', snapshots[0]?.date, '| Последняя:', snapshots[snapshots.length-1]?.date);

  return output;
}

// ─── Запуск ───────────────────────────────────────────────────────────────────
try {
  const result = importPrices();
  console.log('\n✅ Импорт завершён успешно!');
  process.exit(0);
} catch (e) {
  console.error('❌ Ошибка:', e.message);
  console.error(e.stack);
  process.exit(1);
}
