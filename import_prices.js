'use strict';
const XLSX = require('xlsx');
const fs   = require('fs');

const wb = XLSX.readFile('C:\\Users\\user\\Downloads\\нх (2).xlsx');

// Маппинг тадж. названий → ключ продукта
const PRODUCT_MAP = {
  'бехпиёз':               'onion',
  'сабзї':                 'carrot',
  'сабзи':                 'carrot',
  'помидор':               'tomato',
  'бодиринг':              'cucumber',
  'карам':                 'cabbage',
  'себ':                   'apple',
  'картошка':              'potato',
  'гўшти гов':             'beef',
  'гушти гов':             'beef',
  'гўшти гўсфанд':         'mutton',
  'гўшти мурѓ':            'chicken',
  'шир':                   'milk',
  'тухм':                  'eggs',
  'орди навъи 1 (ватанї)': 'flour',
  'орди навъи 1 (ќазоќ.)': 'flour_kaz',
  'орди навъи 1 (ќазоќистон)': 'flour_kaz',
  'биринљ':                'rice',
  'шакар':                 'sugar',
  'равѓани растанї':       'oil',
  'нахўд':                 'chickpea',
  'лўбиё':                 'beans',
  'мош':                   'mash',
  'чойи сиёњ':             'tea_black',
  'чойи кабуд':            'tea_green',
  'бензин аи-92':          'fuel_92',
  'бензин аи-95':          'fuel_95',
  'сўзишвории дизелї':     'diesel',
  'гази моеъ':             'gas_lpg',
};

const SHEET_DATES = {
  'Лист1':  '2020-07-17',
  'Лист2':  '2020-07-01',
  'Лист4':  '2020-10-23',
  'Лист5':  '2021-01-01',
  'Лист6':  '2022-03-30',
  'Лист7':  '2022-03-30',
  'Лист8':  '2022-04-01',
  'Лист9':  '2022-06-01',
  'Лист10': '2022-04-13',
  'Лист11': '2023-10-13',
};

const allPrices = [];

for (const sheetName of wb.SheetNames) {
  const date = SHEET_DATES[sheetName];
  if (!date) continue;

  const ws   = wb.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  for (const row of rows) {
    if (!row || row.length < 3) continue;

    // col1 — название товара (col0 — порядковый номер)
    const rawName = typeof row[1] === 'string' ? row[1].trim() : null;
    if (!rawName || rawName.length < 2) continue;

    const nameLow = rawName.toLowerCase();
    const unit    = typeof row[2] === 'string' ? row[2].trim() : '';

    // Цена — первое положительное число в col3..col6
    let price = null;
    for (let c = 3; c <= 6; c++) {
      const v = parseFloat(row[c]);
      if (!isNaN(v) && v > 0 && v < 100000) { price = v; break; }
    }
    if (!price) continue;

    // Ищем совпадение в маппинге (наиболее длинный ключ побеждает)
    let productKey = null;
    let bestLen    = 0;
    for (const [kw, key] of Object.entries(PRODUCT_MAP)) {
      if (nameLow.includes(kw) && kw.length > bestLen) {
        productKey = key;
        bestLen    = kw.length;
      }
    }
    if (!productKey) continue;

    allPrices.push({ date, product: productKey, name_original: rawName, unit, price_tjs: price, sheet: sheetName });
  }
}

// Группируем по date+product → среднее (убираем дубли между листами одной даты)
const grouped = {};
for (const p of allPrices) {
  const k = `${p.date}_${p.product}`;
  if (!grouped[k]) grouped[k] = { ...p, _arr: [p.price_tjs] };
  else grouped[k]._arr.push(p.price_tjs);
}

const result = Object.values(grouped)
  .map(p => ({
    date:          p.date,
    product:       p.product,
    name_original: p.name_original,
    unit:          p.unit,
    price_tjs:     Math.round(p._arr.reduce((a, b) => a + b, 0) / p._arr.length * 100) / 100,
    source:        'МЭРиТ РТ (ГУМАПЭР)',
  }))
  .sort((a, b) => a.date.localeCompare(b.date) || a.product.localeCompare(b.product));

fs.writeFileSync('data/real_prices.json', JSON.stringify({
  meta: {
    source:        'МЭРиТ РТ — ГУМАПЭР (Главное управление мониторинга и анализа потребительских рынков)',
    periods:       [...new Set(result.map(r => r.date))].sort(),
    total_records: result.length,
    products:      [...new Set(result.map(r => r.product))],
    created_at:    new Date().toISOString(),
  },
  prices: result,
}, null, 2));

// ── отчёт ──────────────────────────────────────────────────────────────────
console.log('Всего записей:', result.length);
console.log('Продуктов:',    [...new Set(result.map(r => r.product))].length);
console.log('Периодов:',     [...new Set(result.map(r => r.date))].length);

console.log('\nПервые 12:');
result.slice(0, 12).forEach(p =>
  console.log(` ${p.date}  ${p.product.padEnd(15)} ${String(p.price_tjs).padStart(7)} TJS | ${p.unit.padEnd(8)} | ${p.name_original}`)
);

const byProduct = {};
for (const p of result) {
  if (!byProduct[p.product]) byProduct[p.product] = [];
  byProduct[p.product].push(p.price_tjs);
}
console.log('\nСводка:');
for (const [prod, prices] of Object.entries(byProduct).sort((a, b) => b[1].length - a[1].length)) {
  const mn   = Math.min(...prices).toFixed(2);
  const mx   = Math.max(...prices).toFixed(2);
  const last = prices[prices.length - 1].toFixed(2);
  console.log(` ${prod.padEnd(15)} точек:${prices.length} | ${mn}–${mx} TJS | посл.=${last}`);
}
