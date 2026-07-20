'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/injectTranslations.js — подстановка перевода в схемы
//
//  Добавляет в schemas/tables.json двуязычные поля:
//    • таблица:    name_tg / name_ru; globalUnit_tg / globalUnit_ru
//    • показатель: name_tg / name_ru; unit_tg / unit_ru
//    • группа:     name_tg / name_ru
//  Источник перевода — schemas/dictionary_tg_ru.json (итоговый).
//  Также удаляет мусорный показатель «у» (обрывок ячейки, Ҷадвали 14А).
//
//  Идемпотентно (можно перезапускать). Порядок пайплайна:
//    extractSchemas.js → injectTranslations.js
//  Запуск: node schemas/injectTranslations.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const TABLES = path.join(__dirname, 'tables.json');
const DICT   = path.join(__dirname, 'dictionary_tg_ru.json');

const norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
const JUNK = new Set(['у']);

function main() {
  const data = JSON.parse(fs.readFileSync(TABLES, 'utf8'));
  const dict = JSON.parse(fs.readFileSync(DICT, 'utf8'));

  // нормализованный поиск: norm(тадж) → рус
  const lut = new Map();
  for (const [k, v] of Object.entries(dict)) lut.set(norm(k), v);
  const tr = s => { const n = norm(s); return n ? (lut.get(n) || '') : ''; };

  let indTotal = 0, indTranslated = 0, junkRemoved = 0, unitTotal = 0, unitTranslated = 0;

  for (const t of data.tables) {
    // заголовок таблицы
    t.name_tg = t.name || '';
    t.name_ru = tr(t.name);
    // общая единица (модель B)
    if (t.globalUnit) { t.globalUnit_tg = t.globalUnit; t.globalUnit_ru = tr(t.globalUnit); }

    // показатели
    if (Array.isArray(t.indicators)) {
      t.indicators = t.indicators.filter(ind => {
        if (JUNK.has(norm(ind.name))) { junkRemoved++; return false; }
        return true;
      });
      for (const ind of t.indicators) {
        ind.name_tg = ind.name || '';
        ind.name_ru = tr(ind.name);
        indTotal++;
        if (ind.name_ru) indTranslated++;
        if (ind.unit) { ind.unit_tg = ind.unit; ind.unit_ru = tr(ind.unit); unitTotal++; if (ind.unit_ru) unitTranslated++; }
      }
      t.indicatorsCount = t.indicators.length;
    }
  }

  fs.writeFileSync(TABLES, JSON.stringify(data, null, 2), 'utf8');

  console.log('[inject] Таблиц:', data.tables.length);
  console.log(`[inject] Показатели: ${indTranslated}/${indTotal} с переводом (${(100*indTranslated/indTotal).toFixed(1)}%)`);
  console.log(`[inject] Единицы:    ${unitTranslated}/${unitTotal} с переводом`);
  console.log('[inject] Удалено мусорных строк «у»:', junkRemoved);
  console.log('[inject] Записано →', path.relative(process.cwd(), TABLES));
}

if (require.main === module) main();
module.exports = { main };
