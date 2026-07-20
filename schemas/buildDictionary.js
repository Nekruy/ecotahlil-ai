'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/buildDictionary.js — Этап 1 двуязычия
//
//  Собирает из tables.json ВСЕ уникальные пользовательские строки на таджикском:
//    • названия таблиц (name)
//    • названия показателей и групп («аз он ҷумла:» и т.п.)
//    • единицы измерения (колонка «Воҳиди ченак» и общая единица)
//  Убирает повторы. Пишет schemas/dictionary_tg_ru.json как { "тадж": "" }.
//
//  Перевод хранится ДАННЫМИ; форма-генератор не меняется. Запуск:
//    node schemas/buildDictionary.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const SCHEMA_FILE = path.join(__dirname, 'tables.json');
const OUT_FILE    = path.join(__dirname, 'dictionary_tg_ru.json');

function norm(s) {
  return String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
}

// Мусорные/непереводимые строки: пусто, одни знаки, «\» и т.п.
function isNoise(s) {
  if (!s) return true;
  if (/^[\\\/.\-_—\s]+$/.test(s)) return true;   // только пунктуация/слэши
  if (/^[\d.,%№()\s-]+$/.test(s)) return true;   // только числа/символы
  return false;
}

function main() {
  const data = JSON.parse(fs.readFileSync(SCHEMA_FILE, 'utf8'));

  const seen = new Set();
  const dict = {};                    // сохраняем порядок вставки
  const cat  = { titles: 0, groups: 0, units: 0, indicators: 0 };

  const add = (raw, category) => {
    const s = norm(raw);
    if (isNoise(s) || seen.has(s)) return;
    seen.add(s);
    dict[s] = '';
    cat[category]++;
  };

  for (const t of data.tables) {
    // 1) название таблицы
    add(t.name, 'titles');
    // 2) общая единица (модель B)
    if (t.globalUnit) add(t.globalUnit, 'units');
    // 3) показатели, группы и построчные единицы
    for (const ind of (t.indicators || [])) {
      add(ind.name, ind.isGroup ? 'groups' : 'indicators');
      if (ind.unit) add(ind.unit, 'units');
    }
  }

  fs.writeFileSync(OUT_FILE, JSON.stringify(dict, null, 2), 'utf8');

  const total = seen.size;
  console.log('[dict] Источник:', path.basename(SCHEMA_FILE), '| таблиц:', data.tables.length);
  console.log('[dict] Уникальных строк:', total);
  console.log(`[dict]   — заголовки таблиц: ${cat.titles}`);
  console.log(`[dict]   — группы:           ${cat.groups}`);
  console.log(`[dict]   — единицы измерения:${cat.units}`);
  console.log(`[dict]   — показатели:       ${cat.indicators}`);
  console.log('[dict] Записано →', path.relative(process.cwd(), OUT_FILE));
}

if (require.main === module) main();
module.exports = { main };
