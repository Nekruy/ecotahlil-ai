'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/finalizeDictionary.js — финальные правки → итоговый словарь
//
//  Вход:  schemas/dictionary_tg_ru_draft.json (черновик после Groq)
//  Выход: schemas/dictionary_tg_ru.json        (ИТОГОВЫЙ, вычитанный)
//
//  Применяет точечные правки вычитки и удаляет мусорные термины.
//  Запуск: node schemas/finalizeDictionary.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const DRAFT = path.join(__dirname, 'dictionary_tg_ru_draft.json');
const FINAL = path.join(__dirname, 'dictionary_tg_ru.json');

// Правки: таджикский ключ → исправленный русский перевод
const CORRECTIONS = {
  'Калобаи пахтагин': 'хлопчатобумажная пряжа',            // было «ткани»
  'Чигити пахта':     'семена хлопчатника',                // было «нити»
  'Рег':              'песок',                              // было «Угольный кокс»
  'Регу сангмайда':   'песок и щебень',                    // было «Кокс и каменный уголь» (доп. правка)
  'Орд':              'мука',                               // было «Кукуруза» (Ҷадвали 4/19)
  'ҷСК НБО «Сангтӯда-1» млн. кВт/с':  'ОАО «Сангтудинская ГЭС-1» млн кВт·ч',
  'ҷСК НБО «Сангтӯда-2» млн. кВт/с.': 'ОАО «Сангтудинская ГЭС-2» млн кВт·ч',
  'Дурнамои нишондиҳандаҳои асосии макроиқтисодии Ҷумҳурии Тоҷикистон барои солҳои 2027-2029':
    'Прогноз основных макроэкономических показателей Республики Таджикистан на 2027-2029 годы',
};

// Добавления: термины, которых не было в черновике (появились после уточнения
// парсинга шапок — ранее прятались из-за неверной границы шапки).
const ADDITIONS = {
  'Растанипарварӣ (га)': 'Растениеводство (га)',
  'Сайёҳии беруна':      'Внешний туризм',
};

// Мусорные термины на удаление (обрывки ячеек)
const REMOVE = ['у'];

function main() {
  const dict = JSON.parse(fs.readFileSync(DRAFT, 'utf8'));

  const applied = [], missing = [];
  for (const [tg, ru] of Object.entries(CORRECTIONS)) {
    if (tg in dict) { dict[tg] = ru; applied.push(tg); }
    else missing.push(tg);
  }

  const added = [];
  for (const [tg, ru] of Object.entries(ADDITIONS)) if (!(tg in dict) || !dict[tg]) { dict[tg] = ru; added.push(tg); }

  const removed = [];
  for (const k of REMOVE) if (k in dict) { delete dict[k]; removed.push(k); }

  fs.writeFileSync(FINAL, JSON.stringify(dict, null, 2), 'utf8');

  console.log('[final] Правок применено:', applied.length, '/', Object.keys(CORRECTIONS).length);
  applied.forEach(k => console.log('   ✓', k, '→', dict[k]));
  if (missing.length) { console.log('[final] НЕ найдены ключи:'); missing.forEach(k => console.log('   ✗', k)); }
  console.log('[final] Удалено мусорных:', removed.length, removed.length ? '(' + removed.join(', ') + ')' : '');
  console.log('[final] Всего терминов в итоговом словаре:', Object.keys(dict).length);
  console.log('[final] Записано →', path.relative(process.cwd(), FINAL));
}

if (require.main === module) main();
module.exports = { CORRECTIONS, REMOVE };
