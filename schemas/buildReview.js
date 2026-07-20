'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/buildReview.js — подготовка чернового словаря к ВЫЧИТКЕ
//
//  Вход:  schemas/tables.json + schemas/dictionary_tg_ru_draft.json
//  Выход:
//    • schemas/dictionary_review.csv           — удобно вычитывать в Excel
//    • schemas/dictionary_review.json          — то же, сгруппировано (кат.→отрасль)
//    • schemas/dictionary_review_suspicious.csv — подозрительные (вычитать первыми)
//    • schemas/abbreviations_applied.md         — правила аббревиатур + аудит
//
//  Ничего в форме/рабочем коде не меняет. Запуск: node schemas/buildReview.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const TABLES = path.join(__dirname, 'tables.json');
const DRAFT  = path.join(__dirname, 'dictionary_tg_ru_draft.json');
const OUT_CSV        = path.join(__dirname, 'dictionary_review.csv');
const OUT_JSON       = path.join(__dirname, 'dictionary_review.json');
const OUT_SUSPICIOUS = path.join(__dirname, 'dictionary_review_suspicious.csv');
const OUT_ABBR       = path.join(__dirname, 'abbreviations_applied.md');

const norm = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
const isNoise = s => !s || /^[\\\/.\-_—\s]+$/.test(s) || /^[\d.,%№()\s-]+$/.test(s);

// ─── Отрасль по тексту (название таблицы ИЛИ сам термин) ──────────────────────
function sectorOf(name) {
  const s = String(name).toLowerCase();
  if (/макроиқтисод/.test(s))                                   return 'Макроэкономика (сводная)';
  if (/нафту газ|нафт|гази табиӣ/.test(s))                      return 'Энергетика: нефть и газ';
  if (/энергетик|нерӯи барқ|кувваи гармӣ|барқ/.test(s))         return 'Энергетика';
  if (/кишоварз|растанипарвар|чорводор|ғалладон|полез|боғ|деҳқон|замин/.test(s)) return 'Сельское хозяйство';
  if (/саноат/.test(s))                                         return 'Промышленность';
  if (/сохтмон|манзил/.test(s))                                 return 'Строительство';
  if (/нақлиёт|боркашон|мусофир|алоқа|коммуникат/.test(s))       return 'Транспорт и связь';
  if (/савдо|содирот|воридот|хизматрасон|туризм|меҳмонхон/.test(s)) return 'Торговля и услуги';
  if (/сармоя|маблағгузор|буҷет|андоз|қарз|грант|даромад|молия/.test(s)) return 'Финансы, инвестиции, налоги';
  if (/аҳолӣ|шуғл|музди меҳнат|бекор|меҳнат|демограф/.test(s))    return 'Демография и труд';
  return 'Прочее';
}

const CAT_ORDER = { 'заголовок': 0, 'группа': 1, 'единица': 2, 'показатель': 3 };

// ─── Сбор сведений по каждому термину ────────────────────────────────────────
function collect() {
  const data  = JSON.parse(fs.readFileSync(TABLES, 'utf8'));
  const draft = JSON.parse(fs.readFileSync(DRAFT, 'utf8'));
  const info  = new Map(); // term -> { category, tables:Set, sectors:Set }

  const touch = (term, category, tableId, sector) => {
    const s = norm(term);
    if (isNoise(s)) return;
    if (!info.has(s)) info.set(s, { category, tables: new Set(), sectors: new Set() });
    const rec = info.get(s);
    rec.tables.add(tableId);
    if (sector) rec.sectors.add(sector);
  };

  for (const t of data.tables) {
    const sec = sectorOf(t.name);
    touch(t.name, 'заголовок', t.id, sec);
    if (t.globalUnit) touch(t.globalUnit, 'единица', t.id, sec);
    for (const ind of (t.indicators || [])) {
      touch(ind.name, ind.isGroup ? 'группа' : 'показатель', t.id, sec);
      if (ind.unit) touch(ind.unit, 'единица', t.id, sec);
    }
  }

  const rows = [];
  for (const [tajik, rec] of info) {
    const secs = [...rec.sectors];
    // приоритет: наиболее конкретная отрасль (не «Прочее») из таблиц термина;
    // если все «Прочее» — классифицируем по ключевым словам самого термина.
    let sector = secs.find(x => x !== 'Прочее') || sectorOf(tajik);
    rows.push({
      tajik,
      russian: draft[tajik] || '',
      category: rec.category,
      sector,
      sectorsAll: secs,
      tables: [...rec.tables],
    });
  }
  return rows;
}

// ─── Подозрительные переводы ─────────────────────────────────────────────────
const TAJIK_LETTERS = /[ҲҳҶҷҒғҚқӮӯӢӣ]/;
const ABBR_IN_RU    = /(НБО|МБГ|ҶСК|ҶДММ|ҶСП|ҷСК|ҶДС)/;

// 4 ошибки, найденные ранее — вычитать в первую очередь
const KNOWN_ERRORS = {
  'Чигити пахта': 'чигит = хлопковые СЕМЕНА (не «нити»)',
  'Калобаи пахтагин': 'калоба = ПРЯЖА (не «ткани»)',
  'ҷСК НБО «Сангтӯда-1» млн. кВт/с': 'осталось «НБО» — нужно ГЭС',
  'Дурнамои нишондиҳандаҳои асосии макроиқтисодии Ҷумҳурии Тоҷикистон барои солҳои 2027-2029': 'стиль: «основных макроэкономических показателей»',
};

function suspicion(r) {
  const reasons = [];
  if (KNOWN_ERRORS[r.tajik]) reasons.push('★ ' + KNOWN_ERRORS[r.tajik]);
  if (!r.russian) reasons.push('пустой перевод');
  if (r.russian && TAJIK_LETTERS.test(r.russian)) reasons.push('остался таджикский текст');
  if (ABBR_IN_RU.test(r.russian)) reasons.push('непереведённая аббревиатура');
  if (r.russian && r.russian.trim().length <= 3) reasons.push('очень короткий перевод');
  if (r.tajik.length <= 3) reasons.push('очень короткий термин');
  if (r.russian && r.russian.trim().toLowerCase() === r.tajik.trim().toLowerCase()) reasons.push('перевод = оригинал');
  return reasons;
}

// приоритет сортировки подозрительных
function suspPriority(reasons) {
  if (reasons.some(x => x.startsWith('★'))) return 0;
  if (reasons.includes('пустой перевод')) return 1;
  if (reasons.includes('остался таджикский текст')) return 2;
  if (reasons.includes('непереведённая аббревиатура')) return 3;
  return 4;
}

// ─── CSV ─────────────────────────────────────────────────────────────────────
const BOM = '﻿';
function csvCell(v) {
  const s = String(v == null ? '' : v);
  return /[";\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function csvLine(arr) { return arr.map(csvCell).join(';'); }

// ─── Правила аббревиатур + аудит ─────────────────────────────────────────────
const ABBR_RULES = [
  { src: 'НБО',   dst: 'ГЭС',    srcRe: /НБО/,           dstRe: /ГЭС/,       note: 'Нерӯгоҳи барқи обӣ → гидроэлектростанция' },
  { src: 'МБГ',   dst: 'ТЭЦ',    srcRe: /МБГ/,           dstRe: /ТЭЦ/,       note: 'Маркази барқу гармидиҳӣ → теплоэлектроцентраль' },
  { src: 'ҶСК',   dst: 'ОАО',    srcRe: /[ҶҷЧ]СК/,       dstRe: /ОАО/,       note: 'ҶС Кушода → открытое акц. общество' },
  { src: 'ҶДММ',  dst: 'ООО',    srcRe: /[Ҷҷ]ДММ/,       dstRe: /ООО/,       note: 'Ҷамъияти дорои масъулияти маҳдуд → ООО' },
  { src: 'кВт/с', dst: 'кВт·ч',  srcRe: /кВт\/с/,        dstRe: /кВт·ч|кВт\.ч/, note: 'единица электроэнергии' },
  { src: 'НБО-и хурд', dst: 'малые ГЭС', srcRe: /НБО-и хурд/, dstRe: /мал[а-яё]* ГЭС/i, note: 'малые гидроэлектростанции' },
];

function auditAbbr(rows) {
  return ABBR_RULES.map(rule => {
    const src = rows.filter(r => rule.srcRe.test(r.tajik));
    const applied = src.filter(r => rule.dstRe.test(r.russian));
    const missed  = src.filter(r => !rule.dstRe.test(r.russian));
    return { rule, srcCount: src.length, applied: applied.length, missed };
  });
}

// ─── main ────────────────────────────────────────────────────────────────────
function main() {
  const rows = collect();

  // сортировка: категория → отрасль → русский
  rows.sort((a, b) =>
    (CAT_ORDER[a.category] - CAT_ORDER[b.category]) ||
    a.sector.localeCompare(b.sector, 'ru') ||
    a.russian.localeCompare(b.russian, 'ru'));

  // 1) основной CSV
  const header = ['Категория', 'Отрасль', 'Таджикский', 'Русский (черновик)', 'Исправление', 'Таблицы'];
  const lines = [csvLine(header)];
  for (const r of rows) lines.push(csvLine([r.category, r.sector, r.tajik, r.russian, '', r.tables.join(', ')]));
  fs.writeFileSync(OUT_CSV, BOM + lines.join('\r\n'), 'utf8');

  // 1b) сгруппированный JSON: категория → отрасль → [ {tajik, russian, tables} ]
  const grouped = {};
  for (const r of rows) {
    (grouped[r.category] ||= {});
    (grouped[r.category][r.sector] ||= []).push({ tajik: r.tajik, russian: r.russian, tables: r.tables });
  }
  fs.writeFileSync(OUT_JSON, JSON.stringify(grouped, null, 2), 'utf8');

  // 2) подозрительные
  const susp = rows.map(r => ({ ...r, reasons: suspicion(r) })).filter(r => r.reasons.length);
  susp.sort((a, b) => (suspPriority(a.reasons) - suspPriority(b.reasons)) || a.tajik.localeCompare(b.tajik, 'ru'));
  const sLines = [csvLine(['Приоритет', 'Причина', 'Категория', 'Таджикский', 'Русский (черновик)', 'Исправление', 'Таблицы'])];
  for (const r of susp) sLines.push(csvLine([suspPriority(r.reasons), r.reasons.join('; '), r.category, r.tajik, r.russian, '', r.tables.join(', ')]));
  fs.writeFileSync(OUT_SUSPICIOUS, BOM + sLines.join('\r\n'), 'utf8');

  // 3) аудит аббревиатур
  const audit = auditAbbr(rows);
  let md = '# Применённые правила аббревиатур и аудит\n\n';
  md += 'Правила заложены в промт перевода. Ниже — сколько терминов содержат исходную аббревиатуру и в скольких перевод её корректно заменил.\n\n';
  md += '| Правило | Расшифровка | Термистов с аббрев. | Применено | Пропущено |\n';
  md += '|---|---|---:|---:|---:|\n';
  for (const a of audit)
    md += `| ${a.rule.src} → ${a.rule.dst} | ${a.rule.note} | ${a.srcCount} | ${a.applied} | ${a.missed.length} |\n`;
  md += '\nЕдиницы: `млн. сомонӣ → млн сомони`, `ҳазор нафар → тыс. человек`, `ҳазор доллар → тыс. долларов`, `ҳазор тонна → тыс. тонн`, `ҳаз. Гкал → тыс. Гкал`, `адад → единиц`.\n';
  for (const a of audit) {
    if (a.missed.length) {
      md += `\n### Пропущено для «${a.rule.src} → ${a.rule.dst}» (${a.missed.length}):\n`;
      for (const m of a.missed.slice(0, 20)) md += `- \`${m.tajik}\` → «${m.russian}»\n`;
      if (a.missed.length > 20) md += `- … и ещё ${a.missed.length - 20}\n`;
    }
  }
  fs.writeFileSync(OUT_ABBR, md, 'utf8');

  // консольная сводка
  console.log('[review] Всего терминов:', rows.length);
  const byCat = {}; for (const r of rows) byCat[r.category] = (byCat[r.category] || 0) + 1;
  console.log('[review] По категориям:', byCat);
  const bySec = {}; for (const r of rows) bySec[r.sector] = (bySec[r.sector] || 0) + 1;
  console.log('[review] По отраслям:', bySec);
  console.log('[review] Подозрительных:', susp.length);
  console.log('[review] Файлы:');
  for (const f of [OUT_CSV, OUT_JSON, OUT_SUSPICIOUS, OUT_ABBR]) console.log('   -', path.relative(process.cwd(), f));
  console.log('[review] Аудит аббревиатур:');
  for (const a of audit) console.log(`   ${a.rule.src}→${a.rule.dst}: с аббрев. ${a.srcCount}, применено ${a.applied}, пропущено ${a.missed.length}`);
}

if (require.main === module) main();
module.exports = { main };
