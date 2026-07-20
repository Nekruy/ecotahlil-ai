'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/extractSchemas.js — Этап 1: извлечение JSON-схем «39 таблиц» МЭРиТ
//
//  Читает Excel-шаблон (Ҷадвали 1-39) и для каждого листа извлекает структуру:
//    • id (Ҷадвали N[А/Б/В]) и название
//    • список строк-показателей с иерархией (уровень по префиксу «- »)
//    • единицы измерения (колонка «Воҳиди ченак» ИЛИ общая единица в скобках)
//    • структуру столбцов по годам и блокам
//      (Ҳисобот/Баҳодиҳӣ/Дурнамо = Отчёт/Оценка/Прогноз, объём + %)
//
//  ВАЖНО: отдельный модуль. Существующий код проекта не затрагивается.
//  Результат → schemas/tables.json (+ пометки о проблемных листах).
//
//  Запуск:  node schemas/extractSchemas.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const SRC_DIR  = path.join(__dirname, 'source');
const OUT_FILE = path.join(__dirname, 'tables.json');

// ─── Поиск исходного файла шаблона в schemas/source/ ─────────────────────────
function findSource(re) {
  const files = fs.readdirSync(SRC_DIR);
  const hit = files.find(f => re.test(f));
  if (!hit) throw new Error(`Не найден файл по шаблону ${re} в ${SRC_DIR}. Есть: ${files.join(', ')}`);
  return path.join(SRC_DIR, hit);
}

// ─── Нормализация текста ячейки ──────────────────────────────────────────────
function norm(v) {
  if (v == null) return '';
  return String(v).replace(/\s+/g, ' ').trim();
}

// ─── Заполнение объединённых ячеек (merge propagation) ───────────────────────
// Возвращает grid[r][c] с растиражированными значениями из левого-верхнего угла
function buildGrid(ws) {
  const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: null });
  const raw  = aoa.map(row => row.slice());          // до размножения объединений
  const grid = aoa.map(row => row.slice());
  const nRows = grid.length;
  const nCols = Math.max(0, ...grid.map(r => r.length));
  for (const r of raw)  while (r.length < nCols) r.push(null);
  for (const r of grid) while (r.length < nCols) r.push(null);

  for (const m of (ws['!merges'] || [])) {
    const val = grid[m.s.r] ? grid[m.s.r][m.s.c] : null;
    for (let r = m.s.r; r <= m.e.r; r++) {
      for (let c = m.s.c; c <= m.e.c; c++) {
        if (grid[r] && grid[r][c] == null) grid[r][c] = val;
      }
    }
  }
  return { grid, raw, nRows, nCols };
}

// ─── id и название из заголовка (строка 0) ───────────────────────────────────
// «Ҷадвали 10 (А). ...»  /  «Ҷадвали 13(Б). ...»  /  «Ҷадвали 1. ...»
function parseIdTitle(titleRaw, sheetName) {
  const t = norm(titleRaw);
  // допускаем и Ҷ (заголовок), и Ч (имя листа как фолбэк).
  // Буква-суффикс подтаблицы (А/Б/В) засчитывается ТОЛЬКО если за ней граница
  // (скобка/точка/пробел/конец) — иначе «34. Афзоиши…» ложно даёт букву «А».
  const re = /[ҶчЧ]адвал[и]?\s*(\d+)(?:\s*[\(．.\s]*([АБВабв])(?=[)\s.．]|$))?/i;
  let m = t.match(re) || norm(sheetName).match(re);
  let num = null, letter = '';
  if (m) { num = m[1]; letter = (m[2] || '').toUpperCase(); }
  const id = num ? `Ҷадвали ${num}${letter}` : `?(${norm(sheetName)})`;
  // название = текст после «Ҷадвали N. », без хвоста из «____» и приписок
  let name = t.replace(re, '').replace(/^[.\s—-]+/, '').trim();
  name = name.split('\n')[0].split(/_{3,}/)[0].replace(/\s{2,}/g, ' ').trim();
  return { id, num: num ? Number(num) : null, letter, name };
}

// ─── Классификация блока по ключевым словам ──────────────────────────────────
function classifyBlock(text) {
  const s = text.toLowerCase();
  if (/ҳисобот|хисобот/.test(s))            return 'Отчёт';
  if (/баҳодиҳӣ|бахо/.test(s))              return 'Оценка';
  if (/параметр/.test(s))                    return 'Прогноз (параметры)';
  if (/дурнамо/.test(s))                     return 'Прогноз';
  if (/ба 01\.01|шумораи умум/.test(s))     return 'База (на начало)';
  return 'н/д';
}

// ─── Классификация подколонки (мера) ─────────────────────────────────────────
// Прим.: заголовки встречаются с разными таджикскими знаками (Ҷ/ҷ U+04B6/07,
// њ U+045A), поэтому меру определяем по устойчивым фрагментам после toLowerCase().
function classifyMeasure(text) {
  const s = text.toLowerCase();
  if (/муқоисавӣ|мукоисави/.test(s))                    return 'объём (сопоставимые цены)';
  if (/суръат|фоиз/.test(s))                             return '%';
  if (/ҳаҷ|хаҷ|ҷм|љм|шумор|нарх|адад|арзиш/.test(s))     return 'объём';
  return 'н/д';
}

// ─── Год из текста ───────────────────────────────────────────────────────────
function parseYear(text) {
  const m = text.match(/(20\d{2})/);
  return m ? Number(m[1]) : null;
}

// ─── Уровень иерархии строки-показателя ──────────────────────────────────────
// Уровень 0 — без префикса; уровень 1 — начинается с «-»; групповые
// заголовки («… :», «аз он ҷумла», «ҳамагӣ», «аз он …») помечаются isGroup.
function classifyIndicator(label) {
  let name = label;
  let level = 0;
  const dash = name.match(/^-\s*/);
  if (dash) { level = 1; name = name.replace(/^-\s*/, ''); }
  name = name.trim();
  const low = name.toLowerCase();
  const isGroup = /:$/.test(name)
    || /^аз он\b/.test(low)
    || /^ҳамагӣ$|^хамаги$/.test(low)
    || /^аз ҷумла|^аз чумла/.test(low);
  return { name, level, isGroup };
}

// ─── Тип таблицы для генератора форм ─────────────────────────────────────────
//   regular — фиксированные показатели (район вписывает числа в готовые строки)
//   entity  — списки предприятий/проектов (район сам добавляет позиции)
//   complex — сложная многоуровневая шапка (>2 подколонок) или пустой
//             плейсхолдер-подтаблица сложной таблицы — требует ручного разбора
// Закреплённые как regular по решению пользователя (пограничные СЭЗ-таблицы —
// списки зон «МОИ «...»», аналогичные Ҷадвали 39, которую договорились оставить).
const REGULAR_PIN = new Set(['Ҷадвали 38', 'Ҷадвали 39']);

function classifyType(res) {
  const f = res.flags || [];
  if (res.empty) return 'complex'; // пустой плейсхолдер (напр. Ҷадвали 4А при Ҷадвали 4)
  if (REGULAR_PIN.has(res.id)) return 'regular';
  if (f.some(x => /предприят|проект|нет строк-показателей/.test(x))) return 'entity';
  if (f.some(x => /сложная шапка/.test(x)))                          return 'complex';
  return 'regular';
}

// ─── Разбор одного листа ─────────────────────────────────────────────────────
function parseSheet(ws, sheetName) {
  const flags = [];
  const { grid, raw, nRows, nCols } = buildGrid(ws);
  if (nRows === 0) {
    const { id, num, letter } = parseIdTitle('', sheetName);
    const eFlags = ['пустой лист (плейсхолдер в шаблоне)'];
    return { id, num, letter, sheetName, title: '', name: '', empty: true,
             type: classifyType({ id, empty: true, flags: eFlags }),
             years: [], blocks: [], columnsCount: 0, columns: [],
             indicatorsCount: 0, indicators: [], flags: eFlags };
  }

  const titleRaw = grid[0] ? grid[0].find(v => v != null) : '';
  const { id, num, letter, name } = parseIdTitle(titleRaw, sheetName);

  // ── Общая единица в скобках (модель B): ищем в строках 1-2 одиночную «(...)» ──
  // Используем «сырой» грид (raw), иначе объединение ячейки на всю строку даёт
  // много значений и одиночная «(адад)» не распознаётся.
  let globalUnit = '';
  for (let r = 1; r <= 2 && r < nRows; r++) {
    const cells = raw[r].filter(v => v != null).map(norm);
    if (cells.length === 1 && /^\(.*\)$/.test(cells[0])) { globalUnit = cells[0].replace(/^\(|\)$/g, '').trim(); break; }
  }

  // ── Признак строки-данных: в колонках-данных (c>=2) нет текста ────────────────
  // ВАЖНО: используем «сырой» грид (raw, до размножения объединений). Иначе
  // строка-раздел данных, объединённая на всю ширину (напр. регион «ВМКБ»),
  // после merge-заполнения выглядит как шапка и сдвигает её границу.
  const isHeaderRow = (r) => {
    for (let c = 2; c < nCols; c++) if (norm(raw[r][c]) !== '') return true;
    return false;
  };

  // ── Границы шапки: от первой непустой строки после title до последней header ──
  let hStart = 1;
  while (hStart < nRows && grid[hStart].every(v => norm(v) === '')) hStart++;
  // если строка hStart — это общая единица «(...)», сдвигаемся ниже
  if (globalUnit && hStart < nRows) {
    const cells = raw[hStart].filter(v => v != null).map(norm);
    if (cells.length === 1 && /^\(.*\)$/.test(cells[0])) hStart++;
  }
  let hEnd = hStart;
  while (hEnd < nRows && isHeaderRow(hEnd)) hEnd++;
  hEnd -= 1; // последняя строка шапки
  if (hEnd < hStart) { flags.push('шапка не распознана'); hEnd = hStart; }

  // «Строка мер» (бо ҳаҷм/бо фоиз/арзиш/…) — надёжная нижняя граница шапки.
  // Ищем ТОЛЬКО внутри уже определённой (по raw) шапки [hStart..hEnd] и лишь
  // сужаем границу вниз: защищает от утечки чисел из частично заполненных
  // шаблонов, но не «съедает» объединённые строки-разделы данных.
  const MEASURE_TOK = /ҳаҷ|хаҷ|ҷм|љм|фоиз|шумор|арзиш|муқоис|мукоис|суръат|нарх/i;
  let measureRow = -1, bestCnt = 1;
  for (let r = hStart; r <= hEnd; r++) {
    let cnt = 0;
    for (let c = 0; c < nCols; c++) if (MEASURE_TOK.test(norm(grid[r][c]))) cnt++;
    if (cnt >= 2 && cnt >= bestCnt) { bestCnt = cnt; measureRow = r; }
  }
  if (measureRow >= hStart) hEnd = measureRow;

  // ── Модель единиц: колонка «Воҳиди ченак» (обычно c=1) ────────────────────────
  let unitCol = null, firstDataCol = 1;
  for (let c = 0; c < Math.min(3, nCols); c++) {
    for (let r = hStart; r <= hEnd; r++) {
      if (/во[ҳх]иди ченак/i.test(norm(grid[r][c]))) { unitCol = c; break; }
    }
    if (unitCol != null) break;
  }
  firstDataCol = (unitCol != null) ? unitCol + 1 : 1;
  const unitModel = (unitCol != null) ? 'A (колонка «Воҳиди ченак»)'
                   : (globalUnit ? 'B (общая единица)' : 'B (единица не указана)');

  // ── Столбцы: год × блок × мера ───────────────────────────────────────────────
  const columns = [];
  const yearsSet = new Set();
  const blocksSet = new Set();
  for (let c = firstDataCol; c < nCols; c++) {
    // объединяем весь текст шапки этой колонки
    let hdrTop = '', hdrBottom = '';
    const parts = [];
    for (let r = hStart; r <= hEnd; r++) {
      const v = norm(grid[r][c]);
      if (v) parts.push(v);
    }
    if (!parts.length) continue;
    hdrTop = parts[0]; hdrBottom = parts[parts.length - 1];
    const joined = parts.join(' | ');
    const block = classifyBlock(joined);
    const year = parseYear(joined);
    const measure = classifyMeasure(joined);
    if (year) yearsSet.add(year);
    if (block !== 'н/д') blocksSet.add(block);
    columns.push({ col: c, block, year, measure, rawHeader: joined });
  }

  // ── Строки-показатели ────────────────────────────────────────────────────────
  const indicators = [];
  for (let r = hEnd + 1; r < nRows; r++) {
    const label = norm(grid[r][0]);
    if (!label) continue;
    if (/^\\+$/.test(label)) continue; // мусорные строки «\»
    const { name: iname, level, isGroup } = classifyIndicator(label);
    const unit = (unitCol != null) ? norm(grid[r][unitCol]) : globalUnit;
    indicators.push({ row: r, level, isGroup, name: iname, unit });
  }

  // ── Пометки о проблемных листах ──────────────────────────────────────────────
  if (indicators.length === 0)
    flags.push('нет строк-показателей (пустой список / сущностная таблица — заполняется вручную)');
  const entityLike = indicators.filter(i => /[«»"]|ҶДММ|ҶСК|ҶСП|МОИ|ҶДС/.test(i.name)).length;
  if (indicators.length && entityLike / indicators.length > 0.4)
    flags.push('строки — названия предприятий/проектов, а не фиксированные показатели');
  if (!yearsSet.size) flags.push('годы в шапке не распознаны');
  if (!blocksSet.size) flags.push('блоки (Отчёт/Оценка/Прогноз) не распознаны');
  // Ҷадвали 1 и подобные: >2 мер на один год = сложная многоуровневая шапка
  const perYear = {};
  for (const c of columns) if (c.year) (perYear[c.year] = perYear[c.year] || []).push(c.measure);
  const maxMeasures = Math.max(0, ...Object.values(perYear).map(a => a.length));
  if (maxMeasures > 2) flags.push('сложная шапка: >2 подколонок на год (проверить вручную)');

  const type = classifyType({ id, empty: false, flags });

  return {
    id, num, letter,
    sheetName,
    title: norm(titleRaw).split('\n')[0].slice(0, 200),
    name,
    type,
    unitModel,
    globalUnit: globalUnit || null,
    headerRows: [hStart, hEnd],
    years: [...yearsSet].sort(),
    blocks: [...blocksSet],
    columnsCount: columns.length,
    columns,
    indicatorsCount: indicators.length,
    indicators,
    flags,
  };
}

// ─── main ────────────────────────────────────────────────────────────────────
function main() {
  const templatePath = findSource(/template.*\.xls$|template.*\.xlsx$|template/i);
  console.log('[extract] Шаблон:', path.relative(process.cwd(), templatePath));
  const wb = XLSX.readFile(templatePath, { cellStyles: false });
  console.log('[extract] Листов:', wb.SheetNames.length);

  const tables = [];
  const problematic = [];
  for (const sheetName of wb.SheetNames) {
    const res = parseSheet(wb.Sheets[sheetName], sheetName);
    tables.push(res);
    if (res.flags && res.flags.length) problematic.push({ id: res.id, sheetName, flags: res.flags });
  }

  const byType = { regular: [], entity: [], complex: [] };
  for (const t of tables) (byType[t.type] || (byType[t.type] = [])).push(t.id);

  const out = {
    generatedFrom: path.basename(templatePath),
    sheetsTotal: wb.SheetNames.length,
    mainTables: tables.filter(t => t.letter === '').length,
    subTables: tables.filter(t => t.letter).length,
    byType: { regular: byType.regular.length, entity: byType.entity.length, complex: byType.complex.length },
    byTypeLists: byType,
    tables,
    problematic,
  };
  fs.writeFileSync(OUT_FILE, JSON.stringify(out, null, 2), 'utf8');
  console.log('[extract] Записано →', path.relative(process.cwd(), OUT_FILE));
  console.log(`[extract] Таблиц: ${tables.length} (основных ${out.mainTables}, подтаблиц ${out.subTables}); проблемных: ${problematic.length}`);
  console.log(`[extract] Типы: regular=${byType.regular.length}, entity=${byType.entity.length}, complex=${byType.complex.length}`);
}

if (require.main === module) main();
module.exports = { parseSheet, parseIdTitle };
