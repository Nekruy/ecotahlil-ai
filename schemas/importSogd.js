'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/importSogd.js — импорт реального отчёта Согдийской области
//
//  Читает schemas/source/sample_sogd.xlsx.xlsx (официальный заполненный отчёт
//  по тем же формам) и раскладывает числа по regular-таблицам системы, затем
//  сохраняет как ОТЧЁТ ТЕРРИТОРИИ «Согдийская область» (login: sogd) через
//  тот же formStore, что и ручной ввод района.
//
//  ЧЕСТНОСТЬ ИСТОЧНИКА:
//    • source = 'official-report-import'  (НЕ выдаётся за ручной ввод района);
//    • status = 'draft' — статус (нет/черновик/готово) выводится из реального
//      процента заполнения, а не форсируется в «готово».
//
//  ПОЧЕМУ ПО СДВИГУ (offset), А НЕ ПО КООРДИНАТАМ:
//    Схема tables.json — национальный шаблон, а sample_sogd — областной отчёт.
//    Столбцы (годы/объёмы) совпадают точно, а строки сдвинуты и частично иные.
//    Поэтому: по опорным показателям (совпадение названий) вычисляем ЕДИНЫЙ
//    сдвиг строк листа относительно схемы, затем читаем значения по этому
//    сдвигу. Таблицы без устойчивого сдвига или без листа честно остаются
//    пустыми (нули на экране центра) — ничего не выдумываем.
//
//  Запуск:  node schemas/importSogd.js         (сухой прогон + запись)
//           node schemas/importSogd.js --dry    (только отчёт покрытия)
//  Существующий код не меняется; повторный запуск идемпотентен (upsert по ключу).
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const store       = require('./formStore');
const { cycleOf } = require('./formGenerator');

const SCHEMA_FILE = path.join(__dirname, 'tables.json');
const XLSX_FILE   = path.join(__dirname, 'source', 'sample_sogd.xlsx.xlsx');

// территория-получатель (должна присутствовать в territories.json)
const TARGET = {
  id:       'terr-sogd',
  login:    'sogd',
  name:     'Согдийская область',
  role:     'region',
  region:   'Согд',
  district: '',
};

const SOURCE_TAG = 'official-report-import';

// ─── нормализация названий (устойчиво к регистру/пробелам/дефисам) ────────────
const norm = x => String(x == null ? '' : x)
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^а-яa-z0-9]/g, '');

// номер таблицы: «Ҷадвали 10а» ↔ «Чадвали 10А.» → «10а»
function normNum(str) {
  const m = String(str).match(/1[0-9]?[абвАБВ]|[0-9]+[абвАБВ]?/);
  return m ? m[0].replace(/\s/g, '').toLowerCase() : '';
}

// ─── читаем метку 1-го столбца по каждой строке листа (1-based индекс) ────────
function sheetLabels(ws) {
  const ref = XLSX.utils.decode_range(ws['!ref']);
  const out = [];
  for (let r = ref.s.r; r <= ref.e.r; r++) {
    const a = XLSX.utils.encode_cell({ r, c: 0 });
    const v = ws[a];
    out[r + 1] = v ? String(v.v) : '';   // out[excelRow] = label
  }
  return out;
}

function cellNum(ws, row1, col1) {
  const a = XLSX.utils.encode_cell({ r: row1 - 1, c: col1 - 1 });
  const v = ws[a];
  if (!v) return null;
  if (typeof v.v === 'number' && isFinite(v.v)) return v.v;
  const n = parseFloat(String(v.v).replace(/\s/g, '').replace(',', '.'));
  return isFinite(n) ? n : null;
}

function cellStr(ws, row1, col1) {
  const a = XLSX.utils.encode_cell({ r: row1 - 1, c: col1 - 1 });
  const v = ws[a];
  return v ? String(v.v) : '';
}

// ─── найти объёмные столбцы листа ────────────────────────────────────────────
// Столбец «%» всегда подписан «...фоиз», а раскладка идёт парами [объём, %].
// Поэтому объёмный столбец = столбец «фоиз» − 1. Это не зависит от того, как
// назван объём («бо ҳаҷм», «бо шумора», «бо арзиш», «бо њачм» …) и есть ли
// колонка единиц измерения.
function detectVolCols(ws) {
  const ref = XLSX.utils.decode_range(ws['!ref']);
  const isPct = t => /фо[ий]з/i.test(String(t));
  for (let r = ref.s.r; r <= Math.min(ref.e.r, ref.s.r + 14); r++) {
    const pctCols = [];
    for (let c = ref.s.c; c <= ref.e.c; c++) {
      const v = ws[XLSX.utils.encode_cell({ r, c })];
      if (v && isPct(v.v)) pctCols.push(c + 1);   // 1-based
    }
    if (pctCols.length >= 2) {
      return { headerRow: r + 1, volCols: pctCols.map(c => c - 1).filter(c => c >= 1) };
    }
  }
  return { headerRow: null, volCols: [] };
}

// ─── определить единый сдвиг строк листа относительно схемы ───────────────────
function detectDelta(indicators, labels) {
  // индекс листа: нормализованная метка → первая строка, где встретилась
  const li = {};
  labels.forEach((lab, r) => {
    const k = norm(lab);
    if (k.length >= 6 && !(k in li)) li[k] = r;
  });
  const votes = {};
  let anchors = 0;
  for (const ind of indicators) {
    const k = norm(ind.name);
    if (k.length < 6) continue;
    if (li[k] != null) { const d = li[k] - ind.row; votes[d] = (votes[d] || 0) + 1; anchors++; }
  }
  const sorted = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  if (!sorted.length) return { delta: null, anchors, conf: 0, agree: 0 };
  const [delta, agree] = sorted[0];
  return { delta: Number(delta), anchors, agree, conf: agree / anchors };
}

// ─── основной проход ─────────────────────────────────────────────────────────
function run({ dry }) {
  const schema = JSON.parse(fs.readFileSync(SCHEMA_FILE, 'utf8'));
  const wb     = XLSX.readFile(XLSX_FILE);
  const tables = schema.tables.filter(t => t.type === 'regular');
  const nowIso = new Date().toISOString();

  const report = [];
  let savedCount = 0, filledCellsTotal = 0;

  for (const t of tables) {
    const sheetName = wb.SheetNames.find(n => normNum(n) === normNum(t.id));
    const inds = t.indicators.filter(i => !i.isGroup);
    // все объёмные (не %) колонки таблицы
    const volCols = t.columns.filter(c => c.measure !== '%');
    const total   = inds.length * volCols.length;

    if (!sheetName) { report.push({ id: t.id, sheet: null, status: 'нет листа', matched: 0, total, filled: 0, delta: null }); continue; }

    const ws     = wb.Sheets[sheetName];
    const labels = sheetLabels(ws);
    const { delta, anchors, agree, conf } = detectDelta(inds, labels);

    // порог доверия к сдвигу: минимум 2 согласных якоря и ≥50% голосов
    const ok = delta != null && agree >= 2 && conf >= 0.5;
    if (!ok) { report.push({ id: t.id, sheet: sheetName, status: 'нет устойчивого сдвига', matched: anchors, total, filled: 0, delta, conf }); continue; }

    // объёмные столбцы листа (по порядку годов) сопоставляем позиционно
    // со столбцами схемы (тоже по порядку годов 2025…2029)
    const sampleVolCols = detectVolCols(ws).volCols;
    if (!sampleVolCols.length) { report.push({ id: t.id, sheet: sheetName, status: 'не найдены столбцы', matched: anchors, total, filled: 0, delta, conf }); continue; }
    const nPairs = Math.min(volCols.length, sampleVolCols.length);

    // читаем значения по сдвигу; ключи — как в форме: 'r'+индекс в table.indicators
    const values = {};
    let filled = 0, matchedRows = 0;
    t.indicators.forEach((ind, idx) => {
      if (ind.isGroup) return;
      const srcRow = ind.row + delta;
      let rowHas = false;
      for (let k = 0; k < nPairs; k++) {
        const year = volCols[k].year;          // год из схемы (2025…2029)
        const num  = cellNum(ws, srcRow, sampleVolCols[k]);
        if (num != null) {
          (values['r' + idx] = values['r' + idx] || {})[year] = num;
          filled++; rowHas = true;
        }
      }
      if (rowHas) matchedRows++;
    });

    filledCellsTotal += filled;
    report.push({ id: t.id, sheet: sheetName, status: filled ? 'ок' : 'пусто', matched: matchedRows, total, filled, delta, conf });

    if (!dry && filled > 0) {
      store.saveDraft(TARGET, {
        tableId:   t.id,
        tableName: t.name_ru || t.name || t.id,
        cycle:     cycleOf(t),
        values,
        status:    'draft',
        source:    SOURCE_TAG,
      }, nowIso);
      savedCount++;
    }
  }

  // ── отчёт покрытия ──
  console.log('\n─── ИМПОРТ ОТЧЁТА СОГДА → территория «' + TARGET.name + '» (' + TARGET.login + ') ───');
  console.log('источник:', SOURCE_TAG, dry ? '[СУХОЙ ПРОГОН — без записи]' : '[запись включена]');
  console.log('таблица         лист               сдвиг  строк   ячеек/итого   %     статус');
  for (const r of report) {
    const pct = r.total ? Math.round(100 * r.filled / r.total) : 0;
    console.log(
      r.id.padEnd(14),
      String(r.sheet || '—').slice(0, 16).padEnd(16),
      String(r.delta == null ? '—' : r.delta).padStart(5),
      String(r.matched).padStart(6),
      (r.filled + '/' + r.total).padStart(12),
      (pct + '%').padStart(6),
      '  ' + r.status,
    );
  }
  const okTables = report.filter(r => r.filled > 0).length;
  console.log('\nИТОГО: таблиц с данными ' + okTables + '/' + tables.length +
              ', заполнено ячеек ' + filledCellsTotal +
              (dry ? '' : ', сохранено записей ' + savedCount));
  if (dry) console.log('Это сухой прогон. Запусти без --dry, чтобы записать в хранилище.');
  return report;
}

run({ dry: process.argv.includes('--dry') });
