'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/territoryData.js — сбор данных дашборда по ЛЮБОЙ территории
//
//  Единый источник для: экспортёра (exportSogd.js) и живого дашборда
//  (formServer /dashboard-data). Берёт реально сохранённые значения из
//  formStore по логину территории и раскладывает по схеме tables.json.
//  Только заполненные таблицы и только показатели, у которых есть числа.
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');
const store       = require('./formStore');
const { cycleOf } = require('./formGenerator');

const SCHEMA_FILE = path.join(__dirname, 'tables.json');
const TERR_FILE   = path.join(__dirname, 'territories.json');
const YEARS       = [2025, 2026, 2027, 2028, 2029];

function loadJSON(f) { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return null; } }

// единица измерения: поле схемы, иначе — из скобок в конце названия
function unitOf(ind, ru) {
  const u = (ind.unit_ru || ind.unit_tg || ind.unit || '').trim();
  if (u && u !== ind.name && u !== ind.name_ru && u !== ind.name_tg) return u;
  const m = String(ru || ind.name || '').match(/\(([^()]*(?:млн|млрд|ҳазор|хазор|сомон|доллар|тонна|квт|кВт|гкал|нафар|адад|дона|га|фоиз|%|воҳид|кат|м3|м³|литр|гектар|метр|коек|тыс)[^()]*)\)\s*$/i);
  return m ? m[1].trim() : '';
}

// номер формы из id: «Ҷадвали 12» → «12», «Ҷадвали 10А» → «10А»
function tableNum(id) {
  const m = String(id).match(/(\d+\s*[абвАБВ]?)/);
  return m ? m[1].replace(/\s/g, '') : id;
}

// человекочитаемая метка источника
function sourceLabel(src) {
  if (src === 'official-report-import') return { ru: 'Официальный отчёт (импорт)', tg: 'Ҳисоботи расмӣ (импорт)' };
  if (src === 'district-form-manual')   return { ru: 'Ручной ввод территории',     tg: 'Воридкунии дастӣ' };
  return { ru: src || 'н/д', tg: src || 'н/д' };
}

// ─── основная сборка ─────────────────────────────────────────────────────────
function buildTerritoryData(login) {
  const schema = loadJSON(SCHEMA_FILE) || { tables: [] };
  const terrs  = loadJSON(TERR_FILE) || [];
  const meta   = terrs.find(t => t.login === login) || null;

  const regular = schema.tables.filter(t => t.type === 'regular');
  const byKey   = new Map(
    store.readAll().filter(r => r.login === login).map(d => [d.tableId + '::' + (d.cycle || ''), d])
  );

  const tables = [];
  const srcSet = new Set();
  let filledCells = 0;

  for (const t of regular) {
    const rec = byKey.get(t.id + '::' + cycleOf(t));
    if (!rec || !rec.values || !Object.keys(rec.values).length) continue;
    if (rec.source) srcSet.add(rec.source);

    const indicators = [];
    t.indicators.forEach((ind, idx) => {
      if (ind.isGroup) return;
      const byYear = rec.values['r' + idx];
      if (!byYear) return;
      const values = {};
      let has = false;
      for (const y of YEARS) {
        const v = byYear[y];
        if (v !== '' && v != null && isFinite(v)) { values[y] = v; has = true; filledCells++; }
        else values[y] = null;
      }
      if (!has) return;
      indicators.push({
        name_ru: ind.name_ru || ind.name_tg || ind.name || '',
        name_tg: ind.name_tg || ind.name || '',
        unit:    unitOf(ind, ind.name_ru),
        values,
      });
    });
    if (!indicators.length) continue;

    tables.push({
      id:      t.id,
      num:     tableNum(t.id),
      name_ru: t.name_ru || t.name || t.id,
      name_tg: t.name_tg || t.name || t.id,
      unit:    (t.globalUnit || '').trim() || null,
      cycle:   cycleOf(t),
      indicators,
    });
  }

  const sources = [...srcSet];
  const src     = sources[0] || null;

  return {
    territory: meta
      ? { login, name_ru: meta.name_ru, name_tg: meta.name_tg, region: meta.region, type: meta.type }
      : { login, name_ru: login, name_tg: login, region: '', type: '' },
    source:      src,
    sourceLabel: sourceLabel(src),
    sources,
    years:       YEARS,
    totalTables: regular.length,
    filledTables: tables.length,
    filledCells,
    tables,
  };
}

module.exports = { buildTerritoryData, unitOf, tableNum, YEARS };
