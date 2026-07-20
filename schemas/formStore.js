'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/formStore.js — файловое хранилище черновиков отчётов «39 таблиц»
//
//  Отдельный модуль Этапа 2. НЕ трогает district_reports.json и database.js
//  (система дневных цен работает как прежде). Черновики таблиц лежат в
//  собственном файле schemas/table_reports.json в той же JSON-парадигме проекта.
//
//  Ключ записи: район (login) + год/цикл + id таблицы. Каждая запись помечена
//  источником (source) и статусом (draft/submitted).
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const STORE_FILE = path.join(__dirname, 'table_reports.json');

function readAll() {
  try {
    if (!fs.existsSync(STORE_FILE)) return [];
    return JSON.parse(fs.readFileSync(STORE_FILE, 'utf8'));
  } catch { return []; }
}

function writeAll(list) {
  fs.writeFileSync(STORE_FILE, JSON.stringify(list, null, 2), 'utf8');
}

// Ключ уникальности черновика: район + таблица + цикл прогноза
function keyOf(login, tableId, cycle) {
  return `${login}::${tableId}::${cycle || ''}`;
}

// ─── Загрузить черновик района по таблице/циклу ──────────────────────────────
function getDraft(user, tableId, cycle) {
  const key = keyOf(user.login, tableId, cycle);
  return readAll().find(r => r.key === key) || null;
}

// ─── Сохранить/обновить черновик (upsert) ────────────────────────────────────
function saveDraft(user, { tableId, tableName, cycle, values, status, source }, nowIso) {
  const list = readAll();
  const key  = keyOf(user.login, tableId, cycle);
  const idx  = list.findIndex(r => r.key === key);

  const base = {
    key,
    // — привязка к району (роль района переиспользуется из auth) —
    userId:   user.id,
    login:    user.login,
    name:     user.name,
    role:     user.role,
    region:   user.region,
    district: user.district,
    // — что за отчёт —
    tableId,
    tableName: tableName || '',
    cycle:    cycle || '',          // напр. «2027-2029»
    year:     cycle || '',          // маркировка «год» (цикл прогноза)
    // источник: по умолчанию ручной ввод района через форму; импортёр передаёт своё
    source:   source || 'district-form-manual',
    status:   status || 'draft',      // draft | submitted
    values:   values || {},           // { "r5": { "2026": 123.4, ... }, ... } — только объёмы
  };

  if (idx === -1) {
    list.push({ ...base, createdAt: nowIso, updatedAt: nowIso });
  } else {
    list[idx] = { ...list[idx], ...base, createdAt: list[idx].createdAt, updatedAt: nowIso };
  }
  writeAll(list);
  return list[idx === -1 ? list.length - 1 : idx];
}

// ─── Все черновики района (для прогресса по таблицам) ────────────────────────
function getUserDrafts(user) {
  return readAll().filter(r => r.login === user.login);
}

module.exports = { getDraft, saveDraft, readAll, getUserDrafts, STORE_FILE };
