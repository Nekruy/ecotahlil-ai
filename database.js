'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  database.js — файловое хранилище (JSON) без внешних зависимостей
//  Файлы: users.json, district_reports.json
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');

const USERS_FILE   = path.join(__dirname, 'users.json');
const REPORTS_FILE = path.join(__dirname, 'district_reports.json');

// ─── Утилиты ────────────────────────────────────────────────────────────────

function readJSON(file, def) {
  try {
    if (!fs.existsSync(file)) return def;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch { return def; }
}

function writeJSON(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

function hashPassword(plain) {
  return crypto.createHash('sha256').update(plain).digest('hex');
}

// ─── Пользователи ───────────────────────────────────────────────────────────

function getUsers() { return readJSON(USERS_FILE, []); }
function getUserByLogin(login) { return getUsers().find(u => u.login === login) || null; }
function getUserById(id) { return getUsers().find(u => u.id === id) || null; }

function updateUser(id, updates) {
  const users = getUsers();
  const idx = users.findIndex(u => u.id === id);
  if (idx === -1) return;
  users[idx] = { ...users[idx], ...updates };
  writeJSON(USERS_FILE, users);
}

// ─── Отчёты ─────────────────────────────────────────────────────────────────

function getReports(filters = {}) {
  const all = readJSON(REPORTS_FILE, []);
  return all.filter(r => {
    if (filters.userId   && r.userId   !== filters.userId)   return false;
    if (filters.district && r.district !== filters.district) return false;
    if (filters.region   && r.region   !== filters.region)   return false;
    if (filters.from     && r.date     <  filters.from)      return false;
    if (filters.to       && r.date     >  filters.to)        return false;
    return true;
  });
}

function saveReport(report) {
  const reports = readJSON(REPORTS_FILE, []);
  const rec = {
    id:        `r_${Date.now()}`,
    createdAt: new Date().toISOString(),
    ...report,
  };
  reports.push(rec);
  writeJSON(REPORTS_FILE, reports);
  return rec;
}

// ─── Инициализация начальных пользователей ───────────────────────────────────

// [login, password, displayName, role, region, district]
const INITIAL_USERS = [
  // ── Душанбе ──
  ['dushanbe',      'dushanbe123',      'Отдел г. Душанбе',          'district', 'Душанбе', 'г. Душанбе'],
  // ── Согд ──────
  ['khujand',       'khujand123',       'Отдел г. Худжанд',          'district', 'Согд', 'г. Худжанд'],
  ['isfara',        'isfara123',        'Отдел Исфары',              'district', 'Согд', 'Исфара'],
  ['istaravshan',   'istaravshan123',   'Отдел Истаравшана',         'district', 'Согд', 'Истаравшан'],
  ['penjakent',     'penjakent123',     'Отдел Пенджикента',         'district', 'Согд', 'Пенджикент'],
  ['konibodom',     'konibodom123',     'Отдел Конибодома',          'district', 'Согд', 'Конибодом'],
  ['buston',        'buston123',        'Отдел Бустона',             'district', 'Согд', 'Бустон'],
  ['mastchoh',      'mastchoh123',      'Отдел Мастчоха',            'district', 'Согд', 'Мастчох'],
  ['spitamen',      'spitamen123',      'Отдел Спитамена',           'district', 'Согд', 'Спитамен'],
  // ── Хатлон ───
  ['kulob',         'kulob123',         'Отдел г. Куляба',           'district', 'Хатлон', 'г. Куляб'],
  ['qurghonteppa',  'qurghonteppa123',  'Отдел г. Курган-Тюбе',     'district', 'Хатлон', 'г. Курган-Тюбе'],
  ['vakhsh',        'vakhsh123',        'Отдел Вахша',               'district', 'Хатлон', 'Вахш'],
  ['danghara',      'danghara123',      'Отдел Дангары',             'district', 'Хатлон', 'Дангара'],
  ['muminobod',     'muminobod123',     'Отдел Муминобода',          'district', 'Хатлон', 'Муминобод'],
  ['vose',          'vose123',          'Отдел Восе',                'district', 'Хатлон', 'Восе'],
  ['hamadoni',      'hamadoni123',      'Отдел Хамадони',            'district', 'Хатлон', 'Хамадони'],
  ['shahrituz',     'shahrituz123',     'Отдел Шахритуза',           'district', 'Хатлон', 'Шахритуз'],
  ['panj',          'panj123',          'Отдел Пянджа',              'district', 'Хатлон', 'Пяндж'],
  ['baljuvon',      'baljuvon123',      'Отдел Балджувона',          'district', 'Хатлон', 'Балджувон'],
  // ── ГБАО ─────
  ['khorog',        'khorog123',        'Отдел г. Хорога',           'district', 'ГБАО', 'г. Хорог'],
  ['ishkoshim',     'ishkoshim123',     'Отдел Ишкашима',            'district', 'ГБАО', 'Ишкашим'],
  ['rushan',        'rushan123',        'Отдел Рушана',              'district', 'ГБАО', 'Рушан'],
  ['shugnan',       'shugnan123',       'Отдел Шугнана',             'district', 'ГБАО', 'Шугнан'],
  ['murghob',       'murghob123',       'Отдел Мургаба',             'district', 'ГБАО', 'Мургаб'],
  // ── РРП ──────
  ['tursunzoda',    'tursunzoda123',    'Отдел Турсунзоды',          'district', 'РРП', 'Турсунзода'],
  ['hisor',         'hisor123',         'Отдел Гиссара',             'district', 'РРП', 'Гиссар'],
  ['varzob',        'varzob123',        'Отдел Варзоба',             'district', 'РРП', 'Варзоб'],
  ['rudaki',        'rudaki123',        'Отдел Рудаки',              'district', 'РРП', 'Рудаки'],
  ['faizobod',      'faizobod123',      'Отдел Файзобода',           'district', 'РРП', 'Файзобод'],
  ['nurobod',       'nurobod123',       'Отдел Нурободода',          'district', 'РРП', 'Нурободод'],
  ['rasht',         'rasht123',         'Отдел Рашта',               'district', 'РРП', 'Рашт'],
  ['tavildara',     'tavildara123',     'Отдел Тавилдары',           'district', 'РРП', 'Тавилдара'],
  // ── Областные ─
  ['sogd_admin',    'sogd2024',    'Управление Согдской области',    'region',   'Согд',   ''],
  ['khatlon_admin', 'khatlon2024', 'Управление Хатлонской области',  'region',   'Хатлон', ''],
  ['gbao_admin',    'gbao2024',    'Управление ГБАО',                'region',   'ГБАО',   ''],
  ['rrp_admin',     'rrp2024',     'Управление РРП',                 'region',   'РРП',    ''],
  // ── Центр ─────
  ['admin',         'merit2024',   'Администратор МЭРиТ',            'admin',    '',       ''],
];

function initDB() {
  if (fs.existsSync(USERS_FILE)) return;   // уже инициализировано

  console.log('[DB] Инициализация пользователей...');
  const users = INITIAL_USERS.map(([login, pwd, name, role, region, district], i) => ({
    id:        `u_${String(i + 1).padStart(3, '0')}`,
    login,
    password:  hashPassword(pwd),
    name,
    role,
    region,
    district,
    phone:     '',
    lastLogin: null,
  }));

  writeJSON(USERS_FILE, users);
  console.log(`[DB] Создано ${users.length} пользователей.`);
}

module.exports = {
  hashPassword,
  getUsers, getUserByLogin, getUserById, updateUser,
  getReports, saveReport,
  initDB,
};
