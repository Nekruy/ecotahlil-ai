'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  auth.js — JWT без внешних библиотек (Node.js crypto)
// ═══════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');
const db = require('./database');

const JWT_SECRET  = process.env.JWT_SECRET || 'ecotahlil-medt-tj-2024-secret-key-!@#';
const TOKEN_TTL   = 24 * 60 * 60; // 24 часа (секунды)
const COOKIE_NAME = 'medt_auth';

// ─── Base64url ───────────────────────────────────────────────────────────────

function b64u(buf) {
  const b = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
  return b.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function b64uDecode(str) {
  // Pad to multiple of 4
  const s = str.replace(/-/g, '+').replace(/_/g, '/');
  return Buffer.from(s + '='.repeat((4 - s.length % 4) % 4), 'base64').toString('utf8');
}

// ─── JWT ─────────────────────────────────────────────────────────────────────

const HEADER = b64u(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));

function signJWT(payload) {
  const pay = b64u(JSON.stringify(payload));
  const sig = b64u(crypto.createHmac('sha256', JWT_SECRET).update(`${HEADER}.${pay}`).digest());
  return `${HEADER}.${pay}.${sig}`;
}

function verifyJWT(token) {
  if (typeof token !== 'string') throw new Error('Токен отсутствует');
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Неверный формат токена');
  const [hdr, pay, sig] = parts;

  const expected = b64u(crypto.createHmac('sha256', JWT_SECRET).update(`${hdr}.${pay}`).digest());

  // Timing-safe comparison
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
    throw new Error('Неверная подпись токена');
  }

  const data = JSON.parse(b64uDecode(pay));
  if (data.exp && data.exp < Math.floor(Date.now() / 1000)) {
    throw new Error('Срок действия токена истёк');
  }
  return data;
}

// ─── Хеширование паролей ────────────────────────────────────────────────────

function hashPassword(plain) {
  return db.hashPassword(plain);
}

// ─── Аутентификация ──────────────────────────────────────────────────────────

function login(loginStr, password) {
  const user = db.getUserByLogin(loginStr);
  if (!user) throw new Error('Неверный логин или пароль');
  if (user.password !== hashPassword(password)) throw new Error('Неверный логин или пароль');

  db.updateUser(user.id, { lastLogin: new Date().toISOString() });

  const now = Math.floor(Date.now() / 1000);
  const payload = {
    id:       user.id,
    login:    user.login,
    name:     user.name,
    role:     user.role,
    region:   user.region,
    district: user.district,
    iat:      now,
    exp:      now + TOKEN_TTL,
  };

  const token = signJWT(payload);
  const pub   = { id: user.id, login: user.login, name: user.name,
                  role: user.role, region: user.region, district: user.district };
  return { token, user: pub };
}

// ─── Извлечение токена из запроса ────────────────────────────────────────────

function extractToken(req) {
  // 1. Authorization: Bearer <token>
  const auth = req.headers['authorization'] || '';
  if (auth.startsWith('Bearer ')) return auth.slice(7);

  // 2. Cookie: medt_auth=<token>
  const cookies = req.headers['cookie'] || '';
  const m = cookies.match(new RegExp(`(?:^|;)\\s*${COOKIE_NAME}=([^;]+)`));
  if (m) return m[1];

  return null;
}

// ─── Middleware — проверка токена ────────────────────────────────────────────

function verifyToken(req) {
  const token = extractToken(req);
  if (!token) throw new Error('Нет токена авторизации');
  return verifyJWT(token);
}

// ─── Заголовки Set-Cookie / Clear-Cookie ────────────────────────────────────

function makeCookie(token) {
  return `${COOKIE_NAME}=${token}; HttpOnly; Path=/; Max-Age=${TOKEN_TTL}; SameSite=Strict`;
}

function clearCookie() {
  return `${COOKIE_NAME}=; HttpOnly; Path=/; Max-Age=0; SameSite=Strict`;
}

module.exports = { login, verifyToken, hashPassword, signJWT, verifyJWT, makeCookie, clearCookie, COOKIE_NAME };
