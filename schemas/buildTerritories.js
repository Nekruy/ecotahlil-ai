'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/buildTerritories.js — справочник территорий (города/районы)
//
//  Строит schemas/territories.json из users.json (role=district) + карты
//  классификации. Структура записи:
//    { login, type, name_ru, name_tg, region, lat, lng }
//  type: "city" (шаҳр) | "district" (ноҳия). lat/lng пока пустые (null) —
//  заполним позже для карты в кабинете администратора.
//
//  Ничего в рабочем коде не меняет (users.json/database.js/auth.js — как есть).
//  Запуск: node schemas/buildTerritories.js
// ═══════════════════════════════════════════════════════════════════════════

const fs   = require('fs');
const path = require('path');

const USERS = path.join(__dirname, '..', 'users.json');
const OUT   = path.join(__dirname, 'territories.json');

// login → { type, name_ru, name_tg, flag? }
// flag — помечены территории, требующие вашей сверки (спорный тип/название).
const CLASSIFY = {
  // ── Душанбе ──
  dushanbe:     { type: 'city',     name_ru: 'г. Душанбе',            name_tg: 'ш. Душанбе' },
  // ── Согд ──
  khujand:      { type: 'city',     name_ru: 'г. Худжанд',           name_tg: 'ш. Хуҷанд' },
  isfara:       { type: 'city',     name_ru: 'г. Исфара',            name_tg: 'ш. Исфара' },
  istaravshan:  { type: 'city',     name_ru: 'г. Истаравшан',        name_tg: 'ш. Истаравшан' },
  penjakent:    { type: 'city',     name_ru: 'г. Пенджикент',        name_tg: 'ш. Панҷакент' },
  konibodom:    { type: 'city',     name_ru: 'г. Канибадам',         name_tg: 'ш. Конибодом' },
  buston:       { type: 'city',     name_ru: 'г. Бустон',            name_tg: 'ш. Бӯстон' },
  mastchoh:     { type: 'district', name_ru: 'Матчинский район',     name_tg: 'ноҳияи Мастчоҳ' },
  spitamen:     { type: 'district', name_ru: 'Спитаменский район',   name_tg: 'ноҳияи Спитамен' },
  // ── Хатлон ──
  kulob:        { type: 'city',     name_ru: 'г. Куляб',             name_tg: 'ш. Кӯлоб' },
  qurghonteppa: { type: 'city',     name_ru: 'г. Бохтар (Курган-Тюбе)', name_tg: 'ш. Бохтар', flag: 'город переименован в Бохтар — подтвердите название' },
  vakhsh:       { type: 'district', name_ru: 'Вахшский район',       name_tg: 'ноҳияи Вахш' },
  danghara:     { type: 'district', name_ru: 'Дангаринский район',   name_tg: 'ноҳияи Данғара' },
  muminobod:    { type: 'district', name_ru: 'Муминабадский район',  name_tg: 'ноҳияи Муъминобод' },
  vose:         { type: 'district', name_ru: 'Восейский район',      name_tg: 'ноҳияи Восеъ' },
  hamadoni:     { type: 'district', name_ru: 'Хамадонийский район',  name_tg: 'ноҳияи Ҳамадонӣ' },
  shahrituz:    { type: 'district', name_ru: 'Шахритусский район',   name_tg: 'ноҳияи Шаҳритус' },
  panj:         { type: 'district', name_ru: 'Пянджский район',      name_tg: 'ноҳияи Панҷ' },
  baljuvon:     { type: 'district', name_ru: 'Бальджуванский район', name_tg: 'ноҳияи Балҷувон' },
  // ── ГБАО ──
  khorog:       { type: 'city',     name_ru: 'г. Хорог',             name_tg: 'ш. Хоруғ' },
  ishkoshim:    { type: 'district', name_ru: 'Ишкашимский район',    name_tg: 'ноҳияи Ишкошим' },
  rushan:       { type: 'district', name_ru: 'Рушанский район',      name_tg: 'ноҳияи Рӯшон' },
  shugnan:      { type: 'district', name_ru: 'Шугнанский район',     name_tg: 'ноҳияи Шуғнон' },
  murghob:      { type: 'district', name_ru: 'Мургабский район',     name_tg: 'ноҳияи Мурғоб' },
  // ── РРП ──
  tursunzoda:   { type: 'city',     name_ru: 'г. Турсунзаде',        name_tg: 'ш. Турсунзода' },
  hisor:        { type: 'city',     name_ru: 'г. Гиссар',            name_tg: 'ш. Ҳисор', flag: 'статус город/район — подтвердите' },
  varzob:       { type: 'district', name_ru: 'Варзобский район',     name_tg: 'ноҳияи Варзоб' },
  rudaki:       { type: 'district', name_ru: 'район Рудаки',         name_tg: 'ноҳияи Рӯдакӣ' },
  faizobod:     { type: 'district', name_ru: 'Файзабадский район',   name_tg: 'ноҳияи Файзобод' },
  nurobod:      { type: 'district', name_ru: 'Нурабадский район',    name_tg: 'ноҳияи Нуробод' },
  rasht:        { type: 'district', name_ru: 'Раштский район',       name_tg: 'ноҳияи Рашт' },
  tavildara:    { type: 'district', name_ru: 'Тавильдаринский район',name_tg: 'ноҳияи Тавилдара' },
};

function main() {
  const users = JSON.parse(fs.readFileSync(USERS, 'utf8'));
  const terr  = users.filter(u => u.role === 'district');

  const list = [];
  const unmapped = [];
  for (const u of terr) {
    const c = CLASSIFY[u.login];
    if (!c) { unmapped.push(u.login); }
    list.push({
      login:   u.login,
      type:    c ? c.type : 'district',
      name_ru: c ? c.name_ru : u.district,
      name_tg: c ? c.name_tg : u.district,
      region:  u.region,
      lat:     null,           // координаты — заполним позже (для карты админа)
      lng:     null,
    });
  }

  fs.writeFileSync(OUT, JSON.stringify(list, null, 2), 'utf8');

  const cities = list.filter(t => t.type === 'city').length;
  console.log('[terr] Территорий:', list.length, '| городов:', cities, '| районов:', list.length - cities);
  if (unmapped.length) console.log('[terr] БЕЗ классификации (взят district по умолчанию):', unmapped.join(', '));
  const flagged = terr.filter(u => CLASSIFY[u.login] && CLASSIFY[u.login].flag);
  if (flagged.length) { console.log('[terr] Требуют сверки:'); flagged.forEach(u => console.log('   '+u.login+' — '+CLASSIFY[u.login].flag)); }
  console.log('[terr] Записано →', path.relative(process.cwd(), OUT));
}

if (require.main === module) main();
module.exports = { CLASSIFY };
