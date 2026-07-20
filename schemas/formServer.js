'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/formServer.js — автономный сервер-превью ДВУЯЗЫЧНОЙ формы (Этап 2)
//
//  Запуск:  node schemas/formServer.js        (порт FORM_PORT или 3100)
//  Открыть: http://localhost:3100/            (по умолчанию — Ҷадвали 6)
//
//  Переключатель РУС / ТҶ (по умолчанию ТҶ), выбор языка запоминается.
//  НИЧЕГО из существующего кода не меняет: require('../auth'), require('../database').
// ═══════════════════════════════════════════════════════════════════════════

const http = require('http');
const path = require('path');
const fs   = require('fs');

const authModule = require('../auth');
const db         = require('../database');
const store      = require('./formStore');
const { renderFormHtml, cycleOf } = require('./formGenerator');
const { buildTerritoryData }      = require('./territoryData');

const PORT = process.env.FORM_PORT || 3100;
const SCHEMA_FILE = path.join(__dirname, 'tables.json');

db.initDB();

function loadSchema() { return JSON.parse(fs.readFileSync(SCHEMA_FILE, 'utf8')); }
function getTable(id) { return loadSchema().tables.find(t => t.id === id) || null; }

const TERR_FILE = path.join(__dirname, 'territories.json');
function loadTerritories() {
  try { return JSON.parse(fs.readFileSync(TERR_FILE, 'utf8')); } catch { return []; }
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function esc(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// двуязычный текст-узел интерфейса
function bi(tg, ru, tag = 'span', cls = '') {
  return `<${tag}${cls ? ` class="${cls}"` : ''} data-tg="${esc(tg)}" data-ru="${esc(ru)}">${esc(tg)}</${tag}>`;
}

// ─── Прогресс заполнения таблицы районом ─────────────────────────────────────
function computeProgress(table, draft) {
  const volCols  = table.columns.filter(c => c.measure !== '%').length;
  const dataRows = (table.indicators || []).filter(i => !i.isGroup).length;
  const total    = dataRows * volCols;
  let filled = 0;
  if (draft && draft.values) {
    for (const row of Object.values(draft.values))
      filled += Object.values(row).filter(v => v !== '' && v != null && isFinite(v)).length;
  }
  filled = Math.min(filled, total);
  const status = (draft && draft.status === 'submitted') ? 'ready'
               : (!draft || filled === 0) ? 'none'
               : (filled >= total && total > 0) ? 'ready' : 'draft';
  return { total, filled, status };
}

// ─── HTML-страница формы ─────────────────────────────────────────────────────
function renderPage(table) {
  const f = renderFormHtml(table);
  const meta = { tableId: f.tableId, nameTg: f.nameTg, nameRu: f.nameRu, cycle: f.cycle, years: f.years };
  return `<!doctype html>
<html lang="tg"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(f.tableId)} — форма отчёта района</title>
<style>
  :root { --gold:#d4a017; --green:#1a7a3c; --red:#c0392b; --line:#e5e7eb; --bg:#f6f7f9; --ink:#1f2937; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:'Segoe UI',system-ui,Arial,sans-serif; background:var(--bg); color:var(--ink); }
  header { background:#0f2f4f; color:#fff; padding:12px 20px; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  header .t { font-size:15px; font-weight:600; }
  header .who { margin-left:auto; font-size:13px; opacity:.9; }
  header .badge { background:var(--gold); color:#1a1a1a; border-radius:6px; padding:2px 8px; font-weight:600; }
  .lang { display:flex; gap:4px; }
  .lang button { border:1px solid #ffffff55; background:transparent; color:#fff; border-radius:6px; padding:3px 10px; cursor:pointer; font-weight:600; font-size:12px; }
  .lang button.on { background:var(--gold); color:#1a1a1a; border-color:var(--gold); }
  .wrap { padding:16px 20px 80px; }
  .card { background:#fff; border:1px solid var(--line); border-radius:10px; padding:14px 16px; margin-bottom:14px; }
  .h1 { font-size:16px; font-weight:700; margin:0 0 4px; }
  .sub { font-size:13px; color:#6b7280; }
  .tbl-scroll { overflow-x:auto; border:1px solid var(--line); border-radius:10px; background:#fff; }
  table { border-collapse:collapse; width:100%; font-size:13px; }
  th, td { border:1px solid var(--line); padding:5px 8px; text-align:center; }
  thead th { background:#eef2f7; position:sticky; top:0; }
  th.ind-h { text-align:left; min-width:340px; background:#e7edf4; }
  th.grp-h { background:#dde7f1; }
  th.pct-h { background:#faf3df; color:#8a6d1a; }
  td.ind { text-align:left; white-space:normal; max-width:440px; }
  td.ind-unit { color:#6b7280; font-style:italic; }
  tr.grp-row td { background:#f3f4f6; text-align:left; font-weight:700; }
  input.numin { width:88px; padding:4px 6px; border:1px solid #cbd5e1; border-radius:5px; text-align:right; font-size:13px; }
  input.numin:focus { outline:2px solid var(--gold); border-color:var(--gold); }
  input.numin.bad { border-color:var(--red); background:#fdeaea; }
  td.pct { background:#fcfaf3; }
  output.pctout { font-variant-numeric:tabular-nums; color:#8a6d1a; font-weight:600; }
  .bar { position:fixed; left:0; right:0; bottom:0; background:#fff; border-top:1px solid var(--line);
         padding:10px 20px; display:flex; align-items:center; gap:14px; }
  .btn { background:var(--green); color:#fff; border:0; border-radius:7px; padding:8px 16px; font-weight:600; cursor:pointer; }
  .btn.secondary { background:#e5e7eb; color:#111; }
  .status { font-size:13px; color:#6b7280; }
  .legend { font-size:12px; color:#6b7280; margin-top:8px; }
  #gate { position:fixed; inset:0; background:rgba(15,47,79,.55); display:flex; align-items:center; justify-content:center; z-index:50; }
  #gate .box { background:#fff; border-radius:12px; padding:22px 24px; width:330px; }
  #gate h2 { margin:0 0 12px; font-size:16px; }
  #gate input { width:100%; padding:9px 10px; margin-bottom:10px; border:1px solid #cbd5e1; border-radius:7px; font-size:14px; }
  #gate .err { color:var(--red); font-size:13px; min-height:18px; }
  .hidden { display:none !important; }
</style>
</head><body>
<header>
  <a href="/forms" style="color:#fff;text-decoration:none;font-size:13px">${bi('← Рӯйхат', '← Список')}</a>
  <span class="badge" id="hTableId"></span>
  <span class="t"><span class="lbl" data-tg="${esc(f.nameTg)}" data-ru="${esc(f.nameRu)}">${esc(f.nameTg)}</span></span>
  <span class="who">
    <span id="hDistrict"></span> · ${bi('Ноҳия', 'Район')} · ${bi('давра', 'цикл')} <span id="hCycle"></span>
  </span>
  <span class="lang">
    <button id="langTg" onclick="setLang('tg')">ТҶ</button>
    <button id="langRu" onclick="setLang('ru')">РУС</button>
  </span>
</header>

<div class="wrap">
  <div class="card">
    <div class="h1"><span class="lbl" data-tg="${esc(f.nameTg)}" data-ru="${esc(f.nameRu)}">${esc(f.nameTg)}</span></div>
    <div class="sub">${esc(f.tableId)} · ${bi('давра', 'цикл')} ${esc(f.cycle || '—')}</div>
    <div class="legend">${bi(
      'Майдонҳои «Ҳаҷм»-ро пур кунед (танҳо рақамҳо). Сутунҳои «Суръати афзоиш, %» худкор ҳисоб мешаванд. Сиёҳнавис худкор захира мешавад.',
      'Заполните поля «Объём» (только числа). Колонки «Темп роста, %» считаются автоматически. Черновик сохраняется автоматически.'
    )}</div>
  </div>

  <div class="tbl-scroll">
    <table id="reportTable">
      <thead>${f.theadHtml}</thead>
      <tbody>${f.tbodyHtml}</tbody>
    </table>
  </div>
</div>

<div class="bar">
  <button class="btn" id="btnSave">${bi('Захира кардани сиёҳнавис', 'Сохранить черновик')}</button>
  <button class="btn secondary" id="btnReload">${bi('Аз нав хондани захира', 'Перечитать сохранённое')}</button>
  <span class="status" id="saveStatus">—</span>
  <span class="status" style="margin-left:auto"><a href="#" id="btnLogout">${bi('Баромадан', 'Выйти')}</a></span>
</div>

<div id="gate" class="hidden">
  <div class="box">
    <h2>${bi('Вуруди ноҳия', 'Вход района', 'span')}</h2>
    <input id="gLogin" data-tgph="Логини ноҳия (мисол: khujand)" data-ruph="Логин района (напр. khujand)">
    <input id="gPass" type="password" data-tgph="Рамз" data-ruph="Пароль">
    <div class="err" id="gErr"></div>
    <button class="btn" id="gBtn" style="width:100%">${bi('Ворид шудан', 'Войти')}</button>
  </div>
</div>

<script>
const META = ${JSON.stringify(meta)};
const TOKEN_KEY = 'medt_token';
const LANG_KEY  = 'medt_form_lang';
let currentUser = null, saveTimer = null;
let lang = localStorage.getItem(LANG_KEY) || 'tg';

function token(){ return localStorage.getItem(TOKEN_KEY); }
function authHeaders(){ return { 'Authorization':'Bearer '+token(), 'Content-Type':'application/json' }; }
function $(s){ return document.querySelector(s); }

// ── переключение языка ──
function setLang(l){
  lang = (l === 'ru') ? 'ru' : 'tg';
  localStorage.setItem(LANG_KEY, lang);
  document.documentElement.lang = lang;
  document.querySelectorAll('[data-tg]').forEach(el => {
    const v = lang === 'ru' ? (el.dataset.ru || el.dataset.tg) : (el.dataset.tg || el.dataset.ru);
    if (v != null) el.textContent = v;
  });
  document.querySelectorAll('[data-tgph]').forEach(el => {
    el.placeholder = lang === 'ru' ? (el.dataset.ruph || el.dataset.tgph) : (el.dataset.tgph || el.dataset.ruph);
  });
  $('#langTg').classList.toggle('on', lang === 'tg');
  $('#langRu').classList.toggle('on', lang === 'ru');
}

// ── число ──
function parseNum(v){ if(v==null) return NaN; const s=String(v).trim().replace(',', '.'); if(s==='') return NaN; return /^-?\\d*\\.?\\d*$/.test(s)?parseFloat(s):NaN; }

function recalcRow(rowKey){
  document.querySelectorAll('output.pctout[data-row="'+rowKey+'"]').forEach(out => {
    const y=+out.dataset.year;
    const cur=document.querySelector('input.numin[data-row="'+rowKey+'"][data-year="'+y+'"]');
    const prev=document.querySelector('input.numin[data-row="'+rowKey+'"][data-year="'+(y-1)+'"]');
    const cv=cur?parseNum(cur.value):NaN, pv=prev?parseNum(prev.value):NaN;
    out.textContent=(isFinite(cv)&&isFinite(pv)&&pv!==0)?(((cv/pv)-1)*100).toFixed(1):'—';
  });
}
function recalcAll(){ new Set([...document.querySelectorAll('input.numin')].map(i=>i.dataset.row)).forEach(recalcRow); }

function collectValues(){
  const values={};
  document.querySelectorAll('input.numin').forEach(i=>{
    const n=parseNum(i.value);
    i.classList.toggle('bad', i.value.trim()!=='' && !isFinite(n));
    if(isFinite(n)) (values[i.dataset.row]=values[i.dataset.row]||{})[i.dataset.year]=n;
  });
  return values;
}
function ui(tg, ru){ return lang==='ru'?ru:tg; }

async function saveDraft(silent){
  if(!token()) return;
  const values=collectValues();
  try{
    const r=await fetch('/draft',{method:'POST',headers:authHeaders(),
      body:JSON.stringify({tableId:META.tableId, tableName:META.nameRu, cycle:META.cycle, values, status:'draft'})});
    if(!r.ok) throw new Error('save');
    const now=new Date().toLocaleTimeString('ru-RU');
    $('#saveStatus').textContent = ui('Сиёҳнавис захира шуд · ','Черновик сохранён · ')+now;
  }catch{ $('#saveStatus').textContent=ui('Хатои захира','Ошибка сохранения'); }
}
function scheduleSave(){ clearTimeout(saveTimer); $('#saveStatus').textContent=ui('Тағйирот…','Изменения…'); saveTimer=setTimeout(()=>saveDraft(true),900); }

async function loadDraft(){
  const r=await fetch('/draft?tableId='+encodeURIComponent(META.tableId)+'&cycle='+encodeURIComponent(META.cycle),{headers:authHeaders()});
  if(!r.ok) return;
  const {draft}=await r.json();
  if(draft&&draft.values){
    for(const [row,byYear] of Object.entries(draft.values))
      for(const [year,val] of Object.entries(byYear)){
        const inp=document.querySelector('input.numin[data-row="'+row+'"][data-year="'+year+'"]');
        if(inp) inp.value=val;
      }
    $('#saveStatus').textContent=ui('Сиёҳнависи захирашуда: ','Загружен черновик: ')+new Date(draft.updatedAt).toLocaleString('ru-RU');
  }
  recalcAll();
}

function fillHeader(){
  $('#hTableId').textContent=META.tableId;
  $('#hCycle').textContent=META.cycle||'—';
  if(currentUser) $('#hDistrict').textContent=currentUser.district||currentUser.name;
}

async function boot(){
  setLang(lang);
  fillHeader();
  document.querySelectorAll('input.numin').forEach(inp=>inp.addEventListener('input',()=>{recalcRow(inp.dataset.row);scheduleSave();}));
  $('#btnSave').addEventListener('click',()=>saveDraft(false));
  $('#btnReload').addEventListener('click',loadDraft);
  $('#btnLogout').addEventListener('click',e=>{e.preventDefault();localStorage.removeItem(TOKEN_KEY);showGate();});
  $('#gBtn').addEventListener('click',doLogin);

  if(!token()) return showGate();
  const me=await fetch('/me',{headers:authHeaders()});
  if(!me.ok) return showGate();
  currentUser=(await me.json()).user; fillHeader(); await loadDraft();
}
function showGate(){ $('#gate').classList.remove('hidden'); }
async function doLogin(){
  $('#gErr').textContent='';
  const login=$('#gLogin').value.trim(), password=$('#gPass').value;
  try{
    const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({login,password})});
    const d=await r.json();
    if(!r.ok) throw new Error(d.error||ui('Хатои вуруд','Ошибка входа'));
    localStorage.setItem(TOKEN_KEY,d.token); currentUser=d.user;
    $('#gate').classList.add('hidden'); fillHeader(); await loadDraft();
  }catch(e){ $('#gErr').textContent=e.message; }
}
boot();
</script>
</body></html>`;
}

// ─── HTML-страница СПИСКА таблиц ─────────────────────────────────────────────
function renderListPage() {
  return `<!doctype html>
<html lang="tg"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отчёты района — 39 таблиц</title>
<style>
  :root { --gold:#d4a017; --green:#1a7a3c; --red:#c0392b; --gray:#94a3b8; --line:#e5e7eb; --bg:#f6f7f9; --ink:#1f2937; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:'Segoe UI',system-ui,Arial,sans-serif; background:var(--bg); color:var(--ink); }
  header { background:#0f2f4f; color:#fff; padding:12px 20px; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  header .badge { background:var(--gold); color:#1a1a1a; border-radius:6px; padding:2px 8px; font-weight:600; }
  header .who { margin-left:auto; font-size:13px; opacity:.9; }
  .lang { display:flex; gap:4px; }
  .lang button { border:1px solid #ffffff55; background:transparent; color:#fff; border-radius:6px; padding:3px 10px; cursor:pointer; font-weight:600; font-size:12px; }
  .lang button.on { background:var(--gold); color:#1a1a1a; border-color:var(--gold); }
  .wrap { padding:18px 20px; }
  .summary { font-size:13px; color:#6b7280; margin-bottom:14px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(320px,1fr)); gap:12px; }
  .cardt { background:#fff; border:1px solid var(--line); border-radius:10px; padding:12px 14px; display:flex; flex-direction:column; gap:8px; }
  .cardt .top { display:flex; align-items:center; gap:8px; }
  .cardt .tid { background:#eef2f7; border-radius:6px; padding:2px 7px; font-weight:600; font-size:12px; white-space:nowrap; }
  .cardt .pill { margin-left:auto; border-radius:20px; padding:2px 10px; font-size:12px; font-weight:600; color:#fff; }
  .pill.none  { background:var(--gray); } .pill.draft { background:var(--gold); color:#1a1a1a; } .pill.ready { background:var(--green); }
  .cardt .nm { font-size:13px; line-height:1.35; min-height:34px; }
  .bar { height:7px; background:#eef2f7; border-radius:5px; overflow:hidden; }
  .bar > i { display:block; height:100%; background:var(--green); width:0; }
  .cardt .row2 { display:flex; align-items:center; gap:10px; font-size:12px; color:#6b7280; }
  .cardt a.open { margin-left:auto; background:var(--green); color:#fff; text-decoration:none; border-radius:7px; padding:5px 12px; font-weight:600; }
  #gate { position:fixed; inset:0; background:rgba(15,47,79,.55); display:flex; align-items:center; justify-content:center; z-index:50; }
  #gate .box { background:#fff; border-radius:12px; padding:22px 24px; width:330px; }
  #gate h2 { margin:0 0 12px; font-size:16px; }
  #gate input { width:100%; padding:9px 10px; margin-bottom:10px; border:1px solid #cbd5e1; border-radius:7px; font-size:14px; }
  #gate .err { color:var(--red); font-size:13px; min-height:18px; }
  .btn { background:var(--green); color:#fff; border:0; border-radius:7px; padding:8px 16px; font-weight:600; cursor:pointer; }
  .hidden { display:none !important; }
</style>
</head><body>
<header>
  <span class="badge">39</span>
  <span style="font-weight:600">${bi('Ҳисоботи ноҳия — ҷадвалҳо', 'Отчёты района — таблицы')}</span>
  <span class="who"><span id="hDistrict"></span></span>
  <span class="lang">
    <button id="langTg" onclick="setLang('tg')">ТҶ</button>
    <button id="langRu" onclick="setLang('ru')">РУС</button>
  </span>
  <a href="#" id="btnLogout" style="color:#fff;font-size:13px">${bi('Баромадан', 'Выйти')}</a>
</header>

<div class="wrap">
  <div class="summary" id="summary"></div>
  <div class="grid" id="grid"></div>
</div>

<div id="gate" class="hidden">
  <div class="box">
    <h2>${bi('Вуруди ноҳия', 'Вход района')}</h2>
    <input id="gLogin" data-tgph="Логини ноҳия (мисол: khujand)" data-ruph="Логин района (напр. khujand)">
    <input id="gPass" type="password" data-tgph="Рамз" data-ruph="Пароль">
    <div class="err" id="gErr"></div>
    <button class="btn" id="gBtn" style="width:100%">${bi('Ворид шудан', 'Войти')}</button>
  </div>
</div>

<script>
const TOKEN_KEY='medt_token', LANG_KEY='medt_form_lang';
let lang=localStorage.getItem(LANG_KEY)||'tg', currentUser=null, tablesData=[];
const ST = { none:{tg:'Оғоз нашуда',ru:'Не начата'}, draft:{tg:'Сиёҳнавис',ru:'Черновик'}, ready:{tg:'Омода',ru:'Готова'} };
function token(){ return localStorage.getItem(TOKEN_KEY); }
function authHeaders(){ return {'Authorization':'Bearer '+token()}; }
function $(s){ return document.querySelector(s); }
function ui(tg,ru){ return lang==='ru'?ru:tg; }

function setLang(l){
  lang=(l==='ru')?'ru':'tg'; localStorage.setItem(LANG_KEY,lang); document.documentElement.lang=lang;
  document.querySelectorAll('[data-tg]').forEach(el=>{const v=lang==='ru'?(el.dataset.ru||el.dataset.tg):(el.dataset.tg||el.dataset.ru); if(v!=null) el.textContent=v;});
  document.querySelectorAll('[data-tgph]').forEach(el=>{el.placeholder=lang==='ru'?(el.dataset.ruph||el.dataset.tgph):(el.dataset.tgph||el.dataset.ruph);});
  $('#langTg').classList.toggle('on',lang==='tg'); $('#langRu').classList.toggle('on',lang==='ru');
  render();
}

function render(){
  if(!tablesData.length) return;
  const done=tablesData.filter(t=>t.status==='ready').length;
  const drafts=tablesData.filter(t=>t.status==='draft').length;
  $('#summary').textContent = ui(
    \`Ҳамагӣ \${tablesData.length} ҷадвал · омода \${done} · сиёҳнавис \${drafts} · оғоз нашуда \${tablesData.length-done-drafts}\`,
    \`Всего \${tablesData.length} таблиц · готово \${done} · черновик \${drafts} · не начато \${tablesData.length-done-drafts}\`);
  $('#grid').innerHTML = tablesData.map(t=>{
    const nm = lang==='ru'?(t.nameRu||t.nameTg):(t.nameTg||t.nameRu);
    const pct = t.total? Math.round(100*t.filled/t.total):0;
    const st = ST[t.status];
    return \`<div class="cardt">
      <div class="top"><span class="tid">\${t.id}</span><span class="pill \${t.status}">\${ui(st.tg,st.ru)}</span></div>
      <div class="nm">\${nm.replace(/</g,'&lt;')}</div>
      <div class="bar"><i style="width:\${pct}%"></i></div>
      <div class="row2"><span>\${t.filled}/\${t.total} · \${pct}%</span>
        <a class="open" href="/form/\${encodeURIComponent(t.id)}">\${ui('Кушодан','Открыть')}</a></div>
    </div>\`;
  }).join('');
}

async function loadTables(){
  const r=await fetch('/tables',{headers:authHeaders()});
  if(!r.ok) return;
  const d=await r.json(); tablesData=d.tables; currentUser=d.user;
  $('#hDistrict').textContent=currentUser.district||currentUser.name;
  render();
}
function showGate(){ $('#gate').classList.remove('hidden'); }
async function doLogin(){
  $('#gErr').textContent='';
  const login=$('#gLogin').value.trim(), password=$('#gPass').value;
  try{
    const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({login,password})});
    const d=await r.json(); if(!r.ok) throw new Error(d.error||ui('Хатои вуруд','Ошибка входа'));
    localStorage.setItem(TOKEN_KEY,d.token); $('#gate').classList.add('hidden'); await loadTables();
  }catch(e){ $('#gErr').textContent=e.message; }
}
async function boot(){
  setLang(lang);
  $('#gBtn').addEventListener('click',doLogin);
  $('#btnLogout').addEventListener('click',e=>{e.preventDefault();localStorage.removeItem(TOKEN_KEY);location.reload();});
  if(!token()) return showGate();
  const me=await fetch('/me',{headers:authHeaders()});
  if(!me.ok) return showGate();
  await loadTables();
}
boot();
</script>
</body></html>`;
}

// ─── HTML-страница ЦЕНТРА (приёмка/мониторинг отчётов районов) ────────────────
function renderCentralPage() {
  return `<!doctype html>
<html lang="tg"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Центральный аппарат — отчёты территорий</title>
<style>
  :root { --gold:#d4a017; --green:#1a7a3c; --red:#c0392b; --gray:#94a3b8; --line:#e5e7eb; --bg:#f6f7f9; --ink:#1f2937; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:'Segoe UI',system-ui,Arial,sans-serif; background:var(--bg); color:var(--ink); }
  header { background:#3a1f4f; color:#fff; padding:12px 20px; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  header .badge { background:var(--gold); color:#1a1a1a; border-radius:6px; padding:2px 8px; font-weight:600; }
  header .who { margin-left:auto; font-size:13px; opacity:.9; }
  .lang { display:flex; gap:4px; }
  .lang button { border:1px solid #ffffff55; background:transparent; color:#fff; border-radius:6px; padding:3px 10px; cursor:pointer; font-weight:600; font-size:12px; }
  .lang button.on { background:var(--gold); color:#1a1a1a; border-color:var(--gold); }
  .wrap { padding:18px 20px; }
  .summary { font-size:13px; color:#6b7280; margin-bottom:14px; }
  .tbl-scroll { overflow-x:auto; border:1px solid var(--line); border-radius:10px; background:#fff; }
  table { border-collapse:collapse; width:100%; font-size:13px; }
  th, td { border:1px solid var(--line); padding:6px 9px; text-align:center; white-space:nowrap; }
  thead th { background:#efe7f4; position:sticky; top:0; }
  td.name { text-align:left; }
  td.reg { text-align:left; color:#6b7280; }
  a.dash-link { color:#3a1f4f; font-weight:600; text-decoration:none; border-bottom:1px dashed #b79ccb; cursor:pointer; }
  a.dash-link:hover { color:#d4a017; border-bottom-color:#d4a017; }
  .tp { display:inline-block; border-radius:5px; padding:1px 6px; font-size:11px; font-weight:600; }
  .tp.city { background:#e0f2fe; color:#075985; } .tp.district { background:#f1f5f9; color:#475569; }
  .tp.region { background:#f3e8ff; color:#6b21a8; }
  tr.reg-row td { background:#f6f1fa; text-align:left; font-weight:700; }
  .bar { height:7px; background:#eef2f7; border-radius:5px; overflow:hidden; min-width:90px; }
  .bar > i { display:block; height:100%; background:var(--green); }
  .cnt { font-weight:600; } .cnt.g { color:var(--green); } .cnt.d { color:#8a6d1a; } .cnt.n { color:var(--gray); }
  #gate { position:fixed; inset:0; background:rgba(58,31,79,.55); display:flex; align-items:center; justify-content:center; z-index:50; }
  #gate .box { background:#fff; border-radius:12px; padding:22px 24px; width:330px; }
  #gate h2 { margin:0 0 12px; font-size:16px; }
  #gate input { width:100%; padding:9px 10px; margin-bottom:10px; border:1px solid #cbd5e1; border-radius:7px; font-size:14px; }
  #gate .err { color:var(--red); font-size:13px; min-height:18px; }
  .btn { background:var(--green); color:#fff; border:0; border-radius:7px; padding:8px 16px; font-weight:600; cursor:pointer; }
  .hidden { display:none !important; }
  .note { font-size:12px; color:#6b7280; margin-top:12px; }
</style>
</head><body>
<header>
  <span class="badge">${bi('Марказ', 'Центр')}</span>
  <span style="font-weight:600">${bi('Ҳисоботи ҳудудҳо', 'Отчёты территорий')}</span>
  <span class="who"><span id="hUser"></span></span>
  <span class="lang">
    <button id="langTg" onclick="setLang('tg')">ТҶ</button>
    <button id="langRu" onclick="setLang('ru')">РУС</button>
  </span>
  <a href="#" id="btnLogout" style="color:#fff;font-size:13px">${bi('Баромадан', 'Выйти')}</a>
</header>

<div class="wrap">
  <div class="summary" id="summary"></div>
  <div class="tbl-scroll">
    <table id="grid"><thead id="thead"></thead><tbody id="tbody"></tbody></table>
  </div>
  <div class="note">${bi(
    'Экран мониторинга: центр видит статус заполнения по каждой территории. Приёмка (принять/вернуть) — следующий шаг.',
    'Экран мониторинга: центр видит статус заполнения по каждой территории. Приёмка (принять / вернуть) — следующий шаг.'
  )}</div>
</div>

<div id="gate" class="hidden">
  <div class="box">
    <h2>${bi('Вуруди марказ/вилоят', 'Вход: центр / область')}</h2>
    <input id="gLogin" data-tgph="Логин (admin ё вилоят)" data-ruph="Логин (admin или область)">
    <input id="gPass" type="password" data-tgph="Рамз" data-ruph="Пароль">
    <div class="err" id="gErr"></div>
    <button class="btn" id="gBtn" style="width:100%">${bi('Ворид шудан', 'Войти')}</button>
  </div>
</div>

<script>
const TOKEN_KEY='medt_token', LANG_KEY='medt_form_lang';
let lang=localStorage.getItem(LANG_KEY)||'tg', data=null;
const ST={none:{tg:'Оғоз нашуда',ru:'Не начата'},draft:{tg:'Сиёҳнавис',ru:'Черновик'},ready:{tg:'Омода',ru:'Готова'}};
function token(){return localStorage.getItem(TOKEN_KEY);}
function authHeaders(){return {'Authorization':'Bearer '+token()};}
function $(s){return document.querySelector(s);}
function ui(tg,ru){return lang==='ru'?ru:tg;}
function esc(s){return String(s).replace(/</g,'&lt;');}

function setLang(l){
  lang=(l==='ru')?'ru':'tg'; localStorage.setItem(LANG_KEY,lang); document.documentElement.lang=lang;
  document.querySelectorAll('[data-tg]').forEach(el=>{const v=lang==='ru'?(el.dataset.ru||el.dataset.tg):(el.dataset.tg||el.dataset.ru); if(v!=null) el.textContent=v;});
  document.querySelectorAll('[data-tgph]').forEach(el=>{el.placeholder=lang==='ru'?(el.dataset.ruph||el.dataset.tgph):(el.dataset.tgph||el.dataset.ruph);});
  $('#langTg').classList.toggle('on',lang==='tg'); $('#langRu').classList.toggle('on',lang==='ru');
  render();
}

function render(){
  if(!data) return;
  const T=data.tablesCount, terr=data.territories;
  const totReady=terr.reduce((a,x)=>a+x.ready,0);
  $('#summary').textContent=ui(
    \`Ҳудудҳо: \${terr.length} · ҷадвалҳо дар ҳар кадом: \${T} · омодашуда (ҳамагӣ): \${totReady}\`,
    \`Территорий: \${terr.length} · таблиц на каждую: \${T} · готово (всего ячеек-таблиц): \${totReady}\`);
  $('#thead').innerHTML='<tr>'+
    '<th style="text-align:left">'+ui('Ҳудуд','Территория')+'</th>'+
    '<th>'+ui('Навъ','Тип')+'</th>'+
    '<th>'+ui('Омода','Готово')+'</th>'+
    '<th>'+ui('Сиёҳнавис','Черновик')+'</th>'+
    '<th>'+ui('Оғоз нашуда','Не начато')+'</th>'+
    '<th>'+ui('Пур шуд, %','Заполнено, %')+'</th>'+
    '<th>'+ui('Навсозии охирин','Обновлено')+'</th></tr>';
  // группировка по области
  const byReg={}; terr.forEach(x=>{(byReg[x.region]=byReg[x.region]||[]).push(x);});
  let html='';
  for(const reg of Object.keys(byReg)){
    html+='<tr class="reg-row"><td colspan="7">'+esc(reg)+'</td></tr>';
    for(const x of byReg[reg]){
      const nm=lang==='ru'?(x.name_ru||x.name_tg):(x.name_tg||x.name_ru);
      const tp=x.type==='city'?ui('Шаҳр','Город'):x.type==='region'?ui('Вилоят','Область'):ui('Ноҳия','Район');
      const upd=x.lastUpd?new Date(x.lastUpd).toLocaleString('ru-RU'):'—';
      const hasData=(x.ready+x.draft)>0;
      const nameCell=hasData
        ? '<a class="dash-link" href="/territory-dashboard?territory='+encodeURIComponent(x.login)+'" title="'+ui('Кушодани дашборд','Открыть дашборд')+'">'+esc(nm)+'</a>'
        : esc(nm);
      html+='<tr><td class="name">'+nameCell+'</td>'+
        '<td><span class="tp '+x.type+'">'+tp+'</span></td>'+
        '<td class="cnt g">'+x.ready+'</td><td class="cnt d">'+x.draft+'</td><td class="cnt n">'+x.none+'</td>'+
        '<td><div style="display:flex;align-items:center;gap:6px"><div class="bar"><i style="width:'+x.pct+'%"></i></div>'+x.pct+'%</div></td>'+
        '<td>'+upd+'</td></tr>';
    }
  }
  $('#tbody').innerHTML=html;
}

async function loadData(){
  const r=await fetch('/central-data',{headers:authHeaders()});
  if(r.status===403){ $('#tbody').innerHTML='<tr><td colspan=7 style="color:#c0392b;padding:16px">'+ui('Танҳо барои admin/вилоят','Доступ только для admin/области')+'</td></tr>'; return; }
  if(!r.ok) return showGate();
  data=await r.json(); $('#hUser').textContent=(data.user.name||'')+' · '+data.user.role;
  render();
}
function showGate(){ $('#gate').classList.remove('hidden'); }
async function doLogin(){
  $('#gErr').textContent='';
  try{
    const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({login:$('#gLogin').value.trim(),password:$('#gPass').value})});
    const d=await r.json(); if(!r.ok) throw new Error(d.error||ui('Хатои вуруд','Ошибка входа'));
    if(!['admin','region'].includes(d.user.role)){ $('#gErr').textContent=ui('Ин экран танҳо барои марказ/вилоят','Этот экран — для центра/области'); return; }
    localStorage.setItem(TOKEN_KEY,d.token); $('#gate').classList.add('hidden'); await loadData();
  }catch(e){ $('#gErr').textContent=e.message; }
}
async function boot(){
  setLang(lang);
  $('#gBtn').addEventListener('click',doLogin);
  $('#btnLogout').addEventListener('click',e=>{e.preventDefault();localStorage.removeItem(TOKEN_KEY);location.reload();});
  if(!token()) return showGate();
  const me=await fetch('/me',{headers:authHeaders()});
  if(!me.ok) return showGate();
  const user=(await me.json()).user;
  if(!['admin','region'].includes(user.role)) return showGate();
  await loadData();
}
boot();
</script>
</body></html>`;
}

// ─── HTML-страница ХАБА ДАШБОРДОВ (admin/область) ────────────────────────────
function renderDashboardsHub() {
  return `<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Дашборды территорий — EcotahlilAI</title>
<style>
  * { box-sizing:border-box; }
  body { margin:0; font-family:'Segoe UI',system-ui,Arial,sans-serif; background:#f4f7fb; color:#14202e; }
  header { background:#15507a; color:#fff; padding:14px 24px; display:flex; align-items:center; gap:14px; }
  header .badge { background:#c8912a; color:#1a1a1a; border-radius:6px; padding:2px 8px; font-weight:600; }
  header a { color:#fff; text-decoration:none; font-size:13px; }
  .wrap { max-width:1120px; margin:0 auto; padding:22px 24px 60px; }
  .reg { font-size:13px; font-weight:700; color:#5e6e83; text-transform:uppercase; letter-spacing:.05em; margin:22px 0 10px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:14px; }
  .card { background:#fff; border:1px solid #e6ecf3; border-radius:14px; padding:16px 17px; text-decoration:none; color:inherit;
          box-shadow:0 1px 2px rgba(20,32,46,.04),0 8px 24px rgba(20,32,46,.05); transition:.12s; display:block; }
  .card:hover { border-color:#15507a; transform:translateY(-2px); }
  .card .nm { font-weight:700; font-size:15px; margin-bottom:4px; }
  .card .tp { font-size:11.5px; color:#5e6e83; }
  .card .bar { height:7px; background:#eef2f7; border-radius:5px; overflow:hidden; margin:12px 0 6px; }
  .card .bar > i { display:block; height:100%; background:#1a7a3c; }
  .card .pct { font-size:12.5px; color:#5e6e83; font-weight:600; }
  .empty { color:#8493a6; padding:30px; text-align:center; }
</style>
</head><body>
<header>
  <span class="badge">Дашборды</span>
  <span style="font-weight:600">Дашборды территорий</span>
  <span style="margin-left:auto"><a href="/central">Приём отчётов →</a></span>
  <span><a href="/" style="margin-left:14px">EcotahlilAI</a></span>
</header>
<div class="wrap"><div id="body"><div class="empty">Загрузка…</div></div></div>
<script>
const token=localStorage.getItem('medt_token');
async function load(){
  if(!token){ document.getElementById('body').innerHTML='<div class="empty">Войдите через <a href="/login">систему</a>.</div>'; return; }
  let r; try{ r=await fetch('/central-data',{headers:{Authorization:'Bearer '+token}}); }catch(e){ return; }
  if(r.status===403){ document.getElementById('body').innerHTML='<div class="empty">Доступ только для admin/области.</div>'; return; }
  if(!r.ok){ document.getElementById('body').innerHTML='<div class="empty">Войдите через <a href="/login">систему</a>.</div>'; return; }
  const d=await r.json();
  const withData=d.territories.filter(x=>(x.ready+x.draft)>0);
  if(!withData.length){ document.getElementById('body').innerHTML='<div class="empty">Пока нет территорий с данными.</div>'; return; }
  const byReg={}; withData.forEach(x=>{(byReg[x.region]=byReg[x.region]||[]).push(x);});
  let html='';
  for(const reg of Object.keys(byReg)){
    html+='<div class="reg">'+esc(reg)+'</div><div class="grid">';
    for(const x of byReg[reg]){
      const tp=x.type==='city'?'Город':x.type==='region'?'Область':'Район';
      html+='<a class="card" href="/territory-dashboard?territory='+encodeURIComponent(x.login)+'">'+
        '<div class="nm">'+esc(x.name_ru||x.name_tg||x.login)+'</div>'+
        '<div class="tp">'+tp+'</div>'+
        '<div class="bar"><i style="width:'+x.pct+'%"></i></div>'+
        '<div class="pct">Заполнено '+x.pct+'% · готово '+x.ready+', черновик '+x.draft+'</div></a>';
    }
    html+='</div>';
  }
  document.getElementById('body').innerHTML=html;
}
function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;');}
load();
</script>
</body></html>`;
}

// ─── HTTP ────────────────────────────────────────────────────────────────────
const DEFAULT_TABLE = 'Ҷадвали 6';

// Пути, которыми владеет модуль форм. Основной server.js по этому списку решает,
// передавать ли запрос в handle() (см. экспорт ниже), не ломая свои маршруты.
const FORM_PATHS = new Set(['/forms', '/central', '/central-data', '/dashboards',
  '/territory-dashboard', '/dashboard-data', '/tables', '/draft', '/me']);
function ownsPath(pathname, method) {
  if (FORM_PATHS.has(pathname)) return true;
  if (pathname.startsWith('/form/')) return true;
  if (pathname === '/login' && method === 'POST') return true;
  return false;
}

// Обработчик всех маршрутов модуля форм (форма территории, центр, дашборд).
async function handle(req, res) {
  try {
    const u = new URL(req.url, 'http://x');

    // список regular-таблиц территории (кабинет ввода)
    if (req.method === 'GET' && u.pathname === '/forms') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(renderListPage());
    }

    // ── Экран центрального аппарата ──────────────────────────────────────────
    if (req.method === 'GET' && u.pathname === '/central') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(renderCentralPage());
    }

    // ── Хаб дашбордов (для admin/области) ────────────────────────────────────
    if (req.method === 'GET' && u.pathname === '/dashboards') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(renderDashboardsHub());
    }

    // ── Дашборд территории (живые данные) ────────────────────────────────────
    if (req.method === 'GET' && u.pathname === '/territory-dashboard') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf8'));
    }

    // данные дашборда по конкретной территории (login) — для любой территории
    if (req.method === 'GET' && u.pathname === '/dashboard-data') {
      let user; try { user = authModule.verifyToken(req); } catch (e) { return json(res, 401, { error: e.message }); }
      const login = u.searchParams.get('territory');
      if (!login) return json(res, 400, { error: 'territory обязателен' });

      // доступ по ролям: admin — любая; область — только свой регион; район — только себя
      const terr = loadTerritories().find(t => t.login === login);
      if (user.role === 'region' && terr && terr.region !== user.region)
        return json(res, 403, { error: 'Доступ ограничен вашей областью' });
      if (user.role === 'district' && login !== user.login)
        return json(res, 403, { error: 'Доступ только к своей территории' });

      return json(res, 200, buildTerritoryData(login));
    }

    // данные для центра: по каждой территории — прогресс по всем regular-таблицам
    if (req.method === 'GET' && u.pathname === '/central-data') {
      let user; try { user = authModule.verifyToken(req); } catch (e) { return json(res, 401, { error: e.message }); }
      if (!['admin', 'region'].includes(user.role))
        return json(res, 403, { error: 'Доступ только для admin/области' });

      const regs = loadSchema().tables.filter(t => t.type === 'regular')
        .map(t => ({ t, cycle: cycleOf(t), total: computeProgress(t, null).total }));
      const byLogin = {};
      for (const d of store.readAll()) (byLogin[d.login] = byLogin[d.login] || []).push(d);

      let terr = loadTerritories();
      if (user.role === 'region') terr = terr.filter(x => x.region === user.region);

      const territories = terr.map(x => {
        const my = byLogin[x.login] || [];
        const byKey = new Map(my.map(d => [d.tableId + '::' + (d.cycle || ''), d]));
        let ready = 0, draft = 0, none = 0, filledTot = 0, totalTot = 0, lastUpd = null;
        for (const { t, cycle, total } of regs) {
          const d = byKey.get(t.id + '::' + cycle);
          const p = computeProgress(t, d);
          if (p.status === 'ready') ready++; else if (p.status === 'draft') draft++; else none++;
          filledTot += p.filled; totalTot += total;
          if (d && d.updatedAt && (!lastUpd || d.updatedAt > lastUpd)) lastUpd = d.updatedAt;
        }
        return { login: x.login, type: x.type, name_ru: x.name_ru, name_tg: x.name_tg,
                 region: x.region, ready, draft, none,
                 pct: totalTot ? Math.round(100 * filledTot / totalTot) : 0, lastUpd };
      });

      return json(res, 200, { territories, tablesCount: regs.length,
                              user: { name: user.name, role: user.role, region: user.region } });
    }

    // список regular-таблиц с прогрессом для текущего района
    if (req.method === 'GET' && u.pathname === '/tables') {
      let user; try { user = authModule.verifyToken(req); } catch (e) { return json(res, 401, { error: e.message }); }
      const drafts = store.getUserDrafts(user);
      const byKey = new Map(drafts.map(d => [d.tableId + '::' + (d.cycle || ''), d]));
      const list = loadSchema().tables.filter(t => t.type === 'regular').map(t => {
        const cycle = cycleOf(t);
        const draft = byKey.get(t.id + '::' + cycle);
        const p = computeProgress(t, draft);
        return { id: t.id, nameTg: t.name_tg || t.name, nameRu: t.name_ru || t.name_tg || t.name,
                 cycle, total: p.total, filled: p.filled, status: p.status };
      });
      return json(res, 200, { tables: list, user: { district: user.district, name: user.name } });
    }

    if (req.method === 'GET' && u.pathname.startsWith('/form/')) {
      const id = decodeURIComponent(u.pathname.slice('/form/'.length));
      const table = getTable(id);
      if (!table) { res.writeHead(404, {'Content-Type':'text/plain; charset=utf-8'}); return res.end('Таблица не найдена: ' + id); }
      if (table.type !== 'regular') { res.writeHead(400, {'Content-Type':'text/plain; charset=utf-8'}); return res.end('Пока поддерживаются только regular-таблицы. Тип: ' + table.type); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(renderPage(table));
    }

    if (req.method === 'POST' && u.pathname === '/login') {
      const { login, password } = JSON.parse((await readBody(req)).toString('utf8') || '{}');
      if (!login || !password) return json(res, 400, { error: 'Введите логин и пароль' });
      try {
        // Вход разрешён всем ролям: районы → кабинет форм, admin/region → центр.
        // Доступ к экранам ограничивается на самих маршрутах (/tables, /central).
        const result = authModule.login(String(login).trim(), password);
        return json(res, 200, { ok: true, token: result.token, user: result.user });
      } catch (e) { return json(res, 401, { error: e.message }); }
    }

    if (req.method === 'GET' && u.pathname === '/me') {
      try { return json(res, 200, { user: authModule.verifyToken(req) }); }
      catch (e) { return json(res, 401, { error: e.message }); }
    }

    if (req.method === 'GET' && u.pathname === '/draft') {
      let user; try { user = authModule.verifyToken(req); } catch (e) { return json(res, 401, { error: e.message }); }
      const draft = store.getDraft(user, u.searchParams.get('tableId'), u.searchParams.get('cycle'));
      return json(res, 200, { draft });
    }

    if (req.method === 'POST' && u.pathname === '/draft') {
      let user; try { user = authModule.verifyToken(req); } catch (e) { return json(res, 401, { error: e.message }); }
      if (user.role !== 'district') return json(res, 403, { error: 'Только район может сохранять отчёт' });
      const payload = JSON.parse((await readBody(req)).toString('utf8') || '{}');
      if (!payload.tableId) return json(res, 400, { error: 'tableId обязателен' });
      const rec = store.saveDraft(user, payload, new Date().toISOString());
      return json(res, 200, { ok: true, key: rec.key, updatedAt: rec.updatedAt });
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  } catch (err) {
    console.error('[formServer]', err);
    json(res, 500, { error: err.message });
  }
}

// Экспорт для монтирования в основной server.js (единый порт, общая авторизация)
module.exports = { handle, ownsPath, FORM_PATHS };

// Автономный запуск (dev): node schemas/formServer.js — на своём порту 3100
if (require.main === module) {
  const server = http.createServer(async (req, res) => {
    const u = new URL(req.url, 'http://x');
    if (req.method === 'GET' && u.pathname === '/') { res.writeHead(302, { Location: '/forms' }); return res.end(); }
    if (req.method === 'GET' && u.pathname === '/health') return json(res, 200, { ok: true });
    if (ownsPath(u.pathname, req.method)) return handle(req, res);
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  });
  server.listen(PORT, () => {
    console.log(`[formServer] http://localhost:${PORT}/forms  (по умолчанию: ${DEFAULT_TABLE})`);
    console.log(`[formServer] черновики → ${path.relative(process.cwd(), store.STORE_FILE)}`);
  });
}
