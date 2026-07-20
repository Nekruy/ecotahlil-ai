'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/translateDictionary.js — Этап 2 двуязычия: ЧЕРНОВОЙ перевод
//
//  Читает schemas/dictionary_tg_ru.json (тадж → ""), переводит термины на
//  русский через Groq (llama-3.3-70b-versatile) и пишет ОТДЕЛЬНЫЙ файл
//  schemas/dictionary_tg_ru_draft.json (оригинал не перезаписывается).
//
//  Это ЧЕРНОВИК для последующей вычитки человеком.
//  Запуск:  GROQ_API_KEY=... node schemas/translateDictionary.js
//  Резюме:  повторный запуск переводит только ещё не заполненные термины.
// ═══════════════════════════════════════════════════════════════════════════

const fs    = require('fs');
const path  = require('path');
const https = require('https');

const SRC   = path.join(__dirname, 'dictionary_tg_ru.json');
const DRAFT = path.join(__dirname, 'dictionary_tg_ru_draft.json');

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL   = 'llama-3.3-70b-versatile';
const BATCH        = 25;
const PAUSE_MS     = 8500;   // держим ~11k токенов/мин под лимитом free-tier (12k TPM)
const MAX_RETRY    = 5;

// ─── Отраслевые соответствия (точный, не буквальный перевод) ─────────────────
const SYSTEM_PROMPT = [
  'Ты профессиональный переводчик экономической и отраслевой документации с ТАДЖИКСКОГО на РУССКИЙ.',
  'Переводишь термины из государственных прогнозных таблиц Министерства экономического развития Таджикистана',
  '(макроэкономика, энергетика, промышленность, сельское хозяйство, торговля, налоги, инвестиции).',
  'Переводи ТОЧНО по смыслу и по принятой русской терминологии, НЕ буквально. Соблюдай соответствия:',
  '• НБО (Нерӯгоҳи барқи обӣ) = ГЭС (гидроэлектростанция). Напр. «НБО-и Норак» → «Нурекская ГЭС», «НБО-и Роғун» → «Рогунская ГЭС», «Силсилаи НБО-и Вахш» → «Вахшский каскад ГЭС».',
  '• МБГ (Маркази барқу гармидиҳӣ) = ТЭЦ. Напр. «МБГ-1 ш. Душанбе» → «ТЭЦ-1 г. Душанбе».',
  '• НБО-и хурд = малые ГЭС. «млн. кВт/с» → «млн кВт·ч». «ҳаз. Гкал» → «тыс. Гкал».',
  '• ҶСК → ОАО, ҶДММ → ООО, ҶСП → предприятие/СП (по смыслу). Названия компаний в «...» сохраняй.',
  '• млн. сомонӣ → млн сомони; ҳазор нафар → тыс. человек; ҳазор доллар → тыс. долларов; ҳазор тонна → тыс. тонн; адад → единиц.',
  '• растанипарварӣ → растениеводство; чорводорӣ → животноводство; моҳидорӣ → рыбоводство; ғалладона → зерновые; гандум → пшеница; пахта → хлопок; сабзавот → овощи; картошка → картофель.',
  '• саноати истихроҷи маъдан → добывающая промышленность; саноати коркард → обрабатывающая промышленность.',
  '• «аз он ҷумла» → «в том числе»; «ҳамагӣ» → «всего»; «аз он дар соҳаи ...» → «из них в сфере ...».',
  '• содирот → экспорт; воридот → импорт; гардиши савдо → товарооборот; хизматрасонӣ → услуги; маблағгузорӣ ба сармояи асосӣ → инвестиции в основной капитал.',
  'Сохраняй числа, единицы и знаки как есть. Не добавляй пояснений.',
].join('\n');

function groqChat(userContent, maxTokens = 2500) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: GROQ_MODEL,
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user',   content: userContent  },
      ],
      max_tokens: maxTokens,
      temperature: 0.2,
      response_format: { type: 'json_object' },
    });
    const req = https.request({
      hostname: 'api.groq.com', path: '/openai/v1/chat/completions', method: 'POST',
      headers: { 'Authorization': 'Bearer ' + GROQ_API_KEY, 'Content-Type': 'application/json',
                 'Content-Length': Buffer.byteLength(body) },
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          if (parsed.error) return reject(new Error(parsed.error.message || 'Groq API error'));
          resolve(parsed.choices?.[0]?.message?.content || '');
        } catch { reject(new Error('Groq: invalid JSON response')); }
      });
    });
    req.on('error', reject);
    req.setTimeout(90000, () => { req.destroy(); reject(new Error('Groq timeout')); });
    req.write(body); req.end();
  });
}

function extractJson(text) {
  let s = String(text).trim().replace(/^```(?:json)?/i, '').replace(/```$/, '').trim();
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  if (a !== -1 && b !== -1) s = s.slice(a, b + 1);
  return JSON.parse(s);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function translateBatch(terms) {
  const list = terms.map((t, i) => `${i + 1}. ${t}`).join('\n');
  const userContent =
    'Переведи на русский каждый термин. Верни СТРОГО JSON-объект вида {"1":"перевод", "2":"перевод", ...} ' +
    'с теми же номерами. Термины:\n' + list;

  for (let attempt = 0; attempt <= MAX_RETRY; attempt++) {
    try {
      const raw = await groqChat(userContent);
      const obj = extractJson(raw);
      return terms.map((_, i) => (obj[String(i + 1)] || '').trim());
    } catch (e) {
      const m = /try again in ([\d.]+)s/i.exec(e.message);
      if ((/rate limit/i.test(e.message)) && attempt < MAX_RETRY) {
        const waitMs = Math.ceil((m ? parseFloat(m[1]) : 8) * 1000) + 800;
        console.log(`[translate]   лимит, жду ${(waitMs/1000).toFixed(1)}с и повторяю…`);
        await sleep(waitMs);
        continue;
      }
      throw e;
    }
  }
}

async function main() {
  if (!GROQ_API_KEY) { console.error('GROQ_API_KEY не задан в окружении.'); process.exit(1); }

  const src = JSON.parse(fs.readFileSync(SRC, 'utf8'));
  // резюме: берём уже готовый draft, если есть
  const draft = fs.existsSync(DRAFT) ? JSON.parse(fs.readFileSync(DRAFT, 'utf8')) : {};
  for (const k of Object.keys(src)) if (!(k in draft)) draft[k] = src[k] || '';

  const pending = Object.keys(draft).filter(k => !draft[k]);
  console.log(`[translate] Всего терминов: ${Object.keys(draft).length}; к переводу: ${pending.length}`);
  if (!pending.length) { console.log('[translate] Всё уже переведено.'); return; }

  let done = 0;
  for (let i = 0; i < pending.length; i += BATCH) {
    const batch = pending.slice(i, i + BATCH);
    try {
      const ru = await translateBatch(batch);
      batch.forEach((t, k) => { if (ru[k]) draft[t] = ru[k]; });
      done += batch.length;
      fs.writeFileSync(DRAFT, JSON.stringify(draft, null, 2), 'utf8'); // прогресс после каждого батча
      console.log(`[translate] ${Math.min(i + BATCH, pending.length)}/${pending.length} готово`);
    } catch (e) {
      console.error(`[translate] батч ${i}-${i + BATCH} ошибка: ${e.message} (пропущен, повторить перезапуском)`);
    }
    if (i + BATCH < pending.length) await sleep(PAUSE_MS);
  }

  const filled = Object.values(draft).filter(Boolean).length;
  console.log(`[translate] Готово. Заполнено ${filled}/${Object.keys(draft).length}. Файл → ${path.relative(process.cwd(), DRAFT)}`);
}

if (require.main === module) main();
module.exports = { main };
