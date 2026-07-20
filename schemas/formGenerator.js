'use strict';

// ═══════════════════════════════════════════════════════════════════════════
//  schemas/formGenerator.js — генератор ДВУЯЗЫЧНОЙ HTML-формы ИЗ схемы
//
//  Универсальный (не хардкод): принимает объект схемы одной regular-таблицы
//  (после injectTranslations — с полями name_tg/name_ru, unit_tg/unit_ru) и
//  строит форму, где КАЖДАЯ подпись несёт оба языка через data-tg/data-ru.
//  Переключение РУС/ТҶ делает клиент (formServer), генератор язык не выбирает.
//
//    • слева — показатели с иерархией (read-only, отступы для подпунктов);
//    • справа — поля ввода по колонкам (год × блок);
//    • колонки «темп роста %» — вычисляемые (output), район их не вводит.
// ═══════════════════════════════════════════════════════════════════════════

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Двуязычный <span>: по умолчанию показываем таджикский (язык формы по умолчанию — ТҶ)
function biSpan(tg, ru, cls) {
  const t = tg || ru || '';
  const r = ru || tg || '';
  return `<span class="${cls || ''}" data-tg="${esc(t)}" data-ru="${esc(r)}">${esc(t)}</span>`;
}

// Подпись меры подколонки на двух языках
const MEASURE = {
  '%':                          { ru: 'Темп роста, %', tg: 'Суръати афзоиш, %' },
  'объём':                      { ru: 'Объём',         tg: 'Ҳаҷм' },
  'объём (сопоставимые цены)':  { ru: 'Сопост. цены',  tg: 'Нархҳои муқоисавӣ' },
};
const measureLabel = m => MEASURE[m] || { ru: '—', tg: '—' };

// Русский блок (из схемы) → таджикский эквивалент
const BLOCK_TG = {
  'Отчёт':               'Ҳисобот',
  'Оценка':              'Баҳодиҳӣ',
  'Прогноз':             'Дурнамо',
  'Прогноз (параметры)': 'Параметрҳои дурнамо',
  'База (на начало)':    'Шумораи умумӣ',
  'н/д':                 '—',
};

// Строка целиком — единица измерения (парная строка-значение «млн. сомонӣ»)
function isUnitOnly(name) {
  const s = String(name).trim();
  if (!s || /:$/.test(s)) return false;
  return /^[\d.,\s№-]*((млн|млрд|ҳазор|хазор|ҳаз)\.?\s*)?(сомонӣ|доллар|доллари?|квт.*|кВт.*|гкал|тонна|м3|м³|нафар|адад|дона|га|литр|л|фоиз|%)\.?$/i.test(s);
}

function groupColumns(columns) {
  const groups = [];
  for (const c of columns) {
    let g = groups.find(x => x.year === c.year && x.block === c.block);
    if (!g) { g = { year: c.year, block: c.block, subs: [] }; groups.push(g); }
    g.subs.push(c);
  }
  return groups;
}

function cycleOf(table) {
  const m = String(table.title || table.name || '').match(/(20\d{2})\s*[-–]\s*(20\d{2})/);
  return m ? `${m[1]}-${m[2]}` : '';
}

// ─── Основной рендер ─────────────────────────────────────────────────────────
function renderFormHtml(table) {
  const groups = groupColumns(table.columns);
  const nSub = table.columns.length;

  // thead: строка 1 — «Показатель» + блок·год; строка 2 — меры
  let head1 = `<th class="ind-h" rowspan="2">${biSpan('Нишондиҳанда', 'Показатель')}</th>`;
  let head2 = '';
  for (const g of groups) {
    const blockRu = g.block, blockTg = BLOCK_TG[g.block] || g.block;
    head1 += `<th class="grp-h" colspan="${g.subs.length}">${biSpan(`${blockTg} · ${g.year}`, `${blockRu} · ${g.year}`)}</th>`;
    for (const c of g.subs) {
      const isPct = c.measure === '%';
      const ml = measureLabel(c.measure);
      head2 += `<th class="sub-h ${isPct ? 'pct-h' : 'vol-h'}">${biSpan(ml.tg, ml.ru)}</th>`;
    }
  }
  const theadHtml = `<tr>${head1}</tr><tr>${head2}</tr>`;

  // tbody
  let tbodyHtml = '';
  table.indicators.forEach((ind, idx) => {
    const tg = ind.name_tg || ind.name || '';
    const ru = ind.name_ru || tg;
    if (ind.isGroup) {
      tbodyHtml += `<tr class="grp-row"><td class="ind grp-cell" colspan="${nSub + 1}">${biSpan(tg, ru, 'lbl')}</td></tr>`;
      return;
    }
    const unitLine = isUnitOnly(tg);
    const indent = (ind.level || 0) * 18 + (unitLine ? 18 : 0) + 8;
    const rowKey = 'r' + idx;

    let cells = `<td class="ind${unitLine ? ' ind-unit' : ''}" style="padding-left:${indent}px">` +
                `${unitLine ? '↳ ' : ''}${biSpan(tg, ru, 'lbl')}</td>`;
    for (const g of groups) {
      for (const c of g.subs) {
        if (c.measure === '%') {
          cells += `<td class="pct"><output class="pctout" data-row="${rowKey}" data-year="${esc(c.year)}">—</output></td>`;
        } else {
          cells += `<td><input class="numin" type="text" inputmode="decimal" autocomplete="off" ` +
                   `data-row="${rowKey}" data-year="${esc(c.year)}" data-measure="vol" data-col="${c.col}"></td>`;
        }
      }
    }
    tbodyHtml += `<tr>${cells}</tr>`;
  });

  return {
    tableId:   table.id,
    nameTg:    table.name_tg || table.name || '',
    nameRu:    table.name_ru || table.name_tg || table.name || '',
    cycle:     cycleOf(table),
    years:     table.years,
    theadHtml,
    tbodyHtml,
    colCount:  nSub,
  };
}

module.exports = { renderFormHtml, cycleOf, isUnitOnly, measureLabel, BLOCK_TG };
