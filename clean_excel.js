/**
 * Excel cleaner for two macro model files.
 *
 * Handles 4 structural patterns found in both files:
 *  A) Summary/forecast sheets  — paired year columns (value + %YoY)
 *  B) Historical model sheets  — Var.Name | Description | 1997 | 1998 | ...
 *  C) CGE detail sheets        — multi-block with repeated year headers
 *  D) Results sheet            — plain year cols, sub-rows for growth/deflator
 *
 * Output: data/cleaned/<label>__<sheet>.csv
 *         data/cleaned/combined_wide.csv  (all summary sheets merged)
 */

const XLSX = require("xlsx");
const fs   = require("fs");
const path = require("path");

// ── paths ────────────────────────────────────────────────────────────────────
const DOWNLOADS = path.join(process.env.USERPROFILE || process.env.HOME, "Downloads");
const FILE_GDP  = path.join(DOWNLOADS, "S-3-до 2035 года-Model GDP -27.04.2026.xlsx");
const FILE_CGE  = path.join(DOWNLOADS, "S-3 посл.вар.-CGE_Model results_2025 — 2032-27.04.2026.xlsx");
const OUT_DIR   = path.join(__dirname, "data", "cleaned");
fs.mkdirSync(OUT_DIR, { recursive: true });

// ── helpers ──────────────────────────────────────────────────────────────────

function isYear(v) {
  if (v == null) return false;
  const n = Number(v);
  return Number.isInteger(n) && n >= 1990 && n <= 2050;
}

function extractYear(v) {
  if (v == null) return null;
  const m = String(v).match(/(19|20)\d{2}/);
  return m ? parseInt(m[0]) : null;
}

function cleanLabel(v) {
  if (v == null) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

/** Forward-fill years across a row array (handles merged-cell pattern). */
function forwardFillYears(row) {
  const result = [];
  let last = null;
  for (const v of row) {
    const y = extractYear(v);
    if (y) last = y;
    result.push(last);
  }
  return result;
}

/** Read all sheets of a workbook as arrays-of-arrays (raw values). */
function readWorkbook(filePath) {
  const wb = XLSX.readFile(filePath, { cellDates: false, raw: true });
  const sheets = {};
  for (const name of wb.SheetNames) {
    sheets[name] = XLSX.utils.sheet_to_json(wb.Sheets[name], {
      header: 1,
      defval: null,
      blankrows: true,
    });
  }
  return { names: wb.SheetNames, sheets };
}

function escapeCSV(v) {
  if (v == null) return "";
  const s = String(v);
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCSV(filePath, rows) {
  if (!rows.length) return;
  const keys = Object.keys(rows[0]);
  const lines = [keys.join(",")];
  for (const row of rows) {
    lines.push(keys.map(k => escapeCSV(row[k])).join(","));
  }
  fs.writeFileSync(filePath, "﻿" + lines.join("\n"), "utf8"); // BOM for Excel
}

// ── PATTERN A: summary sheets ────────────────────────────────────────────────
// Row headerRow   → year labels ("Соли 2025" | null | "Соли 2026" | ...)
// Row headerRow+1 → sub-labels ("Воқеӣ" | "нисбат ба соли X (бо фоиз)")
// Row dataStart+  → data

function parseSummarySheet(rows, { labelCol = 0, unitCol = 1, headerRow = 1, dataStart = 3 } = {}) {
  const yearRow = rows[headerRow] || [];
  const subRow  = rows[headerRow + 1] || [];
  const years   = forwardFillYears(yearRow);

  // Map year → { val: colIdx, pct: colIdx }
  const yearColMap = {};
  for (let i = 0; i < years.length; i++) {
    const y = years[i];
    if (!y || i <= Math.max(labelCol, unitCol ?? -1)) continue;
    const sub = cleanLabel(subRow[i]);
    const isPct = /фоиз|%|нисбат/i.test(sub);
    if (!yearColMap[y]) yearColMap[y] = {};
    if (isPct && yearColMap[y].pct == null) yearColMap[y].pct = i;
    if (!isPct && yearColMap[y].val == null) yearColMap[y].val = i;
  }

  const records = [];
  for (let r = dataStart; r < rows.length; r++) {
    const row   = rows[r] || [];
    const label = cleanLabel(row[labelCol]);
    if (!label) continue;
    const unit  = unitCol != null ? cleanLabel(row[unitCol]) : "";
    for (const [year, cols] of Object.entries(yearColMap)) {
      const val = cols.val != null ? row[cols.val] : null;
      const pct = cols.pct != null ? row[cols.pct] : null;
      if (val == null && pct == null) continue;
      records.push({ year: Number(year), indicator: label, unit, value: val ?? "", yoy_pct: pct ?? "" });
    }
  }
  return records;
}

// ── PATTERN B: historical model sheets ──────────────────────────────────────
// First row with ≥3 year values is the header.
// Non-year cols before the year block = var_name, description.

function parseModelSheet(rows, startCol = 0) {
  let headerRowIdx = -1;
  for (let i = 0; i < rows.length; i++) {
    const yearCount = (rows[i] || []).filter(isYear).length;
    if (yearCount >= 3) { headerRowIdx = i; break; }
  }
  if (headerRowIdx < 0) return [];

  const header    = rows[headerRowIdx];
  const yearCols  = {};
  const labelCols = [];
  for (let i = startCol; i < header.length; i++) {
    if (isYear(header[i])) yearCols[i] = parseInt(header[i]);
    else if (Object.keys(yearCols).length === 0) labelCols.push(i);
  }

  const varCol  = labelCols[0] ?? null;
  const descCol = labelCols[1] ?? null;

  const records = [];
  for (let r = headerRowIdx + 1; r < rows.length; r++) {
    const row  = rows[r] || [];
    const vn   = varCol  != null ? cleanLabel(row[varCol])  : "";
    const desc = descCol != null ? cleanLabel(row[descCol]) : "";
    if (!vn && !desc) continue;
    for (const [ci, year] of Object.entries(yearCols)) {
      const v = row[ci];
      if (v == null) continue;
      records.push({ year, var_name: vn, description: desc, value: v });
    }
  }
  return records;
}

// ── PATTERN C: CGE detail sheets ────────────────────────────────────────────
// Multiple sub-blocks; detect year-header rows (≥4 years in a row).

function parseCGEBlockSheet(rows) {
  function isYearHeaderRow(row) {
    return (row || []).filter(isYear).length >= 4;
  }

  const records = [];
  let currentBlock = "";
  let yearMap = {};

  for (const row of rows) {
    if (!row) continue;

    if (isYearHeaderRow(row)) {
      yearMap = {};
      for (let i = 0; i < row.length; i++) {
        if (isYear(row[i])) yearMap[i] = parseInt(row[i]);
      }
      continue;
    }

    const first  = cleanLabel(row[0]);
    const second = cleanLabel(row[1]);

    if (!Object.keys(yearMap).length) {
      if (first) currentBlock = first;
      continue;
    }

    if (/^source/i.test(first)) continue;

    const equation   = first || second;
    const description = first ? second : "";
    if (!equation) continue;

    const hasData = Object.keys(yearMap).some(ci => {
      const v = row[ci];
      return v != null && typeof v === "number";
    });
    if (!hasData) {
      currentBlock = equation;
      continue;
    }

    for (const [ci, year] of Object.entries(yearMap)) {
      const v = row[ci];
      if (v == null) continue;
      records.push({ year, block: currentBlock, equation, description, value: v });
    }
  }
  return records;
}

// ── PATTERN D: Results sheet ─────────────────────────────────────────────────
// Row 1 = year row (plain ints). Sub-rows labelled "Темп роста" / "Дефлятор".

function parseResultsSheet(rows) {
  const header   = rows[1] || [];
  const yearCols = {};
  for (let i = 0; i < header.length; i++) {
    if (isYear(header[i])) yearCols[i] = parseInt(header[i]);
  }
  if (!Object.keys(yearCols).length) return [];

  const records = [];
  let currentIndicator = "";
  for (let r = 2; r < rows.length; r++) {
    const row   = rows[r] || [];
    const label = cleanLabel(row[0]);
    if (!label) continue;
    const isSub = /темп|дефлятор|рост/i.test(label);
    if (!isSub) currentIndicator = label;
    for (const [ci, year] of Object.entries(yearCols)) {
      const v = row[ci];
      if (v == null) continue;
      records.push({
        year, indicator: currentIndicator,
        sub_row: isSub ? label : "value",
        value: v,
      });
    }
  }
  return records;
}

// ── process one file ─────────────────────────────────────────────────────────

function processFile(filePath, label) {
  console.log("\n" + "=".repeat(60));
  console.log("Processing:", path.basename(filePath));
  console.log("=".repeat(60));

  if (!fs.existsSync(filePath)) {
    console.log("[WARN] File not found:", filePath);
    return;
  }

  const { names, sheets } = readWorkbook(filePath);
  console.log("Sheets:", names.join(", "));

  const safeName = s => s.replace(/[^\wЀ-ӿ]/g, "_");

  // Pattern A: summary sheets
  const summaryDefs = {
    "S-1 реалистичный сценарий": { labelCol:0, unitCol:1, headerRow:1, dataStart:3 },
    "Итог-Модел CGE":            { labelCol:0, unitCol:1, headerRow:1, dataStart:3 },
    "посл.вар.":                 { labelCol:0, unitCol:1, headerRow:1, dataStart:3 },
    "2025-2029":                 { labelCol:0, unitCol:null, headerRow:1, dataStart:3 },
    "Лист1":                     { labelCol:0, unitCol:null, headerRow:1, dataStart:3 },
    "Лист3":                     { labelCol:0, unitCol:null, headerRow:1, dataStart:3 },
  };
  for (const [sheet, opts] of Object.entries(summaryDefs)) {
    if (!names.includes(sheet)) continue;
    try {
      const records = parseSummarySheet(sheets[sheet], opts);
      if (!records.length) { console.log(`  [SKIP] ${sheet} — empty`); continue; }
      const out = path.join(OUT_DIR, `${label}__${safeName(sheet)}.csv`);
      writeCSV(out, records);
      console.log(`  [OK] ${sheet} → ${path.basename(out)}  (${records.length} rows)`);
    } catch (e) { console.log(`  [ERR] ${sheet}:`, e.message); }
  }

  // Pattern B: historical model sheets
  const modelSheets = ["GDP","Agriculture","Industry","Industry-2","OEA",
                       "Household","Trade","Monetary","BoP","Revenue Model","gateway"];
  for (const sheet of modelSheets) {
    if (!names.includes(sheet)) continue;
    try {
      const records = parseModelSheet(sheets[sheet]);
      if (!records.length) { console.log(`  [SKIP] ${sheet} — empty`); continue; }
      const out = path.join(OUT_DIR, `${label}__${safeName(sheet)}.csv`);
      writeCSV(out, records);
      console.log(`  [OK] ${sheet} → ${path.basename(out)}  (${records.length} rows)`);
    } catch (e) { console.log(`  [ERR] ${sheet}:`, e.message); }
  }

  // Pattern D: Results
  if (names.includes("Results")) {
    try {
      const records = parseResultsSheet(sheets["Results"]);
      if (records.length) {
        const out = path.join(OUT_DIR, `${label}__Results.csv`);
        writeCSV(out, records);
        console.log(`  [OK] Results → ${path.basename(out)}  (${records.length} rows)`);
      }
    } catch (e) { console.log(`  [ERR] Results:`, e.message); }
  }

  // Pattern C: CGE detail sheets
  const cgeSheets = ["1_Prices","2_Production_1","2_Production_2",
                     "3_In & SA","4_Demand","5_IntTrade",
                     "6_Closures","7_Dynamics","GDP-total"];
  for (const sheet of cgeSheets) {
    if (!names.includes(sheet)) continue;
    try {
      const records = parseCGEBlockSheet(sheets[sheet]);
      if (!records.length) { console.log(`  [SKIP] ${sheet} — empty`); continue; }
      const out = path.join(OUT_DIR, `${label}__${safeName(sheet)}.csv`);
      writeCSV(out, records);
      console.log(`  [OK] ${sheet} → ${path.basename(out)}  (${records.length} rows)`);
    } catch (e) { console.log(`  [ERR] ${sheet}:`, e.message); }
  }
}

// ── combine all summary CSVs into one wide table ─────────────────────────────

function buildCombined() {
  const files = fs.readdirSync(OUT_DIR).filter(f => f.endsWith(".csv") && f !== "combined_wide.csv");
  const byYearIndicator = {};   // "year|indicator" → value

  for (const f of files) {
    const content = fs.readFileSync(path.join(OUT_DIR, f), "utf8").replace(/^﻿/, "");
    const lines   = content.split("\n").filter(Boolean);
    const header  = lines[0].split(",");
    const iYear   = header.indexOf("year");
    const iInd    = header.indexOf("indicator");
    const iVal    = header.indexOf("value");
    if (iYear < 0 || iInd < 0 || iVal < 0) continue;

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",");
      const year = cols[iYear];
      const ind  = cols[iInd];
      const val  = cols[iVal];
      if (!year || !ind || !val) continue;
      const key = `${year}|${ind}`;
      if (!byYearIndicator[key]) byYearIndicator[key] = { year: Number(year), indicator: ind };
      if (!byYearIndicator[key].value) byYearIndicator[key].value = val;
    }
  }

  // Collect all unique indicators
  const indicators = [...new Set(Object.values(byYearIndicator).map(r => r.indicator))].sort();
  const years      = [...new Set(Object.values(byYearIndicator).map(r => r.year))].sort((a,b)=>a-b);

  const wideRows = [];
  for (const year of years) {
    const row = { year };
    for (const ind of indicators) {
      const key = `${year}|${ind}`;
      row[ind] = byYearIndicator[key]?.value ?? "";
    }
    wideRows.push(row);
  }

  if (!wideRows.length) { console.log("\nNo summary data for combined table."); return; }
  const out = path.join(OUT_DIR, "combined_wide.csv");
  writeCSV(out, wideRows);
  console.log(`\n[COMBINED] ${out}`);
  console.log(`  ${wideRows.length} years × ${indicators.length} indicators`);
}

// ── run ───────────────────────────────────────────────────────────────────────
processFile(FILE_GDP, "gdp_model");
processFile(FILE_CGE, "cge_model");
buildCombined();
console.log("\nDone. CSVs in:", OUT_DIR);
