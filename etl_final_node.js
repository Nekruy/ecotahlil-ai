/**
 * etl_final_node.js — Master dataset builder (Node.js port of etl_final.py)
 *
 * Input:  data/cleaned/*.csv
 * Output: data/final/master_dataset.csv
 *         data/final/features_dataset.csv
 *         data/final/etl_log.txt
 */

const fs   = require("fs");
const path = require("path");

// ── paths ────────────────────────────────────────────────────────────────────
const ROOT         = __dirname;
const CLEANED_DIR  = path.join(ROOT, "data", "cleaned");
const FINAL_DIR    = path.join(ROOT, "data", "final");
const MASTER_CSV   = path.join(FINAL_DIR, "master_dataset.csv");
const FEATURES_CSV = path.join(FINAL_DIR, "features_dataset.csv");
const LOG_FILE     = path.join(FINAL_DIR, "etl_log.txt");

fs.mkdirSync(FINAL_DIR, { recursive: true });

// ── constants ─────────────────────────────────────────────────────────────────
const MISSING_THRESHOLD = 0.40;
const ROLLING_WINDOW    = 3;

// ── logger ───────────────────────────────────────────────────────────────────
const logLines = [];
function log(level, msg) {
  const ts   = new Date().toTimeString().slice(0, 8);
  const line = `${ts}  ${level.padEnd(8)}  ${msg}`;
  console.log(line);
  logLines.push(line);
}
function flushLog() {
  fs.writeFileSync(LOG_FILE, logLines.join("\n") + "\n", "utf8");
}

// ── CSV parser (no external deps) ────────────────────────────────────────────
function parseCSV(filePath) {
  const raw = fs.readFileSync(filePath, "utf8").replace(/^﻿/, "");
  const lines = raw.split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return { headers: [], rows: [] };

  function splitLine(line) {
    const cols = [];
    let cur = "", inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuote && line[i + 1] === '"') { cur += '"'; i++; }
        else inQuote = !inQuote;
      } else if (ch === ',' && !inQuote) {
        cols.push(cur); cur = "";
      } else {
        cur += ch;
      }
    }
    cols.push(cur);
    return cols;
  }

  const headers = splitLine(lines[0]);
  const rows = lines.slice(1).map(l => {
    const vals = splitLine(l);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = vals[i] ?? ""; });
    return obj;
  });
  return { headers, rows };
}

function writeCSV(filePath, data) {
  if (!data.length) { log("WARN", `No data to write: ${path.basename(filePath)}`); return; }
  const keys = Object.keys(data[0]);

  function escapeCSV(v) {
    if (v == null || v === undefined) return "";
    const s = String(v);
    if (s.includes(",") || s.includes('"') || s.includes("\n"))
      return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }

  const lines = [keys.join(",")];
  for (const row of data) lines.push(keys.map(k => escapeCSV(row[k])).join(","));
  fs.writeFileSync(filePath, "﻿" + lines.join("\n"), "utf8");
}

// ── name normalisation ────────────────────────────────────────────────────────
function normalizeName(raw) {
  let s = String(raw ?? "").trim();
  s = s.replace(/﻿/g, "").replace(/​/g, "");
  s = s.replace(/^[\-–—,\s]+|[\-–—,\s]+$/g, "");
  s = s.replace(/\s+/g, " ");
  s = s.replace(/[\s\-–—]+/g, "_");
  // Keep Cyrillic (U+0400–U+04FF), Latin, digits, underscore
  s = s.replace(/[^\wЀ-ӿ]/g, "");
  s = s.replace(/_+/g, "_").replace(/^_+|_+$/g, "");
  return s.toLowerCase();
}

function deduplicateCols(cols) {
  const seen = {};
  return cols.map(c => {
    if (!(c in seen)) { seen[c] = 0; return c; }
    seen[c]++;
    return `${c}_${seen[c] + 1}`;
  });
}

function toFloat(v) {
  if (v == null || v === "") return NaN;
  const n = parseFloat(String(v).replace(",", "."));
  return isNaN(n) ? NaN : n;
}

// ── wide DataFrame abstraction ────────────────────────────────────────────────
// Represented as: { years: number[], cols: string[], data: Map<year, Map<col, number>> }

class WideDF {
  constructor() {
    this.data = new Map();   // year → Map<col, number>
    this.colSet = new Set();
  }

  set(year, col, val) {
    if (!this.data.has(year)) this.data.set(year, new Map());
    const row = this.data.get(year);
    if (!row.has(col)) {
      row.set(col, []);
    }
    row.get(col).push(val);
    this.colSet.add(col);
  }

  /** Resolve accumulated arrays → mean. Returns plain {year→{col→value}} map. */
  resolve() {
    const out = new Map();
    for (const [year, cols] of this.data) {
      const row = {};
      for (const [col, vals] of cols) {
        const valid = vals.filter(v => !isNaN(v));
        row[col] = valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : NaN;
      }
      out.set(year, row);
    }
    return out;
  }

  get years() { return [...this.data.keys()].sort((a, b) => a - b); }
  get cols()  { return [...this.colSet].sort(); }
}

function wdfToRows(wdf) {
  const resolved = wdf.resolve();
  const years    = [...resolved.keys()].sort((a, b) => a - b);
  const cols     = [...wdf.colSet].sort();
  return years.map(y => {
    const row = { year: y };
    const src = resolved.get(y) ?? {};
    for (const c of cols) row[c] = src[c] ?? NaN;
    return row;
  });
}

/** Merge two row-arrays on year. New cols from b that aren't in a get added. */
function mergeOnYear(aRows, bRows) {
  const aMap = new Map(aRows.map(r => [r.year, { ...r }]));
  const aCols = new Set(aRows.length ? Object.keys(aRows[0]) : []);

  for (const bRow of bRows) {
    const year = bRow.year;
    if (!aMap.has(year)) aMap.set(year, { year });
    const aRow = aMap.get(year);
    for (const [k, v] of Object.entries(bRow)) {
      if (k === "year") continue;
      if (!aCols.has(k)) {   // only add genuinely new cols
        aRow[k] = v;
      }
    }
  }

  return [...aMap.values()].sort((a, b) => a.year - b.year);
}

// ── pattern readers ───────────────────────────────────────────────────────────

function detectPattern(headers) {
  const h = new Set(headers.map(c => c.trim().toLowerCase()));
  if (h.has("yoy_pct") && h.has("indicator"))           return "A";
  if (h.has("var_name") && h.has("description"))        return "B";
  if (h.has("block") && h.has("equation"))              return "C";
  if (h.has("sub_row") && h.has("indicator"))           return "D";
  return null;
}

function readPatternA(rows) {
  const wdf = new WideDF();
  for (const r of rows) {
    const year = parseInt(r.year);
    if (isNaN(year)) continue;
    const col = normalizeName(r.indicator);
    if (!col) continue;
    const val = toFloat(r.value);
    if (!isNaN(val)) wdf.set(year, col, val);
  }
  return wdfToRows(wdf);
}

function readPatternB(rows) {
  const wdf = new WideDF();
  for (const r of rows) {
    const year = parseInt(r.year);
    if (isNaN(year)) continue;
    let col = normalizeName(r.var_name);
    if (!col || col === "_") col = normalizeName(r.description);
    if (!col) continue;
    const val = toFloat(r.value);
    if (!isNaN(val)) wdf.set(year, col, val);
  }
  return wdfToRows(wdf);
}

function readPatternC(rows) {
  const wdf = new WideDF();
  for (const r of rows) {
    const year = parseInt(r.year);
    if (isNaN(year)) continue;
    const blockClean = normalizeName(r.block).slice(0, 20);
    const descClean  = normalizeName(r.description);
    const col        = normalizeName(`${blockClean}__${descClean}`);
    if (!col) continue;
    const val = toFloat(r.value);
    if (!isNaN(val)) wdf.set(year, col, val);
  }
  return wdfToRows(wdf);
}

function readPatternD(rows) {
  const wdf = new WideDF();
  for (const r of rows) {
    const year = parseInt(r.year);
    if (isNaN(year)) continue;
    const indClean = normalizeName(r.indicator);
    const subClean = normalizeName(r.sub_row);
    const col = subClean === "value" ? indClean : `${indClean}__${subClean}`;
    if (!col) continue;
    const val = toFloat(r.value);
    if (!isNaN(val)) wdf.set(year, col, val);
  }
  return wdfToRows(wdf);
}

// ── step 1: load combined_wide ────────────────────────────────────────────────

function loadCombinedWide(filePath) {
  log("INFO", `Loading base: ${path.basename(filePath)}`);
  const { headers, rows } = parseCSV(filePath);

  // First column → year
  const yearCol = headers[0];
  const dataCols = headers.slice(1);
  const normCols = deduplicateCols(dataCols.map(normalizeName));

  const result = [];
  for (const r of rows) {
    const year = parseInt(r[yearCol]);
    if (isNaN(year)) continue;
    const out = { year };
    dataCols.forEach((orig, i) => {
      out[normCols[i]] = toFloat(r[orig]);
    });
    result.push(out);
  }

  result.sort((a, b) => a.year - b.year);
  const years = [...new Set(result.map(r => r.year))];
  log("INFO", `  base shape: ${years.length} years × ${normCols.length} cols`);
  return result;
}

// ── step 2: load supplementary CSVs ──────────────────────────────────────────

function loadAllSupplementary(cleanedDir) {
  const SKIP    = new Set(["combined_wide.csv"]);
  const READERS = { A: readPatternA, B: readPatternB, C: readPatternC, D: readPatternD };
  let allRows   = [];

  const files = fs.readdirSync(cleanedDir)
    .filter(f => f.endsWith(".csv") && !SKIP.has(f))
    .sort();

  for (const f of files) {
    const fp = path.join(cleanedDir, f);
    try {
      const { headers, rows } = parseCSV(fp);
      const pattern = detectPattern(headers);
      if (!pattern) { log("WARN", `  Cannot detect pattern: ${f} — skipped`); continue; }
      const parsed = READERS[pattern](rows);
      if (!parsed.length) { log("WARN", `  Empty: ${f}`); continue; }
      const ncols = Object.keys(parsed[0]).length - 1;
      log("INFO", `  [${pattern}] ${f}  → ${parsed.length} years × ${ncols} cols`);
      allRows = allRows.length ? mergeOnYear(allRows, parsed) : parsed;
    } catch (e) {
      log("ERROR", `  Error parsing ${f}: ${e.message}`);
    }
  }

  if (allRows.length) {
    const ncols = Object.keys(allRows[0]).length - 1;
    log("INFO", `Supplementary combined: ${allRows.length} years × ${ncols} cols`);
  }
  return allRows;
}

// ── step 3: merge ─────────────────────────────────────────────────────────────

function mergeDatasets(base, supplement) {
  if (!supplement.length) return base;
  const merged = mergeOnYear(base, supplement);
  const ncols  = Object.keys(merged[0]).length - 1;
  log("INFO", `After merge: ${merged.length} years × ${ncols} cols`);
  return merged;
}

// ── step 4: clean ─────────────────────────────────────────────────────────────

function cleanDataset(rows) {
  if (!rows.length) return { rows, dropped: [] };
  const allCols = Object.keys(rows[0]).filter(k => k !== "year");
  const n = rows.length;

  // Count NaN per col
  const nanCount = {};
  for (const c of allCols) nanCount[c] = 0;
  for (const r of rows) {
    for (const c of allCols) {
      if (isNaN(r[c]) || r[c] == null) nanCount[c]++;
    }
  }

  const dropped = allCols.filter(c => nanCount[c] / n > MISSING_THRESHOLD);
  const keepCols = allCols.filter(c => !dropped.includes(c));

  log("INFO", `Dropped ${dropped.length} cols (>${(MISSING_THRESHOLD*100).toFixed(0)}% missing)`);
  if (dropped.length) {
    dropped.slice(0, 10).forEach(c => log("INFO", `  • ${c}`));
    if (dropped.length > 10) log("INFO", `  … and ${dropped.length - 10} more`);
  }

  // Rebuild rows with only keep cols
  let clean = rows.map(r => {
    const out = { year: r.year };
    for (const c of keepCols) out[c] = isNaN(r[c]) ? null : r[c];
    return out;
  });

  // Forward fill per column
  for (const c of keepCols) {
    let lastVal = null;
    for (const r of clean) {
      if (r[c] != null && !isNaN(r[c])) lastVal = r[c];
      else if (lastVal != null) r[c] = lastVal;
    }
    // Back fill
    lastVal = null;
    for (let i = clean.length - 1; i >= 0; i--) {
      if (clean[i][c] != null && !isNaN(clean[i][c])) lastVal = clean[i][c];
      else if (lastVal != null) clean[i][c] = lastVal;
    }
  }

  const remainNaN = clean.reduce((s, r) =>
    s + keepCols.filter(c => r[c] == null || isNaN(r[c])).length, 0);
  log("INFO", `Clean shape: ${clean.length} years × ${keepCols.length} cols`);
  log("INFO", `Remaining NaN: ${remainNaN} cells`);
  return { rows: clean, dropped };
}

// ── step 5: feature engineering ──────────────────────────────────────────────

function addFeatures(rows) {
  if (!rows.length) return rows;
  const baseCols = Object.keys(rows[0]).filter(k => k !== "year");

  const result = rows.map(r => ({ ...r }));

  for (const col of baseCols) {
    const vals = rows.map(r => r[col] ?? NaN);

    for (let i = 0; i < result.length; i++) {
      // lag1
      result[i][`${col}__lag1`] = i >= 1 ? vals[i - 1] : NaN;
      // lag2
      result[i][`${col}__lag2`] = i >= 2 ? vals[i - 2] : NaN;
      // growth %
      const prev = i >= 1 ? vals[i - 1] : NaN;
      result[i][`${col}__growth_pct`] =
        (!isNaN(prev) && prev !== 0) ? (vals[i] - prev) / prev * 100 : NaN;
      // rolling mean (3)
      const window = [];
      for (let w = Math.max(0, i - ROLLING_WINDOW + 1); w <= i; w++) {
        if (!isNaN(vals[w])) window.push(vals[w]);
      }
      result[i][`${col}__roll_mean_${ROLLING_WINDOW}`] =
        window.length ? window.reduce((a, b) => a + b, 0) / window.length : NaN;
    }
  }

  const featCols = Object.keys(result[0]).length - 1;
  log("INFO", `Feature dataset: ${result.length} years × ${featCols} cols  (+${featCols - baseCols.length} engineered)`);
  return result;
}

// ── summary log ───────────────────────────────────────────────────────────────

function logSummary(master, features, dropped) {
  const mCols  = Object.keys(master[0]).filter(k => k !== "year").length;
  const fCols  = Object.keys(features[0]).filter(k => k !== "year").length;
  const years  = master.map(r => r.year);
  log("INFO", "=".repeat(60));
  log("INFO", `ETL SUMMARY  —  ${new Date().toISOString()}`);
  log("INFO", `  Master dataset : ${master.length} years × ${mCols} indicators`);
  log("INFO", `  Year range     : ${Math.min(...years)} – ${Math.max(...years)}`);
  log("INFO", `  Features file  : ${features.length} years × ${fCols} columns`);
  log("INFO", `  Dropped cols   : ${dropped.length}`);
  log("INFO", "=".repeat(60));
}

// ── null → empty string for CSV output ───────────────────────────────────────
function prepareForCSV(rows) {
  return rows.map(r => {
    const out = {};
    for (const [k, v] of Object.entries(r)) {
      out[k] = (v == null || (typeof v === "number" && isNaN(v))) ? "" : v;
    }
    return out;
  });
}

// ── main ──────────────────────────────────────────────────────────────────────

function main() {
  log("INFO", `ETL started — ${new Date().toISOString()}`);

  // 1. Base
  const base = loadCombinedWide(path.join(CLEANED_DIR, "combined_wide.csv"));

  // 2. Supplementary
  const supplement = loadAllSupplementary(CLEANED_DIR);

  // 3. Merge
  const merged = mergeDatasets(base, supplement);

  // 4. Clean
  const { rows: master, dropped } = cleanDataset(merged);

  // 5. Save master
  writeCSV(MASTER_CSV, prepareForCSV(master));
  log("INFO", `Saved: ${MASTER_CSV}`);

  // 6. Features
  const features = addFeatures(master);

  // 7. Save features
  writeCSV(FEATURES_CSV, prepareForCSV(features));
  log("INFO", `Saved: ${FEATURES_CSV}`);

  // 8. Summary
  logSummary(master, features, dropped);
  flushLog();
  log("INFO", `Log saved: ${LOG_FILE}`);
}

main();
