/**
 * masterDataLoader.js
 *
 * Unified data loader backed by data/final/master_dataset.csv.
 * Drop-in compatible with historicalDB.getDataForForecasting(indicator).
 *
 * Priority chain (all endpoints):
 *   master_dataset.csv  →  historicalDB  →  error
 */

"use strict";

const fs   = require("fs");
const path = require("path");

// ── column alias registry ─────────────────────────────────────────────────────
// Maps semantic model keys → ordered list of candidate CSV columns.
// First column found with ≥ MIN_POINTS non-NaN values wins.
const ALIASES = {
  // ── GDP ──
  gdp:            ["gdpmpt",                     "ввп_в_рыночных_ценах_млн_сомони"],
  gdp_growth:     ["ggdpt"],
  gdp_deflator:   ["defgdpt"],
  gdp_real:       ["gdp2009mpt",                 "ввп_в_рыночных_ценах_2009_года"],
  gdp_per_capita: ["gdppcust",                   "ввп_на_душу_населения_в_сомони"],
  gdp_income:     ["ввп_по_доходам_без_трансфертов"],
  gdp_expenditure:["ввп_по_использованию"],
  gdp_basic:      ["ввп_в_основных_ценах"],

  // ── Inflation ──
  inflation:      ["ипц_в_том_числе_декдек_факт"],
  cpi:            ["ипц_в_том_числе_декдек_факт"],

  // ── Sectors ──
  industry:       ["промышленность"],
  agriculture:    ["сельское_хозяйство_и_рыболовство"],
  construction:   ["constrt"],
  transport:      ["транспорт_и_связь"],
  trade:          ["розничная_и_оптовая_торговля"],
  services:       ["услуги",                     "прочие_услуги_остатком"],

  // ── Demand & Consumption ──
  consumption:    ["конечное_потребление"],
  consumption_hh: ["конечное_потребление_населения"],
  consumption_gov:["конечное_потребление_государства"],
  investment:     ["инвестиции_в_ок",            "инвестиции_в_ок_ввп"],
  domestic_demand:["внутренний_спрос"],

  // ── Trade ──
  exports:        ["экспорт_товаров_и_услуг"],
  imports:        ["импорт_товаров_и_услуг"],
  net_exports:    ["чистый_экспорт_товаров_и_услуг"],

  // ── Labour / Income ──
  wages:          ["оплата_труда_наемных_работников"],
  gross_profit:   ["валовая_прибыль_и_смешанные_доходы"],

  // ── Public finance ──
  taxes:          ["налоги_на_производства_и_импорт"],
  subsidies:      ["субсидии_на_производство_и_импорт"],

  // ── Model var-name codes (Pattern B keys) ──
  constrt:        ["constrt"],
  gconstrt:       ["gconstrt"],
  defconstrt:     ["defconstrt"],
  tranct:         ["tranct"],
  invt:           ["invt"],
  gtranpt:        ["gtranpt"],
};

const MIN_POINTS = 4;

// ── CSV parser (no external deps) ────────────────────────────────────────────
function parseCSV(filePath) {
  const raw     = fs.readFileSync(filePath, "utf8").replace(/^﻿/, "");
  const lines   = raw.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: new Map() };

  function splitLine(line) {
    const cols = []; let cur = "", inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') { if (inQ && line[i+1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
      else if (ch === ',' && !inQ) { cols.push(cur); cur = ""; }
      else cur += ch;
    }
    cols.push(cur);
    return cols;
  }

  const headers = splitLine(lines[0]).map(h => h.trim());
  const rows    = new Map();   // year (int) → { col: number }

  for (let i = 1; i < lines.length; i++) {
    const vals = splitLine(lines[i]);
    const year = parseInt(vals[0]);
    if (isNaN(year)) continue;
    const row = {};
    for (let j = 1; j < headers.length; j++) {
      const v = parseFloat(vals[j]);
      row[headers[j]] = isNaN(v) ? null : v;
    }
    rows.set(year, row);
  }

  return { headers: headers.slice(1), rows };
}

// ── MasterDataLoader ─────────────────────────────────────────────────────────
class MasterDataLoader {
  constructor(csvPath) {
    this.csvPath = csvPath;
    this.headers = [];
    this.rows    = new Map();  // year → { col: value|null }
    this._ready  = false;
    this._load();
  }

  _load() {
    try {
      const { headers, rows } = parseCSV(this.csvPath);
      this.headers = headers;
      this.rows    = rows;
      this._ready  = rows.size > 0;
    } catch (e) {
      console.error("[MasterDataLoader] Failed to load CSV:", e.message);
    }
  }

  get ready()   { return this._ready; }
  get yearMin() { return Math.min(...this.rows.keys()); }
  get yearMax() { return Math.max(...this.rows.keys()); }

  /**
   * Resolve an alias or exact column name → the best matching column.
   * Returns null if nothing found with enough data.
   */
  resolve(alias) {
    const candidates = ALIASES[alias] || [alias];
    for (const col of candidates) {
      if (this.headers.includes(col)) {
        const count = [...this.rows.values()].filter(r => r[col] != null).length;
        if (count >= MIN_POINTS) return col;
      }
    }
    return null;
  }

  /**
   * Return sorted array of { year, value } for a given alias/column.
   * Options:
   *   startYear  – filter from year (inclusive)
   *   endYear    – filter to year (inclusive)
   *   historical – if true, cap at current calendar year (exclude forecast years)
   */
  getYearValuePairs(alias, { startYear, endYear, historical = false } = {}) {
    const col = this.resolve(alias);
    if (!col) return [];

    const cap = historical ? new Date().getFullYear() : Infinity;
    const result = [];

    for (const [year, row] of this.rows) {
      if (startYear && year < startYear) continue;
      if (endYear   && year > endYear)   continue;
      if (year > cap) continue;
      if (row[col] != null) result.push({ year, value: row[col] });
    }

    result.sort((a, b) => a.year - b.year);
    return result;
  }

  /**
   * Return plain number array, sorted by year — matches historicalDB interface.
   * Options same as getYearValuePairs.
   */
  getTimeSeries(alias, opts = {}) {
    return this.getYearValuePairs(alias, opts).map(p => p.value);
  }

  /** historicalDB.getDataForForecasting() drop-in. */
  getDataForForecasting(indicator) {
    return this.getTimeSeries(indicator, { historical: true });
  }

  /** True if alias resolves and has ≥ minPoints values. */
  hasSeries(alias, minPoints = MIN_POINTS) {
    return this.getTimeSeries(alias).length >= minPoints;
  }

  /**
   * Returns the 4-variable input object expected by var_model().
   * Falls back per-variable so partial matches work.
   * exchange_rate and remittances are NOT in master_dataset — returns null
   * for those so the caller can fill from historicalDB.
   */
  getVARInputs({ startYear, endYear } = {}) {
    const opts = { startYear, endYear, historical: true };
    return {
      gdp:           this.hasSeries("gdp_growth") ? this.getTimeSeries("gdp_growth", opts) : null,
      inflation:     this.hasSeries("inflation")  ? this.getTimeSeries("inflation",  opts) : null,
      exchange_rate: null,   // not in master_dataset → caller uses historicalDB
      remittances:   null,   // not in master_dataset → caller uses historicalDB
      _source:       "master_dataset.csv",
    };
  }

  /** Returns { year, value } pairs for any column — used by /api/history. */
  getHistory(alias, opts = {}) {
    return this.getYearValuePairs(alias, opts);
  }

  /** All recognised column names in the CSV. */
  getAvailableIndicators() {
    return this.headers.slice();
  }

  /** All registered aliases that resolve successfully. */
  getAvailableAliases() {
    return Object.keys(ALIASES).filter(a => this.resolve(a) !== null);
  }

  /** Summary stats for logging / /api/master-info endpoint. */
  getSummary() {
    return {
      ready:      this._ready,
      csvPath:    this.csvPath,
      years:      this.rows.size,
      yearRange:  `${this.yearMin}–${this.yearMax}`,
      indicators: this.headers.length,
      aliases:    this.getAvailableAliases(),
    };
  }
}

// ── singleton ─────────────────────────────────────────────────────────────────
const CSV_PATH = path.join(__dirname, "data", "final", "master_dataset.csv");
const loader   = new MasterDataLoader(CSV_PATH);

if (loader.ready) {
  const s = loader.getSummary();
  console.log(
    `[MasterDataLoader] Ready — ${s.years} years (${s.yearRange}), ` +
    `${s.indicators} indicators, ${s.aliases.length} aliases resolved`
  );
} else {
  console.warn("[MasterDataLoader] CSV not loaded — run etl_final_node.js first");
}

module.exports = loader;
