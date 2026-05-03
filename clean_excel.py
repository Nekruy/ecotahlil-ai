"""
Excel cleaner for two macro model files:
  - S-3-до 2035 года-Model GDP -27.04.2026.xlsx
  - S-3 посл.вар.-CGE_Model results_2025 — 2032-27.04.2026.xlsx

Outputs separate CSVs per logical table type.
"""

import re
import pandas as pd
from pathlib import Path

# ── paths ─────────────────────────────────────────────────────────────────────
DOWNLOADS = Path.home() / "Downloads"
FILE_GDP = DOWNLOADS / "S-3-до 2035 года-Model GDP -27.04.2026.xlsx"
FILE_CGE = DOWNLOADS / "S-3 посл.вар.-CGE_Model results_2025 — 2032-27.04.2026.xlsx"
OUT_DIR = Path(__file__).parent / "data" / "cleaned"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── helpers ───────────────────────────────────────────────────────────────────

def is_year(val) -> bool:
    """True for numeric values in plausible year range."""
    try:
        y = int(float(val))
        return 1990 <= y <= 2050
    except (TypeError, ValueError):
        return False


def extract_year(val) -> int | None:
    """Pull a 4-digit year from a string like 'Соли 2025' or plain 2025."""
    if val is None:
        return None
    s = str(val)
    m = re.search(r"(19|20)\d{2}", s)
    return int(m.group()) if m else None


def clean_label(val) -> str:
    """Strip leading/trailing whitespace and normalise spaces."""
    if val is None:
        return ""
    return re.sub(r"\s+", " ", str(val)).strip()


def forward_fill_years(row: list) -> list:
    """
    Handles the 'merged-cell' pattern: ['Соли 2025', None, 'Соли 2026', None, ...]
    Returns a fully-filled list of integers (or None for non-year cells).
    """
    result = []
    last = None
    for v in row:
        y = extract_year(v)
        if y:
            last = y
        result.append(last)
    return result


# ── PATTERN A: summary sheets ─────────────────────────────────────────────────
# Structure:
#   Row 0  – title (skip)
#   Row 1  – year header: "Соли 2025" | None | "Соли 2026" | None ...
#             (first 1-2 cols are label/unit cols, no year)
#   Row 2  – sub-header: "Воқеӣ" | "нисбат ба соли..." repeated
#   Row 3+ – data
#
# Result: long table  year | indicator | unit | value | yoy_pct

def parse_summary_sheet(xl: pd.ExcelFile, sheet: str,
                         label_col: int = 0, unit_col: int | None = 1,
                         header_row: int = 1, data_start: int = 3) -> pd.DataFrame:
    df_raw = xl.parse(sheet, header=None)

    year_row = df_raw.iloc[header_row].tolist()
    sub_row = df_raw.iloc[header_row + 1].tolist() if (header_row + 1) < len(df_raw) else []

    # Identify data columns by year
    years_filled = forward_fill_years(year_row)

    # For each year, collect (col_value, col_pct) pairs
    year_col_map: dict[int, dict] = {}  # year -> {"val": col_idx, "pct": col_idx}
    for i, y in enumerate(years_filled):
        if y is None or i <= max(label_col, unit_col or -1):
            continue
        sub = clean_label(sub_row[i]) if i < len(sub_row) else ""
        is_pct = bool(re.search(r"фоиз|%|нисбат", sub, re.I))
        entry = year_col_map.setdefault(y, {})
        if is_pct:
            entry.setdefault("pct", i)
        else:
            entry.setdefault("val", i)

    records = []
    for _, row in df_raw.iloc[data_start:].iterrows():
        label = clean_label(row.iloc[label_col])
        if not label:
            continue
        unit = clean_label(row.iloc[unit_col]) if unit_col is not None else ""
        for year, cols in year_col_map.items():
            val_col = cols.get("val")
            pct_col = cols.get("pct")
            val = row.iloc[val_col] if val_col is not None else None
            pct = row.iloc[pct_col] if pct_col is not None else None
            if pd.isna(val) and pd.isna(pct):
                continue
            records.append({
                "year": year,
                "indicator": label,
                "unit": unit,
                "value": None if pd.isna(val) else val,
                "yoy_pct": None if pd.isna(pct) else pct,
            })

    return pd.DataFrame(records)


# ── PATTERN B: historical model sheets ────────────────────────────────────────
# Structure:
#   Row 0  – [Var.Name] | [Description] | 1997 | 1998 | ... (year cols as ints)
#   Row 1+ – data  (some sheets start at col B, i.e. col index 1)
#
# Result: long table  year | var_name | description | value

def parse_model_sheet(xl: pd.ExcelFile, sheet: str,
                       start_col: int = 0) -> pd.DataFrame:
    df_raw = xl.parse(sheet, header=None)

    # Find the header row: first row that has at least 3 year-like values
    header_row_idx = None
    for i, row in df_raw.iterrows():
        vals = [v for v in row.tolist() if is_year(v)]
        if len(vals) >= 3:
            header_row_idx = i
            break
    if header_row_idx is None:
        return pd.DataFrame()

    header = df_raw.iloc[header_row_idx].tolist()

    # Locate var_name and description columns (non-year cols before year block)
    year_cols = {}  # col_idx -> year
    label_cols = []
    for i, h in enumerate(header):
        if i < start_col:
            continue
        if is_year(h):
            year_cols[i] = int(float(h))
        else:
            if not year_cols:  # still in label zone
                label_cols.append(i)

    var_col = label_cols[0] if len(label_cols) >= 1 else None
    desc_col = label_cols[1] if len(label_cols) >= 2 else None

    records = []
    for _, row in df_raw.iloc[header_row_idx + 1:].iterrows():
        var = clean_label(row.iloc[var_col]) if var_col is not None else ""
        desc = clean_label(row.iloc[desc_col]) if desc_col is not None else ""
        if not var and not desc:
            continue
        for col_i, year in year_cols.items():
            v = row.iloc[col_i]
            if pd.isna(v):
                continue
            records.append({
                "year": year,
                "var_name": var,
                "description": desc,
                "value": v,
            })

    return pd.DataFrame(records)


# ── PATTERN C: CGE detail sheets ──────────────────────────────────────────────
# Multiple sub-blocks per sheet; each block:
#   title row (RU)  →  ignored
#   "Source: …"     →  ignored
#   year header row →  2024 | 2025 | ... | 2032
#   data rows       →  equation/var | sector_label | val | val | ...
#
# Result: long table  year | equation | description | block | value

def parse_cge_block_sheet(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    df_raw = xl.parse(sheet, header=None)
    rows = df_raw.values.tolist()

    # Find all year-header rows (≥4 consecutive year values in one row)
    def is_year_header_row(row):
        years = [v for v in row if is_year(v)]
        return len(years) >= 4

    records = []
    current_block = ""
    year_map: dict[int, int] = {}  # col_idx -> year

    for r_idx, row in enumerate(rows):
        if is_year_header_row(row):
            year_map = {}
            for c_idx, v in enumerate(row):
                if is_year(v):
                    year_map[c_idx] = int(float(v))
            continue

        # Detect block titles: first non-null cell in col 0 or 1, no year values
        first_val = clean_label(row[0]) if len(row) > 0 else ""
        second_val = clean_label(row[1]) if len(row) > 1 else ""

        if first_val and not year_map:
            current_block = first_val
            continue

        if not year_map:
            continue

        # Source rows
        if first_val.lower().startswith("source"):
            continue

        # Identify label columns (first two non-year non-empty cols)
        equation = first_val or second_val
        description = second_val if first_val else ""
        if not equation:
            continue

        # Check if this is a block title (no numeric data in year cols)
        has_data = any(
            not pd.isna(row[c]) and isinstance(row[c], (int, float))
            for c in year_map
            if c < len(row)
        )
        if not has_data:
            current_block = equation
            continue

        for c_idx, year in year_map.items():
            if c_idx >= len(row):
                continue
            v = row[c_idx]
            if pd.isna(v):
                continue
            records.append({
                "year": year,
                "block": current_block,
                "equation": equation,
                "description": description,
                "value": v,
            })

    return pd.DataFrame(records)


# ── PATTERN D: Results sheet ───────────────────────────────────────────────────
# Row 0 – title; Row 1 – year row (plain integers); data rows with
# sub-rows labelled "Темп роста" / "Дефлятор" immediately after the main row.

def parse_results_sheet(xl: pd.ExcelFile, sheet: str = "Results") -> pd.DataFrame:
    df_raw = xl.parse(sheet, header=None)
    header = df_raw.iloc[1].tolist()

    year_cols = {i: int(float(v)) for i, v in enumerate(header) if is_year(v)}
    if not year_cols:
        return pd.DataFrame()

    records = []
    current_indicator = ""
    for _, row in df_raw.iloc[2:].iterrows():
        label = clean_label(row.iloc[0])
        if not label:
            continue
        sub_keywords = ("темп", "дефлятор", "рост")
        is_sub = any(k in label.lower() for k in sub_keywords)
        if not is_sub:
            current_indicator = label
        full_label = label if is_sub else label
        for c_idx, year in year_cols.items():
            v = row.iloc[c_idx]
            if pd.isna(v):
                continue
            records.append({
                "year": year,
                "indicator": current_indicator,
                "sub_row": label if is_sub else "value",
                "value": v,
            })

    return pd.DataFrame(records)


# ── pivot helper ───────────────────────────────────────────────────────────────

def to_wide(df: pd.DataFrame, index_cols: list[str],
            value_col: str = "value") -> pd.DataFrame:
    """Pivot long → wide with year as rows, indicators as columns."""
    if df.empty:
        return df
    pivot_cols = [c for c in df.columns if c not in index_cols + [value_col]]
    if not pivot_cols:
        return df
    return df.pivot_table(index=index_cols, columns=pivot_cols,
                           values=value_col, aggfunc="first").reset_index()


# ── main ───────────────────────────────────────────────────────────────────────

def process_file(path: Path, label: str):
    print(f"\n{'='*60}")
    print(f"Processing: {path.name}")
    print(f"{'='*60}")

    xl = pd.ExcelFile(path, engine="openpyxl")
    sheets = xl.sheet_names
    print(f"Sheets: {sheets}")

    # ── Summary/forecast sheets (Pattern A) ──────────────────────────────────
    summary_sheets = {
        "S-1 реалистичный сценарий": (0, 1, 1, 3),
        "Итог-Модел CGE":           (0, 1, 1, 3),
        "посл.вар.":                (0, 1, 1, 3),
        "2025-2029":                (0, None, 1, 3),
        "Лист1":                    (0, None, 1, 3),
        "Лист3":                    (0, None, 1, 3),
    }
    for sheet_name, (lc, uc, hr, ds) in summary_sheets.items():
        if sheet_name not in sheets:
            continue
        try:
            df = parse_summary_sheet(xl, sheet_name,
                                     label_col=lc, unit_col=uc,
                                     header_row=hr, data_start=ds)
            if df.empty:
                print(f"  [SKIP] {sheet_name} — empty after parse")
                continue
            out = OUT_DIR / f"{label}__{sheet_name.replace(' ', '_')}.csv"
            df.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"  [OK] {sheet_name} → {out.name}  ({len(df)} rows)")
        except Exception as e:
            print(f"  [ERR] {sheet_name}: {e}")

    # ── Historical model sheets (Pattern B) ──────────────────────────────────
    model_sheets = {
        "GDP":           0,
        "Agriculture":   0,
        "Industry":      0,
        "Industry-2":    0,
        "OEA":           0,
        "Household":     0,
        "Trade":         0,
        "Monetary":      0,
        "BoP":           0,
        "Revenue Model": 0,
        "gateway":       0,
    }
    for sheet_name, sc in model_sheets.items():
        if sheet_name not in sheets:
            continue
        try:
            df = parse_model_sheet(xl, sheet_name, start_col=sc)
            if df.empty:
                print(f"  [SKIP] {sheet_name} — empty after parse")
                continue
            out = OUT_DIR / f"{label}__{sheet_name.replace(' ', '_')}.csv"
            df.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"  [OK] {sheet_name} → {out.name}  ({len(df)} rows)")
        except Exception as e:
            print(f"  [ERR] {sheet_name}: {e}")

    # ── Results sheet (Pattern D) ─────────────────────────────────────────────
    if "Results" in sheets:
        try:
            df = parse_results_sheet(xl, "Results")
            if not df.empty:
                out = OUT_DIR / f"{label}__Results.csv"
                df.to_csv(out, index=False, encoding="utf-8-sig")
                print(f"  [OK] Results → {out.name}  ({len(df)} rows)")
        except Exception as e:
            print(f"  [ERR] Results: {e}")

    # ── CGE detail sheets (Pattern C) ─────────────────────────────────────────
    cge_sheets = [
        "1_Prices", "2_Production_1", "2_Production_2",
        "3_In & SA", "4_Demand", "5_IntTrade",
        "6_Closures", "7_Dynamics", "GDP-total",
    ]
    for sheet_name in cge_sheets:
        if sheet_name not in sheets:
            continue
        try:
            df = parse_cge_block_sheet(xl, sheet_name)
            if df.empty:
                print(f"  [SKIP] {sheet_name} — empty after parse")
                continue
            out = OUT_DIR / f"{label}__{sheet_name.replace(' ', '_')}.csv"
            df.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"  [OK] {sheet_name} → {out.name}  ({len(df)} rows)")
        except Exception as e:
            print(f"  [ERR] {sheet_name}: {e}")

    xl.close()


def build_combined(label_filter: str | None = None):
    """
    Merge all summary CSVs into one wide table: year × indicator.
    (Only sheets with year/indicator/value columns.)
    """
    frames = []
    pattern = f"{label_filter}__*.csv" if label_filter else "*.csv"
    for f in sorted(OUT_DIR.glob(pattern)):
        df = pd.read_csv(f, encoding="utf-8-sig")
        if {"year", "indicator", "value"}.issubset(df.columns):
            df["source"] = f.stem
            frames.append(df[["year", "indicator", "unit" if "unit" in df.columns else "source",
                               "value"]])
    if not frames:
        print("No summary CSVs found for combining.")
        return
    combined = pd.concat(frames, ignore_index=True)
    # Pivot: rows = year, columns = indicator
    wide = combined.pivot_table(
        index="year", columns="indicator", values="value", aggfunc="first"
    ).reset_index()
    wide.columns.name = None
    out = OUT_DIR / "combined_wide.csv"
    wide.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n[COMBINED] {out}  ({wide.shape[0]} years × {wide.shape[1]-1} indicators)")


if __name__ == "__main__":
    if not FILE_GDP.exists():
        print(f"[WARN] Not found: {FILE_GDP}")
    else:
        process_file(FILE_GDP, "gdp_model")

    if not FILE_CGE.exists():
        print(f"[WARN] Not found: {FILE_CGE}")
    else:
        process_file(FILE_CGE, "cge_model")

    build_combined()
    print("\nDone. CSVs in:", OUT_DIR)
