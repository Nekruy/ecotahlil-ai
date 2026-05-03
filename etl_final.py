"""
etl_final.py — Master dataset builder for macro ML models.

Input:  data/cleaned/*.csv
Output: data/final/master_dataset.csv
        data/final/features_dataset.csv
        data/final/etl_log.txt
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ── paths ─────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent
CLEANED    = ROOT / "data" / "cleaned"
FINAL      = ROOT / "data" / "final"
FINAL.mkdir(parents=True, exist_ok=True)

MASTER_CSV   = FINAL / "master_dataset.csv"
FEATURES_CSV = FINAL / "features_dataset.csv"
LOG_FILE     = FINAL / "etl_log.txt"

# ── constants ─────────────────────────────────────────────────────────────────
MISSING_THRESHOLD = 0.40   # drop columns with > 40 % NaN
ROLLING_WINDOW    = 3


# ── logging ───────────────────────────────────────────────────────────────────

def setup_logger() -> logging.Logger:
    log = logging.getLogger("etl")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%H:%M:%S")
    log.addHandler(logging.StreamHandler(sys.stdout))
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log


log = setup_logger()


# ── name normalisation ────────────────────────────────────────────────────────

def normalize_name(raw: str) -> str:
    """
    Lowercase, strip punctuation noise, spaces → underscore.
    Preserves Cyrillic + Latin; removes commas, dashes at boundaries.
    """
    s = str(raw).strip()
    # Remove BOM / zero-width chars
    s = s.replace("﻿", "").replace("​", "")
    # Strip leading/trailing punctuation that leaked from merged cells
    s = re.sub(r'^[\-–—,\s]+|[\-–—,\s]+$', '', s)
    # Collapse internal whitespace
    s = re.sub(r'\s+', ' ', s)
    # Replace spaces and hyphens between words with underscore
    s = re.sub(r'[\s\-–—]+', '_', s)
    # Drop characters that are neither word chars nor underscores
    # Keep Cyrillic (Ѐ-ӿ) + Latin + digits + underscore
    s = re.sub(r'[^\wЀ-ӿ]', '', s)
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s).strip('_')
    return s.lower()


def deduplicate_cols(cols: list[str]) -> list[str]:
    """Append _2, _3, … to duplicate column names."""
    seen: dict[str, int] = {}
    result = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            result.append(c)
        else:
            seen[c] += 1
            result.append(f"{c}_{seen[c] + 1}")
    return result


# ── readers for each pattern ──────────────────────────────────────────────────

def read_pattern_a(path: Path) -> pd.DataFrame:
    """year, indicator, unit, value, yoy_pct  →  wide by indicator."""
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Extract value column only (yoy_pct is derived in feature engineering)
    val = (
        df[["year", "indicator", "value"]]
        .copy()
        .assign(indicator=lambda d: d["indicator"].map(normalize_name))
    )
    val["value"] = pd.to_numeric(val["value"], errors="coerce")
    wide = val.groupby(["year", "indicator"])["value"].mean().unstack("indicator")
    wide.index.name = "year"
    return wide


def read_pattern_b(path: Path) -> pd.DataFrame:
    """year, var_name, description, value  →  wide by var_name."""
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Prefer var_name as the column key (it's a clean code like GDPmp_t)
    df["col_key"] = df["var_name"].fillna("").map(normalize_name)
    # Fall back to description if var_name is empty
    mask_empty = df["col_key"].str.strip("_") == ""
    df.loc[mask_empty, "col_key"] = df.loc[mask_empty, "description"].map(normalize_name)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    wide = df.groupby(["year", "col_key"])["value"].mean().unstack("col_key")
    wide.index.name = "year"
    return wide


def read_pattern_c(path: Path) -> pd.DataFrame:
    """year, block, equation, description, value  →  wide by description."""
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Use description as key; prepend block prefix to avoid clashes
    df["block_clean"] = df["block"].map(normalize_name)
    df["desc_clean"]  = df["description"].map(normalize_name)
    df["col_key"]     = df["block_clean"].str[:20] + "__" + df["desc_clean"]
    df["col_key"]     = df["col_key"].map(normalize_name)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    wide = df.groupby(["year", "col_key"])["value"].mean().unstack("col_key")
    wide.index.name = "year"
    return wide


def read_pattern_d(path: Path) -> pd.DataFrame:
    """year, indicator, sub_row, value  →  wide by indicator+sub_row."""
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    df["ind_clean"] = df["indicator"].map(normalize_name)
    df["sub_clean"] = df["sub_row"].map(normalize_name)
    # "value" sub_row → use indicator name directly; others get a suffix
    df["col_key"] = df.apply(
        lambda r: r["ind_clean"] if r["sub_clean"] == "value"
                  else f"{r['ind_clean']}__{r['sub_clean']}",
        axis=1,
    )

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    wide = df.groupby(["year", "col_key"])["value"].mean().unstack("col_key")
    wide.index.name = "year"
    return wide


def detect_pattern(path: Path) -> str | None:
    """Detect CSV pattern from column names."""
    try:
        header = pd.read_csv(path, encoding="utf-8-sig", nrows=0).columns.tolist()
    except Exception:
        return None
    cols = {c.strip().lower() for c in header}
    if "yoy_pct" in cols and "indicator" in cols:
        return "A"
    if "var_name" in cols and "description" in cols:
        return "B"
    if "block" in cols and "equation" in cols:
        return "C"
    if "sub_row" in cols and "indicator" in cols:
        return "D"
    return None


# ── step 1: load combined_wide as base ───────────────────────────────────────

def load_combined_wide(path: Path) -> pd.DataFrame:
    log.info("Loading base: %s", path.name)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    # Rename first column to year if needed
    df.columns = [c.strip() for c in df.columns]
    if df.columns[0].lower() != "year":
        df = df.rename(columns={df.columns[0]: "year"})

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df.set_index("year").sort_index()

    # Coerce all columns to float (placeholder strings like "value" → NaN)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize column names
    df.columns = [normalize_name(c) for c in df.columns]
    df.columns = deduplicate_cols(list(df.columns))

    log.info("  base shape: %d years × %d cols", *df.shape)
    return df


# ── step 2: load and merge all other CSVs ────────────────────────────────────

def load_all_supplementary(cleaned_dir: Path) -> pd.DataFrame:
    SKIP = {"combined_wide.csv"}
    READERS = {"A": read_pattern_a, "B": read_pattern_b,
               "C": read_pattern_c, "D": read_pattern_d}

    frames: list[pd.DataFrame] = []
    for csv_path in sorted(cleaned_dir.glob("*.csv")):
        if csv_path.name in SKIP:
            continue
        pattern = detect_pattern(csv_path)
        if pattern is None:
            log.warning("  Cannot detect pattern: %s — skipped", csv_path.name)
            continue
        try:
            wide = READERS[pattern](csv_path)
            if wide.empty:
                log.warning("  Empty after parsing: %s", csv_path.name)
                continue
            # Normalize col names
            wide.columns = [normalize_name(c) for c in wide.columns]
            wide.columns = deduplicate_cols(list(wide.columns))
            frames.append(wide)
            log.info("  [%s] %s  → %d years × %d cols",
                     pattern, csv_path.name, *wide.shape)
        except Exception as exc:
            log.error("  Error parsing %s: %s", csv_path.name, exc)

    if not frames:
        log.warning("No supplementary frames loaded.")
        return pd.DataFrame()

    combined = pd.concat(frames, axis=0, join="outer")
    combined = combined.groupby(combined.index).mean()   # aggregate duplicates by year
    combined.index.name = "year"
    log.info("Supplementary combined: %d years × %d cols", *combined.shape)
    return combined


# ── step 3: merge base + supplementary ───────────────────────────────────────

def merge_datasets(base: pd.DataFrame, supplement: pd.DataFrame) -> pd.DataFrame:
    if supplement.empty:
        return base

    # Avoid duplicating columns already in base
    new_cols = [c for c in supplement.columns if c not in base.columns]
    dropped  = len(supplement.columns) - len(new_cols)
    if dropped:
        log.info("Skipped %d duplicate columns already in base", dropped)

    merged = base.join(supplement[new_cols], how="outer")
    merged = merged.sort_index()
    log.info("After merge: %d years × %d cols", *merged.shape)
    return merged


# ── step 4: clean ─────────────────────────────────────────────────────────────

def clean_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    initial_cols = list(df.columns)

    # Cast everything to float
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop columns with > MISSING_THRESHOLD NaN
    missing_ratio = df.isna().mean()
    drop_cols = missing_ratio[missing_ratio > MISSING_THRESHOLD].index.tolist()
    df = df.drop(columns=drop_cols)
    log.info("Dropped %d columns (>%.0f%% missing): %s",
             len(drop_cols), MISSING_THRESHOLD * 100,
             drop_cols[:10])  # log first 10 to keep it readable

    # Forward fill (time-series continuity)
    df = df.ffill()

    # Back fill for any remaining NaN at the start of the series
    df = df.bfill()

    remaining_cols = list(df.columns)
    log.info("Clean shape: %d years × %d cols", *df.shape)
    return df, drop_cols


# ── step 5: feature engineering ──────────────────────────────────────────────

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = list(df.columns)
    feat_frames: list[pd.DataFrame] = []

    for col in base_cols:
        s = df[col]

        lag1  = s.shift(1).rename(f"{col}__lag1")
        lag2  = s.shift(2).rename(f"{col}__lag2")
        growth = ((s - s.shift(1)) / s.shift(1).replace(0, np.nan) * 100
                 ).rename(f"{col}__growth_pct")
        roll  = s.rolling(ROLLING_WINDOW, min_periods=1).mean().rename(
                    f"{col}__roll_mean_{ROLLING_WINDOW}")

        feat_frames.extend([lag1, lag2, growth, roll])

    features = pd.concat([df] + feat_frames, axis=1)
    log.info("Feature dataset: %d years × %d cols  (+%d engineered)",
             *features.shape, features.shape[1] - df.shape[1])
    return features


# ── logging summary ───────────────────────────────────────────────────────────

def log_summary(master: pd.DataFrame, features: pd.DataFrame,
                dropped: list[str]) -> None:
    log.info("=" * 60)
    log.info("ETL SUMMARY  —  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    log.info("  Master dataset : %d years × %d indicators",
             master.shape[0], master.shape[1])
    log.info("  Year range     : %d – %d",
             master.index.min(), master.index.max())
    log.info("  Features file  : %d years × %d columns",
             features.shape[0], features.shape[1])
    log.info("  Dropped cols   : %d", len(dropped))
    if dropped:
        for c in dropped:
            log.info("    • %s", c)
    log.info("  Remaining NaN  : %d cells (master)",
             master.isna().sum().sum())
    log.info("=" * 60)


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("ETL started — %s", datetime.now().isoformat())

    # 1. Base
    base = load_combined_wide(CLEANED / "combined_wide.csv")

    # 2. All other CSVs
    supplement = load_all_supplementary(CLEANED)

    # 3. Merge
    master = merge_datasets(base, supplement)

    # 4. Clean
    master, dropped_cols = clean_dataset(master)

    # 5. Save master
    master.to_csv(MASTER_CSV, encoding="utf-8-sig")
    log.info("Saved: %s", MASTER_CSV)

    # 6. Feature engineering
    features = add_features(master)

    # 7. Save features
    features.to_csv(FEATURES_CSV, encoding="utf-8-sig")
    log.info("Saved: %s", FEATURES_CSV)

    # 8. Summary log
    log_summary(master, features, dropped_cols)
    log.info("Log saved: %s", LOG_FILE)


if __name__ == "__main__":
    main()
