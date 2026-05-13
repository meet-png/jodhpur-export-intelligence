"""Transform raw Comtrade JSON into the canonical clean dataset.

Reads every ``data/raw/comtrade_*.json`` file produced by
``src.ingest.comtrade_api``, flattens Comtrade's nested response into a
tidy long-form DataFrame, applies all cleaning operations the PRD §5.2
specifies, and persists the result to
``data/processed/exports_clean.parquet`` (canonical) and
``data/processed/exports_clean.csv`` (human-inspectable).

Why two output formats?
-----------------------
* **Parquet** is the canonical machine format — columnar, compressed,
  preserves dtypes, ~10x smaller than CSV and ~3x faster to load. All
  notebooks and ``load_db.py`` read the Parquet.
* **CSV** is committed to the repo as a Git-diffable artifact so reviewers
  can scan the data without a notebook. The PRD's acceptance criteria
  explicitly require ``exports_clean.csv``.

Cleaning operations applied (PRD §5.2)
--------------------------------------
1. Standardise country names to ISO 3166-1 alpha-3 via ``pycountry``.
2. Parse all dates to Python ``datetime``; derive ``year``, ``month``,
   ``quarter``, Indian financial year (Apr-Mar), and ``is_peak_season``.
3. Compute ``unit_price_usd`` = ``fob_usd / quantity_kg``, rounded to 4 dp.
4. Add ``source`` column (``'COMTRADE'`` for these rows).
5. Drop rows with null shipment date, null fob_usd, or fob_usd <= 0.
6. Deduplicate on (shipment_date, dest_country, hs_code, fob_usd) per
   PRD §8.3.
7. Flag — but do not drop — outliers above the 99th percentile of
   ``unit_price_usd`` per HS code (PRD §8.3 outlier handling).

CLI
---
    python -m src.transform.clean
    python -m src.transform.clean --raw-dir data/raw --out-dir data/processed

Output
------
    data/processed/exports_clean.parquet
    data/processed/exports_clean.csv
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import pycountry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR_DEFAULT = PROJECT_ROOT / "data" / "raw"
OUT_DIR_DEFAULT = PROJECT_ROOT / "data" / "processed"

# PRD §5.4 — pre-Christmas demand peak window for Western markets.
PEAK_SEASON_MONTHS = (9, 10, 11)

# Comtrade partner codes that are aggregates rather than real countries.
# We exclude these from the country-grain dataset; the aggregate values
# are recoverable by GROUP BY on the cleaned data anyway.
COMTRADE_AGGREGATE_PARTNERS = {
    0,
    99,
    199,
    299,
    399,
    459,
    499,
    568,
    577,
    581,
    711,
    829,
    838,
    839,
    879,
    899,
    0,
}

# Map Comtrade-specific country names that pycountry doesn't recognise to
# proper ISO names. Extended as we discover edge cases.
COUNTRY_NAME_OVERRIDES = {
    "Bolivia (Plurinational State of)": "Bolivia",
    "Iran (Islamic Republic of)": "Iran",
    "Korea, Republic of": "South Korea",
    "Korea, Dem. People's Rep. of": "North Korea",
    "United States of America": "United States",
    "United Rep. of Tanzania": "Tanzania",
    "Russian Federation": "Russia",
    "Viet Nam": "Vietnam",
    "Lao People's Dem. Rep.": "Laos",
    "Syrian Arab Republic": "Syria",
    "Venezuela (Bolivarian Rep. of)": "Venezuela",
    "Türkiye": "Turkey",
    "Czechia": "Czech Republic",
    "Brunei Darussalam": "Brunei",
    "Eswatini": "Swaziland",
    "Cabo Verde": "Cape Verde",
    "Côte d'Ivoire": "Ivory Coast",
    "State of Palestine": "Palestine, State of",
    "China, Hong Kong SAR": "Hong Kong",
    "China, Macao SAR": "Macao",
    # Comtrade's abbreviated short-forms discovered on first real-data clean
    "Bosnia Herzegovina": "Bosnia and Herzegovina",
    "Cayman Isds": "Cayman Islands",
    "Dem. People's Rep. of Korea": "North Korea",
    "Dem. Rep. of the Congo": "Congo, The Democratic Republic of the",
    "Dominican Rep.": "Dominican Republic",
    "Norfolk Isds": "Norfolk Island",
    "Rep. of Korea": "South Korea",
    "Rep. of Moldova": "Moldova",
    "Solomon Isds": "Solomon Islands",
    "Turks and Caicos Isds": "Turks and Caicos Islands",
    # Drop genuine aggregates / non-state areas
    "Other Asia, nes": None,  # genuine aggregate, drop
    "Areas, nes": None,
    "Bunkers": None,
    "Free Zones": None,
    "Special Categories": None,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("clean")


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclass
class CleanReport:
    raw_files_read: int = 0
    rows_in: int = 0
    rows_out: int = 0
    rows_dropped_null: int = 0
    rows_dropped_aggregate: int = 0
    rows_deduped: int = 0
    rows_outlier_flagged: int = 0
    unmapped_countries: set[str] | None = None

    def log(self) -> None:
        log.info("=" * 70)
        log.info("Clean summary:")
        log.info("  raw files read           : %d", self.raw_files_read)
        log.info("  rows ingested            : %d", self.rows_in)
        log.info("  rows after cleaning      : %d", self.rows_out)
        log.info("  rows dropped (null/<=0)  : %d", self.rows_dropped_null)
        log.info("  rows dropped (aggregate) : %d", self.rows_dropped_aggregate)
        log.info("  rows deduped             : %d", self.rows_deduped)
        log.info("  rows flagged outlier     : %d", self.rows_outlier_flagged)
        if self.unmapped_countries:
            log.warning(
                "  %d country names had no ISO mapping — extend COUNTRY_NAME_OVERRIDES: %s",
                len(self.unmapped_countries),
                sorted(self.unmapped_countries),
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_latest_raw_files(raw_dir: Path) -> list[Path]:
    """Return one path per (hs_code, year) — the most recent date-stamped file.

    PRD FR-1 keeps every historical pull on disk; for the cleaned dataset
    we always want the freshest snapshot per (hs_code, year).
    """
    candidates: dict[tuple[str, str], Path] = {}
    for path in sorted(raw_dir.glob("comtrade_*.json")):
        # filename pattern: comtrade_{hs_code}_{year}_{YYYYMMDD}.json
        parts = path.stem.split("_")
        if len(parts) < 4:
            continue
        hs_code, year_str = parts[1], parts[2]
        key = (hs_code, year_str)
        # alphabetical sort puts newer dates last → overwrite
        candidates[key] = path
    return list(candidates.values())


def _to_iso_alpha3(comtrade_name: str) -> str | None:
    """Map a Comtrade country/area name to ISO 3166-1 alpha-3.

    Returns ``None`` for genuine aggregates (e.g. 'Areas, nes', 'Bunkers')
    that should be filtered out of the country-grain dataset.
    """
    if comtrade_name in COUNTRY_NAME_OVERRIDES:
        mapped = COUNTRY_NAME_OVERRIDES[comtrade_name]
        if mapped is None:
            return None
        comtrade_name = mapped

    try:
        country = pycountry.countries.lookup(comtrade_name)
        return country.alpha_3
    except LookupError:
        return None


def _indian_financial_year(d: date) -> str:
    """India's FY runs April through March. Month >= 4 → FY of that year."""
    if d.month >= 4:
        start, end = d.year, d.year + 1
    else:
        start, end = d.year - 1, d.year
    return f"FY{start % 100:02d}-{end % 100:02d}"


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------


def _flatten_one(payload: dict) -> pd.DataFrame:
    """Flatten one Comtrade JSON payload into a DataFrame."""
    rows = payload.get("data") or []
    if not rows:
        return pd.DataFrame()
    df = pd.json_normalize(rows)
    return df


def _normalise(df: pd.DataFrame, report: CleanReport) -> pd.DataFrame:
    """Apply all cleaning operations.

    Mutates ``report`` in place with diagnostic counters.
    """
    if df.empty:
        return df

    report.rows_in = len(df)

    # ----- 1. Build shipment_date from refYear/refMonth -----
    # Comtrade returns refMonth as the integer month or 52 for "annual" rows.
    # We treat each monthly row as the first of its month.
    df["shipment_date"] = pd.to_datetime(
        dict(
            year=df["refYear"].astype("Int64"),
            month=df["refMonth"].astype("Int64"),
            day=1,
        ),
        errors="coerce",
    )

    # ----- 2. Filter aggregate / nonsense partners -----
    before = len(df)
    df = df[~df["partnerCode"].isin(COMTRADE_AGGREGATE_PARTNERS)].copy()
    report.rows_dropped_aggregate = before - len(df)

    # ----- 3. ISO country mapping -----
    unmapped: set[str] = set()
    iso_codes: list[str | None] = []
    for name in df["partnerDesc"].astype(str):
        iso = _to_iso_alpha3(name)
        iso_codes.append(iso)
        if iso is None and name not in COUNTRY_NAME_OVERRIDES:
            unmapped.add(name)
    df["dest_iso_alpha3"] = iso_codes
    report.unmapped_countries = unmapped or None

    # Drop rows we couldn't map to a real country
    before = len(df)
    df = df[df["dest_iso_alpha3"].notna()].copy()
    report.rows_dropped_aggregate += before - len(df)

    # ----- 4. Rename canonical columns -----
    df = df.rename(
        columns={
            "cmdCode": "hs_code",
            "cmdDesc": "hs_desc",
            "primaryValue": "fob_usd",
            "qty": "quantity_kg",
            "partnerDesc": "dest_country_name",
            "reporterDesc": "reporter_country_name",
        }
    )

    # ----- 5. Type coercions and arithmetic -----
    df["fob_usd"] = pd.to_numeric(df["fob_usd"], errors="coerce")
    df["quantity_kg"] = pd.to_numeric(df["quantity_kg"], errors="coerce")

    # Drop rows with null/zero/negative core values
    before = len(df)
    df = df[
        df["shipment_date"].notna()
        & df["fob_usd"].notna()
        & (df["fob_usd"] > 0)
        & df["quantity_kg"].notna()
        & (df["quantity_kg"] > 0)
    ].copy()
    report.rows_dropped_null = before - len(df)

    # Compute unit price ($/kg)
    df["unit_price_usd"] = (df["fob_usd"] / df["quantity_kg"]).round(4)

    # ----- 6. Calendar features -----
    df["year"] = df["shipment_date"].dt.year.astype("int16")
    df["month"] = df["shipment_date"].dt.month.astype("int8")
    df["quarter"] = df["shipment_date"].dt.quarter.astype("int8")
    df["financial_year"] = df["shipment_date"].dt.date.map(_indian_financial_year)
    df["is_peak_season"] = df["month"].isin(PEAK_SEASON_MONTHS)

    # ----- 7. Deduplicate -----
    dedup_keys = ["shipment_date", "dest_iso_alpha3", "hs_code", "fob_usd"]
    before = len(df)
    df = df.drop_duplicates(subset=dedup_keys, keep="first").copy()
    report.rows_deduped = before - len(df)

    # ----- 8. Outlier flag (per-HS-code 99th percentile) -----
    df["is_outlier"] = False
    for hs, sub in df.groupby("hs_code"):
        if len(sub) < 20:
            continue
        threshold = sub["unit_price_usd"].quantile(0.99)
        df.loc[
            (df["hs_code"] == hs) & (df["unit_price_usd"] > threshold), "is_outlier"
        ] = True
    report.rows_outlier_flagged = int(df["is_outlier"].sum())

    # ----- 9. Source tag -----
    df["source"] = "COMTRADE"

    # ----- 10. Project to canonical column order -----
    canonical_cols = [
        "shipment_date",
        "year",
        "month",
        "quarter",
        "financial_year",
        "is_peak_season",
        "hs_code",
        "hs_desc",
        "dest_iso_alpha3",
        "dest_country_name",
        "reporter_country_name",
        "fob_usd",
        "quantity_kg",
        "unit_price_usd",
        "is_outlier",
        "source",
    ]
    keep = [c for c in canonical_cols if c in df.columns]
    df = (
        df[keep]
        .sort_values(["shipment_date", "hs_code", "dest_iso_alpha3"])
        .reset_index(drop=True)
    )

    report.rows_out = len(df)
    return df


def clean_all(
    raw_dir: Path = RAW_DIR_DEFAULT, out_dir: Path = OUT_DIR_DEFAULT
) -> CleanReport:
    raw_files = _find_latest_raw_files(raw_dir)
    if not raw_files:
        raise FileNotFoundError(f"No comtrade_*.json files found in {raw_dir}")

    report = CleanReport(raw_files_read=len(raw_files))
    log.info("Reading %d raw Comtrade files from %s", len(raw_files), raw_dir)

    frames: list[pd.DataFrame] = []
    for path in raw_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            log.warning("  %-60s  malformed JSON: %s", path.name, exc)
            continue
        df = _flatten_one(payload)
        log.info("  %-60s  %5d rows", path.name, len(df))
        frames.append(df)

    if not frames:
        raise RuntimeError("Every raw file was empty or malformed.")

    # Filter out empty DataFrames before concat — Pandas 2.2 deprecated
    # silent handling of empty/all-NA entries in concat.
    non_empty_frames = [f for f in frames if not f.empty]
    if not non_empty_frames:
        raise RuntimeError("Every raw file was empty.")
    raw = pd.concat(non_empty_frames, ignore_index=True, sort=False)
    cleaned = _normalise(raw, report)

    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / "exports_clean.parquet"
    csv_path = out_dir / "exports_clean.csv"
    cleaned.to_parquet(parquet_path, index=False, compression="snappy")
    cleaned.to_csv(csv_path, index=False)
    log.info(
        "Wrote %s (%d rows, %.1f KB)",
        parquet_path.name,
        len(cleaned),
        parquet_path.stat().st_size / 1024,
    )
    log.info(
        "Wrote %s (%d rows, %.1f KB)",
        csv_path.name,
        len(cleaned),
        csv_path.stat().st_size / 1024,
    )

    report.log()
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR_DEFAULT)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR_DEFAULT)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    clean_all(args.raw_dir, args.out_dir)


if __name__ == "__main__":
    main()
