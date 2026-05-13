"""Load the cleaned dataset into Supabase Postgres.

Reads ``data/processed/exports_clean.parquet`` and populates the JEIS star
schema in the Postgres instance referenced by ``DATABASE_URL``:

* ``dim_time``       — pre-populated calendar 2018-2030 (idempotent)
* ``dim_country``    — UPSERT one row per unique ISO alpha-3 destination
* ``dim_product``    — UPSERT one row per unique HS code
* ``fact_shipment``  — TRUNCATE + bulk INSERT (full weekly refresh)
* ``pipeline_run``   — audit row capturing this run's outcome

Idempotency strategy
--------------------
* **Dimensions** UPSERT via ``INSERT ... ON CONFLICT DO UPDATE``. Re-running
  the loader against the same data is a no-op.
* **fact_shipment** is fully replaced on each run. Comtrade data is a
  monotonically refreshed snapshot — there's no concept of "new shipments
  arriving between runs" at the row grain we use (monthly country totals).
  Truncate + reinsert is simpler, faster, and guarantees the table
  matches the most recent clean output. Historical reconstruction is
  possible from the date-stamped raw files in ``data/raw/`` (PRD FR-1).

CLI
---
    python -m src.load.load_db                # full load
    python -m src.load.load_db --dry-run      # parse + FK-resolve, no write
    python -m src.load.load_db --skip-truncate  # append instead of replace

Why bulk INSERT and not pandas.to_sql under the hood?
-----------------------------------------------------
For 12k rows, ``to_sql`` works but issues thousands of individual INSERT
statements over the wire — minutes against Supabase free tier. We build
one big multi-VALUES statement (psycopg2 ``execute_values``) so the
entire fact load is ~3-5 seconds.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "exports_clean.parquet"

CALENDAR_START = date(2018, 1, 1)
CALENDAR_END = date(2030, 12, 31)
PEAK_SEASON_MONTHS = (9, 10, 11)

# HS code → human description + category mapping (PRD §8.1, with 440929 fix).
HS_CATALOG = {
    "440929": ("Wood non-coniferous continuously shaped", "Handicraft-Wood"),
    "442090": ("Wooden articles for table/kitchen/indoor", "Handicraft-Wood"),
    "330749": ("Room deodorisers and perfuming preparations", "Handicraft-Fragrance"),
    "130232": ("Mucilages and thickeners from guar seeds", "Guar-Refined"),
    "130239": ("Mucilages and thickeners from other plants", "Guar-Other"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load_db")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class LoadReport:
    started_at: datetime
    finished_at: datetime | None = None
    fact_rows_loaded: int = 0
    dim_country_upserts: int = 0
    dim_product_upserts: int = 0
    dim_time_rows: int = 0
    status: str = "RUNNING"
    error: str | None = None

    @property
    def elapsed_sec(self) -> float:
        if self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def get_engine() -> Engine:
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        log.error("DATABASE_URL not set in .env")
        sys.exit(1)
    return create_engine(url, pool_pre_ping=True, future=True)


# ---------------------------------------------------------------------------
# dim_time — pre-populate full calendar window
# ---------------------------------------------------------------------------


def populate_dim_time(engine: Engine) -> int:
    """Insert all dates in the calendar window (idempotent)."""
    sql = """
        INSERT INTO dim_time (time_id, date, year, month, quarter, financial_year, is_peak_season, iso_week)
        VALUES %s
        ON CONFLICT (time_id) DO NOTHING
    """

    rows = []
    d = CALENDAR_START
    while d <= CALENDAR_END:
        time_id = int(d.strftime("%Y%m%d"))
        # Indian FY: April–March
        if d.month >= 4:
            fy_start, fy_end = d.year, d.year + 1
        else:
            fy_start, fy_end = d.year - 1, d.year
        fy = f"FY{fy_start % 100:02d}-{fy_end % 100:02d}"
        quarter = (d.month - 1) // 3 + 1
        rows.append(
            (
                time_id,
                d,
                d.year,
                d.month,
                quarter,
                fy,
                d.month in PEAK_SEASON_MONTHS,
                d.isocalendar().week,
            )
        )
        d += timedelta(days=1)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        raw.commit()
    finally:
        raw.close()

    log.info(
        "dim_time populated (%d candidate dates, ON CONFLICT skipped existing)",
        len(rows),
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Dimension UPSERTs
# ---------------------------------------------------------------------------


def upsert_dim_country(engine: Engine, df: pd.DataFrame) -> int:
    """One row per unique destination country observed in the clean data."""
    countries = (
        df[["dest_iso_alpha3", "dest_country_name"]]
        .drop_duplicates(subset=["dest_iso_alpha3"])
        .dropna()
    )
    rows = list(countries.itertuples(index=False, name=None))

    sql = """
        INSERT INTO dim_country (iso_alpha3, country_name)
        VALUES %s
        ON CONFLICT (iso_alpha3) DO UPDATE
            SET country_name = EXCLUDED.country_name,
                last_updated = NOW()
    """
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        raw.commit()
    finally:
        raw.close()

    log.info("dim_country upserted %d rows", len(rows))
    return len(rows)


def upsert_dim_product(engine: Engine) -> int:
    """One row per HS code in the PRD scope."""
    rows = [(hs, desc, cat) for hs, (desc, cat) in HS_CATALOG.items()]

    sql = """
        INSERT INTO dim_product (hs_code, hs_desc, category)
        VALUES %s
        ON CONFLICT (hs_code) DO UPDATE
            SET hs_desc = EXCLUDED.hs_desc,
                category = EXCLUDED.category,
                last_updated = NOW()
    """
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows)
        raw.commit()
    finally:
        raw.close()

    log.info("dim_product upserted %d rows", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Fact load
# ---------------------------------------------------------------------------


def resolve_foreign_keys(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Look up country_id, product_id, time_id for every row in df."""
    log.info("Resolving FK references...")
    with engine.connect() as conn:
        countries = pd.read_sql(
            text("SELECT country_id, iso_alpha3 FROM dim_country"), conn
        )
        products = pd.read_sql(
            text("SELECT product_id, hs_code FROM dim_product"), conn
        )

    df = df.merge(
        countries, left_on="dest_iso_alpha3", right_on="iso_alpha3", how="left"
    )
    df = df.merge(products, on="hs_code", how="left")
    # time_id is deterministic from shipment_date (YYYYMMDD as int)
    df["time_id"] = df["shipment_date"].dt.strftime("%Y%m%d").astype(int)

    missing_country = df["country_id"].isna().sum()
    missing_product = df["product_id"].isna().sum()
    if missing_country or missing_product:
        raise RuntimeError(
            f"FK resolution failed — missing country_id: {missing_country}, "
            f"missing product_id: {missing_product}. Check dim table population."
        )

    return df


def load_fact_shipment(engine: Engine, df: pd.DataFrame, *, truncate: bool) -> int:
    """Bulk-insert into fact_shipment via execute_values."""
    if truncate:
        log.info("Truncating fact_shipment...")
        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.execute("TRUNCATE TABLE fact_shipment RESTART IDENTITY CASCADE")
            raw.commit()
        finally:
            raw.close()

    cols = (
        "shipment_date",
        "time_id",
        "product_id",
        "dest_country_id",
        "fob_usd",
        "quantity_kg",
        "unit_price_usd",
        "is_outlier",
        "source",
    )
    sql = f"""
        INSERT INTO fact_shipment ({", ".join(cols)})
        VALUES %s
    """
    # Build the row tuples in the exact column order.
    rows = list(
        zip(
            df["shipment_date"].dt.date,
            df["time_id"],
            df["product_id"],
            df["country_id"],
            df["fob_usd"],
            df["quantity_kg"],
            df["unit_price_usd"],
            df["is_outlier"],
            df["source"],
        )
    )

    log.info("Inserting %d rows into fact_shipment...", len(rows))
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        raw.commit()
    finally:
        raw.close()

    return len(rows)


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


def _current_git_sha() -> str | None:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=PROJECT_ROOT,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        return None


def log_pipeline_run(engine: Engine, report: LoadReport) -> None:
    sql = """
        INSERT INTO pipeline_run (
            started_at, finished_at, status,
            rows_ingested, rows_loaded, failed_step, error_message, git_sha
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    git_sha = _current_git_sha()
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(
                sql,
                (
                    report.started_at,
                    report.finished_at,
                    report.status,
                    report.fact_rows_loaded,
                    report.fact_rows_loaded,
                    None if report.status == "SUCCESS" else "load_db",
                    report.error,
                    git_sha,
                ),
            )
        raw.commit()
    finally:
        raw.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(dry_run: bool = False, skip_truncate: bool = False) -> LoadReport:
    started = datetime.utcnow()
    report = LoadReport(started_at=started)

    if not PARQUET_PATH.exists():
        report.status = "FAILED"
        report.error = f"parquet not found: {PARQUET_PATH}"
        report.finished_at = datetime.utcnow()
        log.error(report.error)
        return report

    df = pd.read_parquet(PARQUET_PATH)
    log.info("Loaded %d rows from %s", len(df), PARQUET_PATH.name)

    engine = get_engine()

    try:
        report.dim_time_rows = populate_dim_time(engine)
        report.dim_country_upserts = upsert_dim_country(engine, df)
        report.dim_product_upserts = upsert_dim_product(engine)

        df = resolve_foreign_keys(df, engine)

        if dry_run:
            log.info("[dry-run] Would have loaded %d fact rows.", len(df))
            report.fact_rows_loaded = 0
            report.status = "DRY_RUN"
        else:
            report.fact_rows_loaded = load_fact_shipment(
                engine, df, truncate=not skip_truncate
            )
            report.status = "SUCCESS"

    except Exception as exc:
        report.status = "FAILED"
        report.error = f"{exc.__class__.__name__}: {exc}"
        log.exception("Load failed")

    finally:
        report.finished_at = datetime.utcnow()
        if not dry_run:
            try:
                log_pipeline_run(engine, report)
            except Exception:
                log.exception("Failed to write pipeline_run audit row")
        engine.dispose()

    log.info("=" * 70)
    log.info(
        "Load %s in %.1fs — %d fact rows, %d country upserts, %d product upserts",
        report.status,
        report.elapsed_sec,
        report.fact_rows_loaded,
        report.dim_country_upserts,
        report.dim_product_upserts,
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip-truncate",
        action="store_true",
        help="Append to fact_shipment instead of TRUNCATE+INSERT",
    )
    args = parser.parse_args()

    report = run(dry_run=args.dry_run, skip_truncate=args.skip_truncate)
    sys.exit(0 if report.status in ("SUCCESS", "DRY_RUN") else 1)


if __name__ == "__main__":
    main()
