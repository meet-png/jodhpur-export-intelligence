"""Ingest IMD Rajasthan south-west monsoon rainfall (annual).

Rajasthan grows ~70 % of India's guar. The south-west monsoon (Jun-Sep)
determines the guar crop, so monsoon performance is the supply-side driver
in ``06_guar_model.ipynb``'s SARIMAX model (rig count is the demand-side
driver).

Data provenance
---------------
IMD publishes seasonal rainfall as a percentage of the Long Period Average
(LPA) per sub-division. There is no stable machine API on the free tier, so
the committed source of truth is
``data/external/imd_rajasthan_monsoon.csv`` — curated, version-controlled,
fully reproducible offline. ``rainfall_mm`` is derived from ``lpa_pct``
against Rajasthan's published seasonal LPA of 435.0 mm and is stored
explicitly in the source file (the loader reads it, never recomputes, so
there is no float-rounding drift).

CLI
---
    python -m src.ingest.monsoon                 # ingest + load to Postgres
    python -m src.ingest.monsoon --no-db         # write processed CSV only
    python -m src.ingest.monsoon --dry-run       # parse only, no writes

Output
------
    data/processed/monsoon_clean.csv   (year, state, rainfall_mm, lpa_pct)
    monsoon_yearly                     (Postgres, UPSERT on (year, state))
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_CSV = PROJECT_ROOT / "data" / "external" / "imd_rajasthan_monsoon.csv"
OUT_CSV = PROJECT_ROOT / "data" / "processed" / "monsoon_clean.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("monsoon")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class MonsoonIngestReport:
    started_at: datetime
    finished_at: datetime | None = None
    rows_read: int = 0
    rows_upserted: int = 0
    csv_path: str | None = None
    status: str = "RUNNING"
    error: str | None = None

    @property
    def elapsed_sec(self) -> float:
        if self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def build_frame() -> pd.DataFrame:
    """Read and validate the curated IMD monsoon series."""
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(
            f"Curated source missing: {SOURCE_CSV}. This file is committed to "
            "the repo — restore it from git."
        )

    df = pd.read_csv(SOURCE_CSV)
    log.info("Read %d monsoon rows from %s", len(df), SOURCE_CSV.name)

    required = {"year", "state", "lpa_pct", "rainfall_mm"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Source CSV missing columns: {sorted(missing)}")

    df["year"] = df["year"].astype(int)
    df["state"] = df["state"].astype(str).str.upper()
    df["lpa_pct"] = df["lpa_pct"].astype(float)
    df["rainfall_mm"] = df["rainfall_mm"].astype(float)

    if (df["lpa_pct"] <= 0).any() or (df["rainfall_mm"] <= 0).any():
        raise ValueError("Non-positive lpa_pct/rainfall_mm in source data")

    out = df[["year", "state", "rainfall_mm", "lpa_pct"]].sort_values("year")
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Database engine + UPSERT
# ---------------------------------------------------------------------------


def _get_engine() -> Engine | None:
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        log.warning("DATABASE_URL not set — skipping Postgres load (CSV still written)")
        return None
    return create_engine(url, pool_pre_ping=True, future=True)


def upsert_monsoon(engine: Engine, df: pd.DataFrame) -> int:
    """UPSERT into monsoon_yearly (idempotent on composite PK (year, state))."""
    rows = list(
        zip(
            df["year"].astype(int),
            df["state"].astype(str),
            df["rainfall_mm"].astype(float),
            df["lpa_pct"].astype(float),
        )
    )
    sql = """
        INSERT INTO monsoon_yearly (year, state, rainfall_mm, lpa_pct)
        VALUES %s
        ON CONFLICT (year, state) DO UPDATE
            SET rainfall_mm = EXCLUDED.rainfall_mm,
                lpa_pct     = EXCLUDED.lpa_pct
    """
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        raw.commit()
    finally:
        raw.close()
    log.info("monsoon_yearly upserted %d rows", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(dry_run: bool = False, no_db: bool = False) -> MonsoonIngestReport:
    report = MonsoonIngestReport(started_at=datetime.utcnow())
    try:
        df = build_frame()
        report.rows_read = len(df)

        if dry_run:
            log.info("[dry-run] would write %d rows to %s", len(df), OUT_CSV.name)
            report.status = "DRY_RUN"
            return report

        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUT_CSV, index=False)
        report.csv_path = str(OUT_CSV.relative_to(PROJECT_ROOT))
        log.info("Wrote %s (%d rows)", OUT_CSV.name, len(df))

        if not no_db:
            engine = _get_engine()
            if engine is not None:
                try:
                    report.rows_upserted = upsert_monsoon(engine, df)
                finally:
                    engine.dispose()

        report.status = "SUCCESS"
    except Exception as exc:
        report.status = "FAILED"
        report.error = f"{exc.__class__.__name__}: {exc}"
        log.exception("Monsoon ingest failed")
    finally:
        report.finished_at = datetime.utcnow()

    log.info("=" * 70)
    log.info(
        "Monsoon ingest %s in %.1fs — %d rows read, %d upserted",
        report.status,
        report.elapsed_sec,
        report.rows_read,
        report.rows_upserted,
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no writes")
    parser.add_argument(
        "--no-db", action="store_true", help="Write processed CSV but skip Postgres"
    )
    args = parser.parse_args()

    report = run(dry_run=args.dry_run, no_db=args.no_db)
    sys.exit(0 if report.status in ("SUCCESS", "DRY_RUN") else 1)


if __name__ == "__main__":
    main()
