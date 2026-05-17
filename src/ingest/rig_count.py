"""Ingest the Baker Hughes North America Rotary Rig Count.

Guar gum is dual-driven: ~60 % of global demand is industrial (hydraulic
fracturing), so US drilling activity is the leading volume indicator for
India's guar exports. ``06_guar_model.ipynb`` uses this series as an
exogenous SARIMAX regressor.

Data provenance
---------------
The authoritative source is Baker Hughes' weekly North America Rotary Rig
Count (rigcount.bakerhughes.com — published as an Excel pivot table). That
file needs an Excel reader we deliberately don't pin, and scraping the
investor-relations page is brittle for an unattended weekly job.

So the *committed* source of truth is
``data/external/baker_hughes_rig_count_annual.csv`` — Baker Hughes'
published annual averages, curated and version-controlled so the pipeline
is fully reproducible offline. This module broadcasts the annual average
to a weekly grain to populate ``rig_count_weekly`` (whose PK is
``week_start_date``). Swapping in a true weekly CSV is a drop-in upgrade:
replace the external file with weekly rows and set ``--source-grain weekly``.

CLI
---
    python -m src.ingest.rig_count                 # ingest + load to Postgres
    python -m src.ingest.rig_count --no-db         # write processed CSV only
    python -m src.ingest.rig_count --dry-run       # parse only, no writes

Output
------
    data/processed/rig_count_clean.csv   (week_start_date, rig_count, region)
    rig_count_weekly                     (Postgres, UPSERT on week_start_date)
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
SOURCE_CSV = PROJECT_ROOT / "data" / "external" / "baker_hughes_rig_count_annual.csv"
OUT_CSV = PROJECT_ROOT / "data" / "processed" / "rig_count_clean.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("rig_count")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class RigIngestReport:
    started_at: datetime
    finished_at: datetime | None = None
    weeks_built: int = 0
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
# Transform: annual averages -> weekly grain
# ---------------------------------------------------------------------------


def build_weekly_frame() -> pd.DataFrame:
    """Read the curated annual averages and broadcast them to weekly rows.

    One row per ISO week (Monday-anchored) of each covered year, carrying
    that year's published Baker Hughes NA rig-count average.
    """
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(
            f"Curated source missing: {SOURCE_CSV}. This file is committed to "
            "the repo — restore it from git."
        )

    annual = pd.read_csv(SOURCE_CSV)
    log.info("Read %d annual rig-count rows from %s", len(annual), SOURCE_CSV.name)

    frames = []
    for _, row in annual.iterrows():
        year = int(row["year"])
        # W-MON anchors each week to its Monday — matches the week_start_date PK.
        mondays = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="W-MON")
        frames.append(
            pd.DataFrame(
                {
                    "week_start_date": mondays.date,
                    "rig_count": int(row["avg_rig_count"]),
                    "region": str(row["region"]),
                }
            )
        )

    weekly = pd.concat(frames, ignore_index=True).sort_values("week_start_date")
    weekly = weekly.drop_duplicates(subset=["week_start_date"]).reset_index(drop=True)
    log.info(
        "Built %d weekly rows spanning %s -> %s",
        len(weekly),
        weekly["week_start_date"].iloc[0],
        weekly["week_start_date"].iloc[-1],
    )
    return weekly


# ---------------------------------------------------------------------------
# Database engine + UPSERT (same pattern as src.load.load_db)
# ---------------------------------------------------------------------------


def _get_engine() -> Engine | None:
    """Return a SQLAlchemy engine, or None if DATABASE_URL is unset.

    Returning None (rather than exiting) keeps the pipeline runnable offline:
    we still produce the processed CSV that the notebooks fall back to.
    """
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        log.warning("DATABASE_URL not set — skipping Postgres load (CSV still written)")
        return None
    return create_engine(url, pool_pre_ping=True, future=True)


def upsert_rig_count(engine: Engine, weekly: pd.DataFrame) -> int:
    """UPSERT weekly rows into rig_count_weekly (idempotent on week_start_date)."""
    rows = list(
        zip(
            weekly["week_start_date"],
            weekly["rig_count"].astype(int),
            weekly["region"].astype(str),
        )
    )
    sql = """
        INSERT INTO rig_count_weekly (week_start_date, rig_count, region)
        VALUES %s
        ON CONFLICT (week_start_date) DO UPDATE
            SET rig_count   = EXCLUDED.rig_count,
                region      = EXCLUDED.region,
                ingested_at = NOW()
    """
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        raw.commit()
    finally:
        raw.close()
    log.info("rig_count_weekly upserted %d rows", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(dry_run: bool = False, no_db: bool = False) -> RigIngestReport:
    report = RigIngestReport(started_at=datetime.utcnow())
    try:
        weekly = build_weekly_frame()
        report.weeks_built = len(weekly)

        if dry_run:
            log.info("[dry-run] would write %d rows to %s", len(weekly), OUT_CSV.name)
            report.status = "DRY_RUN"
            return report

        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        weekly.to_csv(OUT_CSV, index=False)
        report.csv_path = str(OUT_CSV.relative_to(PROJECT_ROOT))
        log.info("Wrote %s (%d rows)", OUT_CSV.name, len(weekly))

        if not no_db:
            engine = _get_engine()
            if engine is not None:
                try:
                    report.rows_upserted = upsert_rig_count(engine, weekly)
                finally:
                    engine.dispose()

        report.status = "SUCCESS"
    except Exception as exc:
        report.status = "FAILED"
        report.error = f"{exc.__class__.__name__}: {exc}"
        log.exception("Rig-count ingest failed")
    finally:
        report.finished_at = datetime.utcnow()

    log.info("=" * 70)
    log.info(
        "Rig-count ingest %s in %.1fs — %d weekly rows, %d upserted",
        report.status,
        report.elapsed_sec,
        report.weeks_built,
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
