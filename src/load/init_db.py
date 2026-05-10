"""Apply the JEIS star-schema DDL to the configured Postgres instance.

Reads ``DATABASE_URL`` from the environment (loaded via ``.env``), connects
with SQLAlchemy, and executes ``src/load/schema.sql`` as a single transaction.

Idempotent: ``schema.sql`` uses ``CREATE TABLE IF NOT EXISTS`` everywhere, so
re-running this script is safe and produces no duplicate objects.

Usage
-----
    python -m src.load.init_db                  # apply schema
    python -m src.load.init_db --check          # report row counts only
    python -m src.load.init_db --drop-first     # DANGER: drop & recreate
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("init_db")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# Tables we expect to exist after schema.sql runs successfully.
EXPECTED_TABLES = (
    "dim_country",
    "dim_product",
    "dim_company",
    "dim_time",
    "fact_shipment",
    "rig_count_weekly",
    "monsoon_yearly",
    "pipeline_run",
)


def get_engine() -> Engine:
    """Build a SQLAlchemy engine from DATABASE_URL.

    Supabase hands out URLs starting with ``postgresql://`` which SQLAlchemy
    accepts but maps to the legacy ``psycopg2`` driver — that's exactly what
    we want (we pinned ``psycopg2-binary`` in requirements.txt to avoid the
    Windows compile dance with ``psycopg`` v3).
    """
    load_dotenv()  # idempotent; safe even if .env doesn't exist
    url = os.getenv("DATABASE_URL")
    if not url:
        log.error(
            "DATABASE_URL is not set. Copy .env.example to .env and fill in "
            "your Supabase connection string."
        )
        sys.exit(1)

    # Hide the password when we log the destination
    safe_url = url.split("@", 1)[-1] if "@" in url else url
    log.info("Connecting to %s", safe_url)

    # `pool_pre_ping` saves us from the classic "MySQL has gone away"-style
    # error when Supabase's free tier pauses an idle connection.
    return create_engine(url, pool_pre_ping=True, future=True)


def apply_schema(engine: Engine) -> None:
    """Execute the bundled schema.sql against the target database.

    Multi-statement DDL doesn't play nicely with SQLAlchemy 2.0's
    ``text()`` or ``exec_driver_sql()`` — both end up passing an
    ``immutabledict`` to psycopg2's cursor.execute, which raises
    ``TypeError: immutabledict is not a sequence`` even though no
    parameters are intended.

    Workaround: grab the raw psycopg2 connection from the engine and
    call ``cursor.execute(sql)`` directly. psycopg2 natively supports
    multi-statement scripts (it ships them to the server in one batch).
    """
    if not SCHEMA_PATH.exists():
        log.error("schema.sql not found at %s", SCHEMA_PATH)
        sys.exit(2)

    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    log.info("Applying schema from %s (%d bytes)", SCHEMA_PATH.name, len(sql))

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()
        raw_conn.commit()
        log.info("Schema applied successfully.")
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def report_table_status(engine: Engine) -> None:
    """Print row counts for every expected table — used as a smoke test."""
    log.info("Verifying object existence and reporting row counts:")
    with engine.connect() as conn:
        for tbl in EXPECTED_TABLES:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar_one()
                log.info("  %-22s %10d rows", tbl, count)
            except SQLAlchemyError as exc:
                log.error("  %-22s MISSING (%s)", tbl, exc.__class__.__name__)


def drop_all(engine: Engine) -> None:
    """Drop the JEIS schema. Used only with explicit --drop-first."""
    log.warning("Dropping all JEIS tables — this is destructive.")
    drop_sql = "\n".join(
        f"DROP TABLE IF EXISTS {tbl} CASCADE;" for tbl in reversed(EXPECTED_TABLES)
    )
    drop_sql += "\nDROP VIEW IF EXISTS v_monthly_export CASCADE;"
    drop_sql += "\nDROP VIEW IF EXISTS v_buyer_risk_register CASCADE;"
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
    log.warning("Drop complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Skip schema apply; only report current table row counts.",
    )
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="DROP existing JEIS tables before applying. Destructive.",
    )
    args = parser.parse_args()

    engine = get_engine()

    if args.check:
        report_table_status(engine)
        return

    if args.drop_first:
        drop_all(engine)

    apply_schema(engine)
    report_table_status(engine)


if __name__ == "__main__":
    main()
