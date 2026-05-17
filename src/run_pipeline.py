"""End-to-end ETL orchestrator for the Jodhpur Export Intelligence System.

Runs the full weekly refresh as a simple, ordered, procedural script. The
PRD deliberately rules out Airflow: the refresh is linear and finishes in
<15 min, so a scheduler's complexity buys nothing.

Pipeline stages
---------------
  1. ingest.comtrade    pull India monthly export flows. Auto-skipped if
                         COMTRADE_API_KEY is unset (the existing date-stamped
                         raw files are reused) or with --skip-ingest.
  2. ingest.rig_count    Baker Hughes NA rig count  (guar DEMAND regressor)
  3. ingest.monsoon      IMD Rajasthan monsoon      (guar SUPPLY regressor)
  4. transform.clean     flatten raw JSON -> exports_clean.parquet + .csv
  5. transform.validate  20-expectation quality gate. HARD HALT on any
                         failure — PRD FR-2: never load bad numbers.
  6. load.init_db        apply star-schema DDL (idempotent). Skipped if
                         DATABASE_URL is unset.
  7. load.load_db        TRUNCATE + bulk load fact_shipment + dimensions.
                         Skipped if DATABASE_URL is unset.

Halt-on-failure
---------------
The first stage that fails stops the pipeline immediately with a non-zero
exit code. Stages 6-7 are *skipped* (not failed) when DATABASE_URL is
absent, so the pipeline still produces a validated dataset offline — the
shape a recruiter can run without a Supabase account.

CLI
---
    python -m src.run_pipeline                 # full refresh
    python -m src.run_pipeline --skip-ingest   # reuse existing raw + external data
    python -m src.run_pipeline --dry-run       # everything except the DB write
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_pipeline")

# Statuses
OK = "OK"
SKIPPED = "SKIPPED"
FAILED = "FAILED"

_PLACEHOLDER_PREFIXES = ("paste_", "your_", "<", "changeme")


@dataclass
class StageResult:
    name: str
    status: str
    detail: str = ""
    elapsed_sec: float = 0.0


@dataclass
class PipelineResult:
    started_at: datetime = field(default_factory=datetime.utcnow)
    stages: list[StageResult] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return any(s.status == FAILED for s in self.stages)

    def summary(self) -> None:
        total = sum(s.elapsed_sec for s in self.stages)
        log.info("=" * 72)
        log.info("PIPELINE SUMMARY")
        for s in self.stages:
            badge = {OK: "[ OK ]", SKIPPED: "[SKIP]", FAILED: "[FAIL]"}[s.status]
            log.info("  %s  %-22s  %6.1fs  %s", badge, s.name, s.elapsed_sec, s.detail)
        log.info("-" * 72)
        log.info(
            "  %s in %.1fs",
            "FAILED" if self.failed else "SUCCESS",
            total,
        )
        log.info("=" * 72)


def _has_real_value(key: str) -> bool:
    """True if env var is set and not a copy-pasted placeholder."""
    val = os.getenv(key)
    return bool(val) and not val.startswith(_PLACEHOLDER_PREFIXES)


def _run_stage(result: PipelineResult, name: str, fn) -> bool:
    """Execute one stage. Returns True to continue, False to halt.

    ``fn`` returns (status, detail). Any exception is treated as FAILED.
    """
    log.info("-" * 72)
    log.info(">>> STAGE: %s", name)
    t0 = time.perf_counter()
    try:
        status, detail = fn()
    except Exception as exc:
        status, detail = FAILED, f"{exc.__class__.__name__}: {exc}"
        log.exception("Stage %s raised", name)
    elapsed = time.perf_counter() - t0
    result.stages.append(StageResult(name, status, detail, elapsed))
    log.info("<<< %s: %s (%.1fs) %s", name, status, elapsed, detail)
    return status != FAILED


# ---------------------------------------------------------------------------
# Stage implementations — each returns (status, detail)
# ---------------------------------------------------------------------------


def _stage_comtrade(skip_ingest: bool):
    if skip_ingest:
        return SKIPPED, "--skip-ingest: reusing existing raw files"
    if not _has_real_value("COMTRADE_API_KEY"):
        return SKIPPED, "COMTRADE_API_KEY unset — reusing existing raw files"
    from src.ingest import comtrade_api

    summary = comtrade_api.fetch_all()
    detail = (
        f"{summary.ok_count} OK, {summary.failed_count} failed, "
        f"{summary.total_rows} rows"
    )
    # Mirror comtrade_api.main()'s own contract: any failed call is a failure.
    return (FAILED if summary.failed_count > 0 else OK), detail


def _stage_rig_count(skip_ingest: bool, dry_run: bool):
    if skip_ingest:
        return SKIPPED, "--skip-ingest"
    from src.ingest import rig_count

    rep = rig_count.run(dry_run=dry_run)
    ok = rep.status in ("SUCCESS", "DRY_RUN")
    return (OK if ok else FAILED), f"{rep.weeks_built} weekly rows ({rep.status})"


def _stage_monsoon(skip_ingest: bool, dry_run: bool):
    if skip_ingest:
        return SKIPPED, "--skip-ingest"
    from src.ingest import monsoon

    rep = monsoon.run(dry_run=dry_run)
    ok = rep.status in ("SUCCESS", "DRY_RUN")
    return (OK if ok else FAILED), f"{rep.rows_read} rows ({rep.status})"


def _stage_clean():
    from src.transform import clean

    rep = clean.clean_all()
    if rep.rows_out <= 0:
        return FAILED, "clean produced 0 rows"
    return OK, f"{rep.rows_out} rows -> exports_clean.parquet"


def _stage_validate():
    from src.transform import validate

    rep = validate.validate()
    detail = f"{rep.success_count} passed, {rep.failure_count} failed"
    # PRD FR-2: a single failed expectation halts the pipeline.
    return (OK if rep.overall_success else FAILED), detail


def _stage_init_db(dry_run: bool):
    if not _has_real_value("DATABASE_URL"):
        return SKIPPED, "DATABASE_URL unset"
    if dry_run:
        return SKIPPED, "--dry-run: schema apply skipped"
    from src.load import init_db

    engine = init_db.get_engine()
    try:
        init_db.apply_schema(engine)
    finally:
        engine.dispose()
    return OK, "schema applied (idempotent)"


def _stage_load_db(dry_run: bool):
    if not _has_real_value("DATABASE_URL"):
        return SKIPPED, "DATABASE_URL unset"
    from src.load import load_db

    rep = load_db.run(dry_run=dry_run)
    ok = rep.status in ("SUCCESS", "DRY_RUN")
    return (OK if ok else FAILED), f"{rep.fact_rows_loaded} fact rows ({rep.status})"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(skip_ingest: bool = False, dry_run: bool = False) -> PipelineResult:
    load_dotenv()
    result = PipelineResult()
    log.info("=" * 72)
    log.info("JEIS pipeline starting — skip_ingest=%s dry_run=%s", skip_ingest, dry_run)

    stages = [
        ("ingest.comtrade", lambda: _stage_comtrade(skip_ingest)),
        ("ingest.rig_count", lambda: _stage_rig_count(skip_ingest, dry_run)),
        ("ingest.monsoon", lambda: _stage_monsoon(skip_ingest, dry_run)),
        ("transform.clean", _stage_clean),
        ("transform.validate", _stage_validate),
        ("load.init_db", lambda: _stage_init_db(dry_run)),
        ("load.load_db", lambda: _stage_load_db(dry_run)),
    ]

    for name, fn in stages:
        if not _run_stage(result, name, fn):
            log.error("Halting pipeline — stage %s failed.", name)
            break

    result.summary()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip all ingestion; reuse existing raw + external data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run every stage except the destructive DB write.",
    )
    args = parser.parse_args()

    result = run(skip_ingest=args.skip_ingest, dry_run=args.dry_run)
    sys.exit(1 if result.failed else 0)


if __name__ == "__main__":
    main()
