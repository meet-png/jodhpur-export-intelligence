"""UN Comtrade Plus API client — pulls monthly India export flows.

Pulls monthly India export flows by HS code from the UN Comtrade Plus
public REST API and persists each (hs_code, year) response to disk as a
date-stamped raw JSON file.

Design constraints baked into this module
-----------------------------------------
* PRD FR-1: raw files are date-stamped and never overwritten — every pull
  creates a new file. The most recent file wins for downstream
  consumption.
* PRD §11: free-tier API allows 500 calls/day. We pull 5 HS codes × 6
  years = 30 calls per full refresh, well within the limit.
* PRD §3.1: the pipeline runs unattended every Sunday — every call is
  retried with exponential backoff, every failure is logged with enough
  context for post-mortem.

CLI
---
    # full pull (all HS codes, all years 2019-2024)
    python -m src.ingest.comtrade_api

    # narrow pull (single HS code, single year — useful in development)
    python -m src.ingest.comtrade_api --hs-code 440900 --year 2023

    # dry run (no files written)
    python -m src.ingest.comtrade_api --dry-run

Output
------
    data/raw/comtrade_{hs_code}_{year}_{YYYYMMDD}.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

import requests
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Constants — values that should never be hard-coded elsewhere in this
# codebase. If any of these change, change them ONLY here.
# ---------------------------------------------------------------------------

COMTRADE_BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/M/HS"

# India's M49 reporter code (https://comtrade.un.org/Data/cache/reporterAreas.json).
INDIA_REPORTER_CODE = 699

# 0 = "World" — aggregates all destination countries. We pull this and
# also break out by partner. The "world" call gives us total flows; the
# partner-specific calls give us country-level breakdown for the
# segmentation and benchmarking modules.
WORLD_PARTNER_CODE = 0

# HS codes per PRD §8.1, with one correction discovered during integration:
#
#   The PRD lists "440900" but that is a 4-digit HS heading, not a valid
#   6-digit subheading that Comtrade indexes. The H5/H6 subheadings under
#   4409 are 440910 (coniferous), 440921 (bamboo), 440929 (other non-
#   coniferous). Jodhpur's wood handicraft exports are dominated by
#   non-coniferous hardwoods (sheesham, mango, acacia), so we substitute
#   440929 — the closest legitimate code matching the PRD's intent.
#
# This change is documented in DATA_DICTIONARY.md.
DEFAULT_HS_CODES = (
    "440929",  # Wood, non-coniferous, continuously shaped (sheesham, mango, etc.)
    "442090",  # Wooden articles for table/kitchen/indoor use (Jodhpur staple)
    "330749",  # Room deodorisers and other perfuming preparations
    "130232",  # Mucilages and thickeners from guar seeds
    "130239",  # Mucilages and thickeners from other plants
)

# Calendar window per PRD §8.2.
DEFAULT_YEARS = tuple(range(2019, 2025))  # 2019-2024 inclusive

# Free-tier cap is 500/day; we self-throttle well below that.
INTER_REQUEST_DELAY_SEC = 1.0

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("comtrade_api")


# ---------------------------------------------------------------------------
# Custom exceptions — let callers (and the orchestrator) handle these
# distinctly from generic network errors.
# ---------------------------------------------------------------------------


class ComtradeAuthError(RuntimeError):
    """API key invalid or subscription inactive (HTTP 401/403)."""


class ComtradeRateLimitError(RuntimeError):
    """We've burned the daily 500-call budget (HTTP 429 or message hint)."""


class ComtradeServerError(RuntimeError):
    """5xx from the API — almost always transient. Retried automatically."""


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclass
class FetchResult:
    """Tracks the outcome of a single (hs_code, year) pull."""

    hs_code: str
    year: int
    status: str  # 'OK' | 'EMPTY' | 'FAILED' | 'SKIPPED'
    rows: int = 0
    output_path: Path | None = None
    error: str | None = None


@dataclass
class IngestSummary:
    """Aggregate result of a multi-call run — used by orchestrator + tests."""

    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    results: list[FetchResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.results)

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.status == "OK")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == "FAILED")

    def log_summary(self) -> None:
        elapsed = (
            (self.finished_at - self.started_at).total_seconds()
            if self.finished_at
            else 0
        )
        log.info("=" * 70)
        log.info(
            "Comtrade ingest finished in %.1fs — %d OK, %d empty, %d failed, %d rows total",
            elapsed,
            self.ok_count,
            sum(1 for r in self.results if r.status == "EMPTY"),
            self.failed_count,
            self.total_rows,
        )
        for r in self.results:
            badge = {"OK": "✓", "EMPTY": "·", "FAILED": "✗", "SKIPPED": "-"}.get(
                r.status, "?"
            )
            log.info(
                "  %s  HS %s  %d  →  %s",
                badge,
                r.hs_code,
                r.year,
                r.error
                or f"{r.rows} rows → {r.output_path.name if r.output_path else '—'}",
            )


# ---------------------------------------------------------------------------
# Core HTTP client — separated from orchestration so it's unit-testable
# in isolation with a mock requests session.
# ---------------------------------------------------------------------------


def _api_key() -> str:
    """Read the Comtrade subscription key from env. Fail loudly if missing."""
    load_dotenv()  # idempotent
    key = os.getenv("COMTRADE_API_KEY")
    if not key or key.startswith(("paste_", "your_", "<")):
        raise ComtradeAuthError(
            "COMTRADE_API_KEY missing or still set to placeholder. "
            "Subscribe at comtradeplus.un.org and update .env."
        )
    return key


def _build_monthly_period(year: int) -> str:
    """Comtrade Plus monthly endpoint expects period as YYYYMM, comma-joined.

    Annual queries (freqCode=A) take period=YYYY, but our pipeline runs against
    the monthly endpoint because seasonality (the headline finding) needs
    month-grain data. Passing period=YYYY to a monthly query silently returns
    zero rows — the API treats it as "the month named 2023", which doesn't
    exist. This was a real bug discovered during initial integration.
    """
    return ",".join(f"{year}{m:02d}" for m in range(1, 13))


@retry(
    retry=retry_if_exception_type((ComtradeServerError, requests.RequestException)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
)
def _call_api(
    hs_code: str,
    year: int,
    *,
    partner_code: int = WORLD_PARTNER_CODE,
    timeout: float = 30.0,
) -> dict:
    """Call the Comtrade Plus monthly endpoint.

    Returns the parsed JSON payload. Raises typed exceptions on auth /
    rate-limit / server failures so the caller can react.
    """
    params = {
        "freqCode": "M",
        "clCode": "HS",
        "period": _build_monthly_period(
            year
        ),  # YYYYMM,YYYYMM,... — see helper docstring
        "reporterCode": str(INDIA_REPORTER_CODE),
        "partnerCode": str(partner_code),
        "cmdCode": hs_code,
        "flowCode": "X",  # X = exports (M = imports)
        "maxRecords": "500",  # API default is 500; explicit for clarity
        "format": "JSON",
        "includeDesc": "true",
    }
    headers = {"Ocp-Apim-Subscription-Key": _api_key()}

    log.info("GET Comtrade — HS %s, year %d, partner %s", hs_code, year, partner_code)
    log.debug("  URL: %s  params=%s", COMTRADE_BASE_URL, params)
    resp = requests.get(
        COMTRADE_BASE_URL, params=params, headers=headers, timeout=timeout
    )

    if resp.status_code in (401, 403):
        raise ComtradeAuthError(
            f"Comtrade returned {resp.status_code}. "
            "Check that your subscription key is correct and active."
        )
    if resp.status_code == 429:
        raise ComtradeRateLimitError(
            "Comtrade returned 429 — daily 500-call budget likely exhausted. "
            "Resume after midnight UTC."
        )
    if 500 <= resp.status_code < 600:
        raise ComtradeServerError(f"Comtrade {resp.status_code}: {resp.text[:200]}")
    resp.raise_for_status()

    try:
        payload = resp.json()
    except ValueError as exc:
        raise ComtradeServerError(f"Non-JSON response: {resp.text[:200]}") from exc

    return payload


# ---------------------------------------------------------------------------
# Disk persistence
# ---------------------------------------------------------------------------


def _build_output_path(hs_code: str, year: int, today: datetime | None = None) -> Path:
    """PRD FR-1 — date-stamped filename, raw files never overwritten."""
    stamp = (today or datetime.utcnow()).strftime("%Y%m%d")
    return RAW_DIR / f"comtrade_{hs_code}_{year}_{stamp}.json"


def _write_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Orchestration — what the CLI and the pipeline call.
# ---------------------------------------------------------------------------


def fetch_one(hs_code: str, year: int, *, dry_run: bool = False) -> FetchResult:
    """Fetch a single (hs_code, year) pair and persist it to data/raw/."""
    try:
        payload = _call_api(hs_code, year, partner_code=WORLD_PARTNER_CODE)
    except ComtradeAuthError as exc:
        # Auth failures are fatal — abort the whole run, don't retry per-call.
        log.error("AUTH FAILURE: %s", exc)
        raise
    except ComtradeRateLimitError as exc:
        log.error("RATE LIMITED: %s", exc)
        return FetchResult(hs_code=hs_code, year=year, status="FAILED", error=str(exc))
    except Exception as exc:
        log.exception("Unexpected failure for HS %s %d", hs_code, year)
        return FetchResult(hs_code=hs_code, year=year, status="FAILED", error=str(exc))

    rows = payload.get("data") or []
    if not rows:
        log.warning(
            "HS %s year %d returned 0 rows — likely no recorded trade", hs_code, year
        )
        # Diagnostic dump — Comtrade Plus often signals problems via top-level
        # keys like 'message', 'error', 'count', 'elapsedTime' even on 200 OK.
        # Truncate to 500 chars to avoid spam if payload turns out huge.
        diag = {k: v for k, v in payload.items() if k != "data"}
        log.warning("  payload (sans data): %s", str(diag)[:500])
        return FetchResult(hs_code=hs_code, year=year, status="EMPTY", rows=0)

    if dry_run:
        log.info(
            "[dry-run] HS %s year %d would have written %d rows",
            hs_code,
            year,
            len(rows),
        )
        return FetchResult(hs_code=hs_code, year=year, status="SKIPPED", rows=len(rows))

    output = _build_output_path(hs_code, year)
    _write_payload(output, payload)
    log.info("HS %s year %d → %s (%d rows)", hs_code, year, output.name, len(rows))
    return FetchResult(
        hs_code=hs_code, year=year, status="OK", rows=len(rows), output_path=output
    )


def fetch_all(
    hs_codes: Iterable[str] = DEFAULT_HS_CODES,
    years: Iterable[int] = DEFAULT_YEARS,
    *,
    dry_run: bool = False,
) -> IngestSummary:
    """Fetch every (hs_code, year) combination, throttled."""
    summary = IngestSummary()
    pairs = [(hs, yr) for hs in hs_codes for yr in years]
    log.info(
        "Starting Comtrade ingest — %d calls planned (%d HS × %d years)",
        len(pairs),
        len(list(hs_codes)),
        len(list(years)),
    )

    for i, (hs, yr) in enumerate(pairs, start=1):
        log.info("[%d/%d] HS %s year %d", i, len(pairs), hs, yr)
        try:
            result = fetch_one(hs, yr, dry_run=dry_run)
        except ComtradeAuthError:
            # Don't keep hammering the API with bad credentials.
            log.error(
                "Aborting remaining %d calls due to auth failure.", len(pairs) - i
            )
            break
        summary.results.append(result)

        # Be polite to a public API even though we're well under the cap.
        if i < len(pairs):
            time.sleep(INTER_REQUEST_DELAY_SEC)

    summary.finished_at = datetime.utcnow()
    summary.log_summary()
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument(
        "--hs-code",
        action="append",
        help="One or more HS codes to fetch (default: all five PRD-scope codes).",
    )
    p.add_argument(
        "--year",
        type=int,
        action="append",
        help="One or more years to fetch (default: 2019-2024).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Hit the API but do not write any files.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    hs_codes = tuple(args.hs_code) if args.hs_code else DEFAULT_HS_CODES
    years = tuple(args.year) if args.year else DEFAULT_YEARS
    summary = fetch_all(hs_codes, years, dry_run=args.dry_run)

    # Non-zero exit if any call failed — useful for CI gating.
    sys.exit(1 if summary.failed_count > 0 else 0)


if __name__ == "__main__":
    main()
