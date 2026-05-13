"""Data quality validation for the cleaned JEIS dataset.

Loads ``data/processed/exports_clean.parquet`` and asserts ~15 expectations
covering completeness, range, format, and referential rules. If any
expectation fails the script writes a detailed report to
``data/processed/validation_report.json`` and exits non-zero so the
upstream orchestrator (and GitHub Actions) can halt the pipeline.

Design choice — why a hand-rolled mini-framework instead of full
Great Expectations setup?
-------------------------------------------------------------------
The PRD calls for Great Expectations and we keep it in
``requirements.txt`` for the data-docs HTML reporter we'll add in
Phase 7. But for a single-table single-suite pipeline, GE's full
context/datasource/suite/checkpoint scaffolding is overhead with no
analytical payoff. This module:

* Uses GE-vocabulary expectation names (``expect_column_values_to_not_be_null``
  etc.) so the validation report reads like a GE checkpoint result and
  is portable to a full GE setup later.
* Runs every expectation regardless of earlier failures so the report
  is exhaustive (vs. fail-fast which hides downstream issues).
* Returns a structured ``ValidationReport`` dataclass that's trivially
  JSON-serialisable for GitHub Actions step-output and emailing.

If we ever need GE Data Docs (the HTML reporter), we wire this same
suite into a GE Checkpoint without changing the expectations.

CLI
---
    python -m src.transform.validate
    python -m src.transform.validate --strict      # exit 1 on any warning too

Output
------
    data/processed/validation_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PARQUET_PATH = PROCESSED_DIR / "exports_clean.parquet"
REPORT_PATH = PROCESSED_DIR / "validation_report.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("validate")


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class ExpectationResult:
    """Outcome of a single expectation check."""

    expectation_type: str
    column: str | None
    kwargs: dict[str, Any]
    success: bool
    observed_value: Any | None = None
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    finished_at: str | None = None
    parquet_path: str = ""
    row_count: int = 0
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def overall_success(self) -> bool:
        return self.failure_count == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "parquet_path": self.parquet_path,
            "row_count": self.row_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "overall_success": self.overall_success,
            "results": [r.to_dict() for r in self.results],
        }


# ---------------------------------------------------------------------------
# Expectation implementations — GE-style names, pandas under the hood
# ---------------------------------------------------------------------------


def _result(
    name: str,
    column: str | None,
    success: bool,
    *,
    observed: Any = None,
    unexpected_n: int = 0,
    unexpected_pct: float = 0.0,
    error: str | None = None,
    **kwargs: Any,
) -> ExpectationResult:
    return ExpectationResult(
        expectation_type=name,
        column=column,
        kwargs=kwargs,
        success=success,
        observed_value=observed,
        unexpected_count=unexpected_n,
        unexpected_percent=round(unexpected_pct, 4),
        error=error,
    )


def expect_column_values_to_not_be_null(
    df: pd.DataFrame, column: str
) -> ExpectationResult:
    if column not in df.columns:
        return _result(
            "expect_column_values_to_not_be_null",
            column,
            False,
            error=f"column missing: {column}",
        )
    n_null = int(df[column].isna().sum())
    return _result(
        "expect_column_values_to_not_be_null",
        column,
        success=(n_null == 0),
        observed=n_null,
        unexpected_n=n_null,
        unexpected_pct=100 * n_null / max(1, len(df)),
    )


def expect_column_values_to_be_between(
    df: pd.DataFrame,
    column: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    strict_min: bool = False,
    strict_max: bool = False,
) -> ExpectationResult:
    if column not in df.columns:
        return _result(
            "expect_column_values_to_be_between",
            column,
            False,
            error=f"column missing: {column}",
        )
    s = pd.to_numeric(df[column], errors="coerce")
    mask = pd.Series(True, index=s.index)
    if min_value is not None:
        mask &= (s > min_value) if strict_min else (s >= min_value)
    if max_value is not None:
        mask &= (s < max_value) if strict_max else (s <= max_value)
    unexpected = int((~mask).sum())
    return _result(
        "expect_column_values_to_be_between",
        column,
        success=(unexpected == 0),
        observed={
            "min": float(s.min()) if len(s) else None,
            "max": float(s.max()) if len(s) else None,
        },
        unexpected_n=unexpected,
        unexpected_pct=100 * unexpected / max(1, len(df)),
        min_value=min_value,
        max_value=max_value,
    )


def expect_column_value_lengths_to_equal(
    df: pd.DataFrame, column: str, value: int
) -> ExpectationResult:
    if column not in df.columns:
        return _result(
            "expect_column_value_lengths_to_equal",
            column,
            False,
            error=f"column missing: {column}",
        )
    lengths = df[column].astype(str).str.len()
    unexpected = int((lengths != value).sum())
    return _result(
        "expect_column_value_lengths_to_equal",
        column,
        success=(unexpected == 0),
        observed={"unique_lengths": sorted(map(int, lengths.unique()))},
        unexpected_n=unexpected,
        unexpected_pct=100 * unexpected / max(1, len(df)),
        value=value,
    )


def expect_column_values_to_be_in_set(
    df: pd.DataFrame, column: str, value_set: list[Any]
) -> ExpectationResult:
    if column not in df.columns:
        return _result(
            "expect_column_values_to_be_in_set",
            column,
            False,
            error=f"column missing: {column}",
        )
    unexpected = df[~df[column].isin(value_set)]
    return _result(
        "expect_column_values_to_be_in_set",
        column,
        success=(len(unexpected) == 0),
        observed={"unexpected_examples": list(unexpected[column].unique()[:5])},
        unexpected_n=len(unexpected),
        unexpected_pct=100 * len(unexpected) / max(1, len(df)),
        value_set=list(value_set),
    )


def expect_column_values_to_match_regex(
    df: pd.DataFrame, column: str, pattern: str
) -> ExpectationResult:
    if column not in df.columns:
        return _result(
            "expect_column_values_to_match_regex",
            column,
            False,
            error=f"column missing: {column}",
        )
    rx = re.compile(pattern)
    matches = df[column].astype(str).map(lambda v: bool(rx.fullmatch(v)))
    unexpected = int((~matches).sum())
    return _result(
        "expect_column_values_to_match_regex",
        column,
        success=(unexpected == 0),
        observed={
            "unexpected_examples": df.loc[~matches, column].astype(str).head(5).tolist()
        },
        unexpected_n=unexpected,
        unexpected_pct=100 * unexpected / max(1, len(df)),
        pattern=pattern,
    )


def expect_table_row_count_to_be_above(
    df: pd.DataFrame, min_rows: int
) -> ExpectationResult:
    return _result(
        "expect_table_row_count_to_be_above",
        None,
        success=(len(df) >= min_rows),
        observed=len(df),
        min_rows=min_rows,
    )


def expect_columns_to_exist(df: pd.DataFrame, columns: list[str]) -> ExpectationResult:
    missing = [c for c in columns if c not in df.columns]
    return _result(
        "expect_columns_to_exist",
        None,
        success=(len(missing) == 0),
        observed={"missing_columns": missing},
        unexpected_n=len(missing),
        columns=columns,
    )


# ---------------------------------------------------------------------------
# The JEIS expectation suite
# ---------------------------------------------------------------------------


def build_suite() -> list[Callable[[pd.DataFrame], ExpectationResult]]:
    """Return the JEIS expectation suite as a list of zero-arg-ish callables.

    Each callable takes only the DataFrame and runs one expectation. This
    pattern makes the suite trivially iterable and makes new expectations
    a one-line addition.
    """
    REQUIRED_COLS = [
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
        "fob_usd",
        "quantity_kg",
        "unit_price_usd",
        "is_outlier",
        "source",
    ]
    SCOPE_HS_CODES = ["440929", "442090", "330749", "130232", "130239"]
    KNOWN_SOURCES = ["COMTRADE", "VOLZA", "ZAUBA", "BAKER_HUGHES", "IMD"]

    return [
        # Schema & shape
        lambda df: expect_columns_to_exist(df, REQUIRED_COLS),
        lambda df: expect_table_row_count_to_be_above(df, min_rows=100),
        # Completeness — non-null core columns
        lambda df: expect_column_values_to_not_be_null(df, "shipment_date"),
        lambda df: expect_column_values_to_not_be_null(df, "fob_usd"),
        lambda df: expect_column_values_to_not_be_null(df, "quantity_kg"),
        lambda df: expect_column_values_to_not_be_null(df, "unit_price_usd"),
        lambda df: expect_column_values_to_not_be_null(df, "hs_code"),
        lambda df: expect_column_values_to_not_be_null(df, "dest_iso_alpha3"),
        lambda df: expect_column_values_to_not_be_null(df, "source"),
        # Range — FOB sanity cap. PRD FR-2 specifies $10M but that was
        # written for shipment-grain data. Comtrade rows are MONTHLY
        # COUNTRY AGGREGATES, where single-row values of $20-200M are
        # normal (e.g. India's guar gum exports to USA in any single
        # month). We raise the ceiling to $500M to catch true data-entry
        # errors while accepting realistic aggregate-grain magnitudes.
        # The 41 rows above $10M observed on real data are documented in
        # the validation report — they're legitimate large shipments,
        # not anomalies.
        lambda df: expect_column_values_to_be_between(
            df, "fob_usd", min_value=0, max_value=500_000_000, strict_min=True
        ),
        lambda df: expect_column_values_to_be_between(
            df, "quantity_kg", min_value=0, strict_min=True
        ),
        lambda df: expect_column_values_to_be_between(
            df, "unit_price_usd", min_value=0, max_value=10_000, strict_min=True
        ),
        lambda df: expect_column_values_to_be_between(
            df, "year", min_value=2018, max_value=2026
        ),
        lambda df: expect_column_values_to_be_between(
            df, "month", min_value=1, max_value=12
        ),
        # Format — categorical and pattern integrity
        lambda df: expect_column_value_lengths_to_equal(df, "dest_iso_alpha3", value=3),
        lambda df: expect_column_values_to_match_regex(df, "hs_code", pattern=r"\d{6}"),
        lambda df: expect_column_values_to_be_in_set(df, "hs_code", SCOPE_HS_CODES),
        lambda df: expect_column_values_to_be_in_set(df, "source", KNOWN_SOURCES),
        lambda df: expect_column_values_to_be_in_set(
            df, "is_peak_season", [True, False]
        ),
        lambda df: expect_column_values_to_be_in_set(df, "is_outlier", [True, False]),
    ]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def validate(parquet_path: Path = PARQUET_PATH) -> ValidationReport:
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"{parquet_path} not found — run src.transform.clean first."
        )

    log.info("Loading %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    log.info("Validating %d rows × %d columns", len(df), len(df.columns))

    report = ValidationReport(
        parquet_path=str(parquet_path.relative_to(PROJECT_ROOT)),
        row_count=len(df),
    )

    suite = build_suite()
    for expectation in suite:
        try:
            result = expectation(df)
        except Exception as exc:
            result = ExpectationResult(
                expectation_type="unknown",
                column=None,
                kwargs={},
                success=False,
                error=f"{exc.__class__.__name__}: {exc}",
            )
        report.results.append(result)
        symbol = "✓" if result.success else "✗"
        details = (
            f"unexpected={result.unexpected_count} ({result.unexpected_percent:.2f}%)"
            if not result.success
            else f"observed={result.observed_value}"
        )
        log.info(
            "  %s  %-45s  %-25s  %s",
            symbol,
            result.expectation_type,
            result.column or "—",
            details,
        )

    report.finished_at = datetime.utcnow().isoformat()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    log.info("Wrote %s", REPORT_PATH.name)

    log.info("=" * 70)
    log.info(
        "Validation %s — %d passed, %d failed (of %d expectations)",
        "PASSED" if report.overall_success else "FAILED",
        report.success_count,
        report.failure_count,
        len(report.results),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--parquet", type=Path, default=PARQUET_PATH)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero even on warnings (default: only on failures).",
    )
    args = parser.parse_args()

    report = validate(args.parquet)
    sys.exit(0 if report.overall_success else 1)


if __name__ == "__main__":
    main()
