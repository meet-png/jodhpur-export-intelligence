"""Render data/processed/validation_report.json as GitHub-flavoured Markdown.

Used by .github/workflows/weekly-refresh.yml to surface the data-quality
result directly in the Actions run summary (no need to open artifacts).
Prints to stdout; the workflow appends it to $GITHUB_STEP_SUMMARY.

Exit code is always 0 — this is a reporter, not a gate. The pipeline's own
validate stage is the gate.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPORT = Path("data/processed/validation_report.json")


def main() -> int:
    # GitHub runners are UTF-8; Windows consoles default to cp1252 and would
    # choke on the status emoji. Reconfigure defensively so this runs anywhere.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

    if not REPORT.exists():
        print(
            "## Data validation\n\n"
            "No `validation_report.json` was produced — the pipeline halted "
            "before the validate stage. Check the run log."
        )
        return 0

    r = json.loads(REPORT.read_text(encoding="utf-8"))
    ok = r.get("overall_success", False)

    print(f"## Data validation: {'PASSED ✅' if ok else 'FAILED ❌'}")
    print()
    print(f"- Rows validated: **{r.get('row_count', 0):,}**")
    print(
        f"- Expectations: **{r.get('success_count', 0)} passed**, "
        f"**{r.get('failure_count', 0)} failed**"
    )

    if not ok:
        print()
        print("### Failed expectations")
        for e in r.get("results", []):
            if not e.get("success"):
                print(
                    f"- `{e.get('expectation_type')}` on "
                    f"`{e.get('column') or '—'}` "
                    f"(unexpected={e.get('unexpected_count', 0)})"
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
