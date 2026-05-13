"""Diagnostic probe — fires 6 parameter combinations at Comtrade Plus and
reports which ones return actual data.

Used to figure out what subset of (freqCode, clCode, partnerCode, ...) the
free-tier subscription supports for India exporter queries. Run once as
part of pipeline calibration; not invoked by the pipeline itself.

    python scripts/probe_comtrade.py
"""

from __future__ import annotations

import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv

BASE = "https://comtradeapi.un.org/data/v1/get"


PROBES: list[dict[str, Any]] = [
    # 1. Baseline — exactly what the pipeline tries today (and gets 0 rows)
    {
        "label": "Monthly · World partner · clCode=HS · 12-month period",
        "path": "C/M/HS",
        "params": {
            "freqCode": "M",
            "clCode": "HS",
            "period": ",".join(f"2023{m:02d}" for m in range(1, 13)),
            "reporterCode": "699",
            "partnerCode": "0",
            "cmdCode": "440900",
            "flowCode": "X",
        },
    },
    # 2. Annual instead of monthly — does India report annual?
    {
        "label": "Annual · World partner · clCode=HS",
        "path": "C/A/HS",
        "params": {
            "freqCode": "A",
            "clCode": "HS",
            "period": "2023",
            "reporterCode": "699",
            "partnerCode": "0",
            "cmdCode": "440900",
            "flowCode": "X",
        },
    },
    # 3. Monthly but specific partner USA — bilateral channels often have data
    {
        "label": "Monthly · USA partner · clCode=HS · single month 202301",
        "path": "C/M/HS",
        "params": {
            "freqCode": "M",
            "clCode": "HS",
            "period": "202301",
            "reporterCode": "699",
            "partnerCode": "842",
            "cmdCode": "440900",
            "flowCode": "X",
        },
    },
    # 4. Mirror — USA reporting India as partner. Trade flows are symmetric;
    # if Comtrade has US->India import data we can use it as a proxy for
    # India->US export when India's own filing is missing.
    {
        "label": "Mirror · USA reporter, India partner · monthly 202301 · imports",
        "path": "C/M/HS",
        "params": {
            "freqCode": "M",
            "clCode": "HS",
            "period": "202301",
            "reporterCode": "842",
            "partnerCode": "699",
            "cmdCode": "440900",
            "flowCode": "M",
        },
    },
    # 5. Specific HS revision (H5 = HS 2017) instead of generic HS
    {
        "label": "Monthly · World · clCode=H5 · 202301",
        "path": "C/M/H5",
        "params": {
            "freqCode": "M",
            "clCode": "H5",
            "period": "202301",
            "reporterCode": "699",
            "partnerCode": "0",
            "cmdCode": "440900",
            "flowCode": "X",
        },
    },
    # 6. A different HS code — is the problem code-specific?
    {
        "label": "Annual · World · clCode=HS · cmd=130232 (guar gum)",
        "path": "C/A/HS",
        "params": {
            "freqCode": "A",
            "clCode": "HS",
            "period": "2023",
            "reporterCode": "699",
            "partnerCode": "0",
            "cmdCode": "130232",
            "flowCode": "X",
        },
    },
]


def run_probe(probe: dict[str, Any], key: str) -> tuple[int, int, str]:
    """Returns (http_status, row_count, brief_message)."""
    url = f"{BASE}/{probe['path']}"
    headers = {"Ocp-Apim-Subscription-Key": key}
    try:
        resp = requests.get(url, params=probe["params"], headers=headers, timeout=30)
    except requests.RequestException as exc:
        return -1, 0, f"network error: {exc}"

    try:
        payload = resp.json()
    except ValueError:
        return resp.status_code, 0, f"non-json body: {resp.text[:120]}"

    rows = len(payload.get("data") or [])
    msg_bits = []
    for k in ("count", "elapsedTime", "error", "message"):
        v = payload.get(k)
        if v not in (None, "", " "):
            msg_bits.append(f"{k}={v!r}")
    return resp.status_code, rows, ", ".join(msg_bits) or "(no diagnostic fields)"


def main() -> None:
    load_dotenv()
    key = os.getenv("COMTRADE_API_KEY")
    if not key:
        print("ERROR: COMTRADE_API_KEY not set in .env")
        sys.exit(1)

    print("\n=== Comtrade Plus parameter probe ===")
    print(f"  {len(PROBES)} probes\n")

    for i, probe in enumerate(PROBES, start=1):
        status, rows, diag = run_probe(probe, key)
        verdict = "DATA" if rows > 0 else ("EMPTY" if status == 200 else "FAIL")
        print(f"[{i}] {probe['label']}")
        print(f"    HTTP {status}  rows={rows}  →  {verdict}")
        print(f"    {diag}")
        print()


if __name__ == "__main__":
    main()
