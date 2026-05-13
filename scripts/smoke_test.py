"""Smoke test — verify the local environment is wired up correctly.

Run this once after `pip install -r requirements.txt` and after creating .env.
It checks four things:

    1. Required env vars are present.
    2. We can import every critical library.
    3. Postgres connection opens and `SELECT 1` returns.
    4. Comtrade API key returns a 200 on a tiny test call.

Exit code 0 = all good. Non-zero = something to fix.

    python scripts/smoke_test.py
"""

from __future__ import annotations

import os
import sys
import textwrap
from importlib import import_module

from dotenv import load_dotenv

REQUIRED_ENV = ("DATABASE_URL", "COMTRADE_API_KEY")
REQUIRED_LIBS = (
    "pandas",
    "numpy",
    "sqlalchemy",
    "psycopg2",
    "prophet",
    "sklearn",
    "great_expectations",
    "matplotlib",
    "requests",
)

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def ok(msg: str) -> None:
    print(f"{GREEN}[ok]{RESET}   {msg}")


def fail(msg: str) -> None:
    print(f"{RED}[fail]{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"{YELLOW}[warn]{RESET} {msg}")


def check_env() -> bool:
    load_dotenv()
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        fail(f".env missing required keys: {missing}")
        print(
            textwrap.dedent("""
            Fix:
              1. cp .env.example .env
              2. Open .env in VS Code and fill in the values.
        """)
        )
        return False
    ok(f"env vars present ({', '.join(REQUIRED_ENV)})")
    return True


def check_imports() -> bool:
    failed = []
    for lib in REQUIRED_LIBS:
        try:
            import_module(lib)
        except ImportError as exc:
            failed.append((lib, str(exc)))
    if failed:
        for lib, err in failed:
            fail(f"could not import {lib}: {err}")
        print("Fix: pip install -r requirements.txt")
        return False
    ok(f"all {len(REQUIRED_LIBS)} required libraries import cleanly")
    return True


def check_postgres() -> bool:
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 AS ping, current_database() AS db, version() AS v")
            ).one()
        ok(f"postgres connect: db={row.db}, version={row.v.split(',')[0]}")
        return True
    except Exception as exc:
        fail(f"postgres connection failed: {exc.__class__.__name__}: {exc}")
        print(
            textwrap.dedent("""
            Common fixes:
              * Check DATABASE_URL spelling — port should be 6543 (pooler), not 5432.
              * If special chars in password (@ # & ? /), URL-encode them.
              * Supabase free tier pauses after a week idle. Visit your project
                page in the browser to wake it, then retry.
        """)
        )
        return False


def check_comtrade() -> bool:
    import requests

    key = os.environ["COMTRADE_API_KEY"]
    # smallest possible call: India total exports of HS 440900 to USA, 2023, monthly
    url = (
        "https://comtradeapi.un.org/data/v1/get/C/M/HS"
        "?freqCode=M&clCode=HS&period=2023&reporterCode=699"
        "&partnerCode=842&cmdCode=440900&flowCode=X"
    )
    try:
        resp = requests.get(
            url,
            headers={"Ocp-Apim-Subscription-Key": key},
            timeout=15,
        )
    except requests.RequestException as exc:
        fail(f"comtrade network error: {exc}")
        return False

    if resp.status_code == 200:
        try:
            payload = resp.json()
            n = len(payload.get("data", []))
            ok(f"comtrade API: 200 OK, {n} rows returned for HS 440900 IN→US 2023")
            return True
        except ValueError:
            warn("comtrade returned 200 but payload is not JSON")
            return False
    elif resp.status_code == 401:
        fail("comtrade API: 401 Unauthorized — your COMTRADE_API_KEY is wrong")
        return False
    elif resp.status_code == 403:
        fail("comtrade API: 403 Forbidden — key valid but no active subscription")
        return False
    else:
        fail(f"comtrade API: HTTP {resp.status_code} — {resp.text[:200]}")
        return False


def main() -> None:
    print("\n=== JEIS smoke test ===\n")
    results = [
        ("env", check_env()),
        ("imports", check_imports()),
    ]
    if all(r for _, r in results):
        # Only run network checks if local checks pass.
        results.append(("postgres", check_postgres()))
        results.append(("comtrade", check_comtrade()))

    print("\nSummary:")
    for name, passed in results:
        symbol = f"{GREEN}OK{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {name:10s} {symbol}")

    if all(r for _, r in results):
        print(
            f"\n{GREEN}All checks passed. You are ready to run the pipeline.{RESET}\n"
        )
        sys.exit(0)
    else:
        print(
            f"\n{RED}One or more checks failed — fix above before continuing.{RESET}\n"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
