# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Autonomy Rules

- Run all bash/powershell commands yourself without asking, except: deleting files, installing new packages
- Push to git automatically after every commit — no need to ask
- Run notebooks and scripts yourself and read the output — never ask me to run them and send screenshots
- When you fix something, verify it yourself by running it
- Commit working code to git automatically with descriptive messages
- Only pause and ask me for: major architecture decisions, API keys/secrets, and destructive operations

## Project Overview

Jodhpur Export Intelligence System (JEIS) — an end-to-end analytics platform for Jodhpur's furniture/handicraft export cluster (HS codes 440929, 442090, 330749, 130232, 130239). Pulls public trade data from UN Comtrade, transforms it through a validation pipeline, loads into a Supabase PostgreSQL star schema, and surfaces insights via Jupyter notebooks and Power BI.

## Commands

```bash
# Environment setup
pip install -r requirements.txt
cp .env.example .env   # then fill COMTRADE_API_KEY and DATABASE_URL

# Full ETL pipeline (ingest → transform → load)
python -m src.run_pipeline

# Individual pipeline stages
python -m src.ingest.comtrade_api                          # full pull (5 HS codes × 6 years)
python -m src.ingest.comtrade_api --hs-code 440929 --year 2023  # narrow pull
python -m src.ingest.comtrade_api --dry-run                # no file writes

python -m src.transform.clean                              # flatten JSON → Parquet + CSV
python -m src.transform.validate                           # run 20-expectation quality suite

python -m src.load.init_db                                 # apply schema.sql (idempotent)
python -m src.load.init_db --check                         # row counts only
python -m src.load.init_db --drop-first                    # DANGER: drops all tables

python -m src.load.load_db                                 # TRUNCATE + bulk insert fact table
python -m src.load.load_db --dry-run                       # parse + FK resolve, no write
python -m src.load.load_db --skip-truncate                 # append instead of replace

# Environment smoke test
python scripts/smoke_test.py                               # checks env vars, imports, DB, API key
python scripts/probe_comtrade.py                           # tests 6 API parameter combos

# Linting and formatting (ruff replaces black + flake8 + isort)
ruff check .
ruff format .

# Tests
pytest                                                     # test suite (currently sparse)
```

## Architecture

### Pipeline Layers

```
UN Comtrade API → data/raw/comtrade_{hs}_{year}_{date}.json   (immutable, date-stamped)
                → data/processed/exports_clean.{parquet,csv}   (canonical)
                → Supabase PostgreSQL (star schema)
                → notebooks/ + Power BI dashboard
```

**Orchestrator**: `src/run_pipeline.py` — a simple procedural script, not Airflow. The weekly refresh takes <15 min so no scheduler complexity is warranted.

**Ingest** (`src/ingest/comtrade_api.py`): Hits UN Comtrade Plus REST API at `https://comtradeapi.un.org/data/v1/get/C/M/HS`. India reporter code = 699. 30 calls per full refresh (5 HS × 6 years) well within the 500/day free-tier cap. Uses tenacity exponential backoff. Raw files are never overwritten — append-only with date stamp.

**Transform** (`src/transform/clean.py`): Flattens nested Comtrade JSON to tidy DataFrame. Key operations: ISO 3166-1 alpha-3 country normalisation via pycountry, peak-season flag (`PEAK_SEASON_MONTHS = (9, 10, 11)`), aggregate-partner exclusion (codes 0, 99, 199, etc. are world totals, not real countries), outlier flagging (≥99th percentile unit price per HS code — flagged, not dropped). Output: Parquet (primary) + CSV (human-readable).

**Validate** (`src/transform/validate.py`): 20 Great Expectations-style assertions (hand-rolled, no full GE scaffold). Pipeline halts on any failure rather than loading bad data. Writes `data/processed/validation_report.json`.

**Load** (`src/load/`):
- `schema.sql`: Kimball star schema — `dim_country`, `dim_product`, `dim_company`, `dim_time`, `fact_shipment`. Also `rig_count_weekly`, `monsoon_yearly` (regressors for guar model), `pipeline_run` (audit log). All DDL uses `IF NOT EXISTS`.
- `load_db.py`: Dimensions use UPSERT (`INSERT ... ON CONFLICT DO UPDATE`). Fact table uses TRUNCATE + bulk insert via `execute_values` each weekly run (full refresh, not incremental). FK resolution maps ISO codes → dimension PKs in-memory.
- Supabase connection uses port 6543 (pooler) with `pool_pre_ping=True` to handle free-tier pauses.

### Notebooks (notebooks/)

Six notebooks in sequence:
1. `01_EDA.ipynb` — 6 narrated charts, headline findings
2. `02_demand_forecast.ipynb` — Prophet time-series with cross-validation
3. `03_market_segmentation.ipynb` — K-means country clustering (k=4, business-driven)
4. `04_price_benchmark.ipynb` — FOB price gap vs. Vietnamese/Moroccan competitors
5. `05_buyer_risk.ipynb` — weighted risk score (no labelled defaults, so no logistic regression)
6. `06_guar_model.ipynb` — Prophet + rig count + monsoon regressors

Notebooks write back to the DB: `dim_country.cluster_label` (notebook 3), `dim_company.risk_score` (notebook 5).

### Key Design Constraints

- **Immutable raw files**: PRD FR-1 — never overwrite `data/raw/`; always date-stamp new pulls.
- **Halt on validation failure**: PRD FR-2 — pipeline exits non-zero rather than shipping wrong numbers.
- **Parameterised SQL only**: No f-string interpolation into queries anywhere in the codebase.
- **No floating dependency versions**: All 60 packages pinned in `requirements.txt`.
- **Notebook outputs stripped**: `nbstripout` pre-commit hook prevents large output blobs in git.

### Environment Variables

| Variable | Purpose |
|---|---|
| `COMTRADE_API_KEY` | UN Comtrade Plus subscription key |
| `DATABASE_URL` | Supabase PostgreSQL URI (pooler, port 6543) |
| `ALERT_EMAIL_FROM` / `ALERT_EMAIL_PASSWORD` | Gmail App Password for GitHub Actions alerts |
| `COMTRADE_MAX_ROWS` | 0 = unlimited (dev cap) |
| `RUN_MODE` | `dev` or `prod` (controls batch sizes and logging) |

### Pre-commit Hooks

Four hooks run before every commit: **gitleaks** (secret scanning with custom rules for Postgres URIs, Comtrade keys, Supabase JWTs, Gmail passwords), **ruff** (lint + format), **pre-commit-hooks** (large files, YAML/JSON/TOML validation), **nbstripout** (clear notebook outputs). Do not bypass with `--no-verify`.

### HS Codes in Scope

| Code | Category |
|---|---|
| 440929 | Wood furniture components |
| 442090 | Decorative wooden articles |
| 330749 | Fragrance products |
| 130232 | Guar gum |
| 130239 | Other natural gums |

Note: PRD references "440900" (invalid 4-digit code) — the correct 6-digit code is **440929**. This distinction matters for API calls.
