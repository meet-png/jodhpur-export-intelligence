# Jodhpur Export Intelligence System (JEIS)

> **An end-to-end, self-refreshing analytics pipeline for Jodhpur's furniture & guar-gum export cluster — built entirely on free public data.**

📊 **[Interactive dashboard](#-live-dashboard)** &nbsp;·&nbsp; 📓 **[Notebook tour](notebooks/)** &nbsp;·&nbsp; 🏗 **[Architecture & design rationale](docs/ARCHITECTURE.md)** &nbsp;·&nbsp; 🔁 **[Weekly-refresh workflow](.github/workflows/weekly-refresh.yml)**

---

## 📊 Live dashboard

A 5-tab interactive dashboard ([`streamlit_app.py`](streamlit_app.py)) lets you explore every headline finding — including a live slider that re-forecasts guar demand under different US rig-count scenarios.

<!-- After deploying on Streamlit Community Cloud, replace the line below with:
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://YOUR-APP.streamlit.app) -->

**▶ Live link:** _deploying — runs locally right now in one command:_

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py        # opens at http://localhost:8501
```

The app reads only the version-controlled CSVs in `data/processed/` — **no database or API key needed**, so it behaves identically locally and on Streamlit Cloud.

---

## TL;DR — three findings that change a decision

Built on **12,828 rows** of UN Comtrade trade flows (5 HS codes, ~177 destination countries, Jan 2019 – Dec 2024), cross-checked against Baker Hughes rig counts and IMD monsoon data:

1. **The "September–November peak" the industry plans around does not exist at the cluster level.** The aggregate is **−8.0%** *below* annual average in Sep–Nov — because guar gum (83% of cluster revenue) runs a counter-cyclical oilfield demand pattern (**−11.1%** in Sep–Nov) that swamps the genuine handicraft pre-Christmas peak (**+9.1%**). The actionable conclusion is the opposite of the brief: run **two** production calendars, not one.

2. **A naïve price-gap model says the cluster leaves ₹18,310 Cr on the table. ~74% of that is a measurement artifact.** Morocco's "guar gum" (HS 130232) sells at 19× India's price because it's food/pharma-grade — a different product under the same HS code. Stripping that out, the **defensible opportunity is ≈ ₹4,711 Cr** (grade-adjusted, 30% capture, ₹83/USD; full FX sensitivity in-notebook). Refusing to report the inflated number is the point.

3. **Guar export value is driven more by US drilling than by the monsoon.** The SARIMAX model with exogenous regressors puts the 12-month base forecast at **$380.1M (≈ ₹3,155 Cr)**, with a **₹1,540 Cr swing** between high- and low-rig-count scenarios — a bigger lever than monsoon supply. Watch Baker Hughes, not just the IMD forecast.

> Honesty note: the forecast models score **MAPE ≈ 25%**, which *misses* the PRD's <20% target. That is reported, not hidden — see [Honest limitations](#honest-limitations). The value here is decision-framing and uncertainty quantification, not false precision.

---

## Why I built this

I'm from Jodhpur. Growing up here, I watched export businesses around me — handicraft houses in Basni, guar processors near Boranada — generate hundreds of crores in revenue while operating with zero data infrastructure. Every decision (when to produce, where to sell, what price to quote, which buyer to trust) is made by memory and intuition.

That works until it doesn't. CRISIL documented in 2023 that handicraft exporters' working-capital cycles stretch from 90 to 120+ days during weak demand periods. FISME quantified ₹7.34 lakh crore in delayed MSME payments nationally as of March 2024. EPCH data shows the September–November pre-Christmas window is planned for every year on a cluster-wide assumption this project shows is wrong.

This is my attempt to close that gap with tools that already exist for free: UN Comtrade, Baker Hughes, IMD, Python, PostgreSQL. **No company data was used. Every input is public and reproducible.**

## What it does

JEIS is a fully automated, weekly-refreshing pipeline that:

1. **Ingests** from **3 public sources** — UN Comtrade (trade flows), Baker Hughes (NA rig count), IMD (Rajasthan monsoon) — each with a versioned offline fallback so it runs on a fresh clone.
2. **Transforms** raw JSON into a clean Kimball star schema in PostgreSQL (Supabase free tier).
3. **Gates on data quality** — a 20-expectation validation suite; the pipeline **halts before the DB load** on any failure (never ships bad numbers).
4. **Runs 5 analytical notebooks** — demand forecasting (SARIMAX), market segmentation (K-means), price-gap benchmarking, buyer-risk scoring, and a guar commodity model with rig-count + monsoon regressors.
5. **Refreshes itself every Sunday** via GitHub Actions, with a Gmail alert on failure.

## Headline analytical results

| Notebook | Output | Verified result |
|---|---|---|
| `02_demand_forecast` | Seasonality + SARIMAX | Aggregate Sep–Nov **−8.0%** (guar −11.1%, handicraft +9.1%); MAPE 25.0% |
| `03_market_segmentation` | K-means, k=4 | 130 countries clustered: **65 Core, 44 Declining, 12 Untapped, 9 Rising Star**; 22-market "Core-but-Declining" watchlist (USA −15.1%, China −22.6% CAGR) |
| `04_price_benchmark` | FOB gap vs Vietnam/Morocco | Raw ₹18,310 Cr → **grade-adjusted ₹4,711 Cr** after removing the Morocco product-grade artifact |
| `05_buyer_risk` | 5-signal weighted score | 132 destination markets scored; **₹363.8 Cr** of historic revenue concentrated in High-risk markets |
| `06_guar_model` | SARIMAX + exog regressors | Base **$380.1M (₹3,155 Cr)** / 12 mo; **₹1,540 Cr** rig-count swing; rig count > monsoon as the driver |

Notebooks 3 and 5 write `cluster_label` / `risk_score` back to the database.

## Architecture

```
            ┌─────────────────────────────────────────────┐
            │  Public sources (each w/ offline fallback)  │
            │  UN Comtrade  ·  Baker Hughes  ·  IMD        │
            └───────────────────────┬─────────────────────┘
                                    │  src.run_pipeline (orchestrator)
                                    ▼
   ┌──────────────┐   ┌────────────────────────┐   ┌────────────────────┐
   │  src/ingest/ │──▶│  src/transform/        │──▶│  src/load/         │
   │ comtrade ·   │   │  clean.py              │   │  schema.sql        │
   │ rig_count ·  │   │  validate.py  ◀─ HALT  │   │  init_db.py        │
   │ monsoon      │   │  (20 expectations)     │   │  load_db.py        │
   │ → data/raw/  │   │  → data/processed/     │   │  → Supabase PG16   │
   └──────────────┘   └────────────────────────┘   └─────────┬──────────┘
                                                             │
                                                             ▼
                                              ┌──────────────────────────┐
                                              │  notebooks/  (6×)         │
                                              │  SARIMAX · K-means ·      │
                                              │  benchmark · risk · guar  │
                                              └──────────────────────────┘
```

Orchestrated by [`src/run_pipeline.py`](src/run_pipeline.py) and scheduled by [`.github/workflows/weekly-refresh.yml`](.github/workflows/weekly-refresh.yml) (Sunday 02:00 UTC / 07:30 IST + manual dispatch). The pipeline **halts on the first failure** — validation is a hard gate before any DB write (PRD FR-2). DB stages **skip gracefully** (not fail) when `DATABASE_URL` is unset, so it runs offline.

Full design rationale (why a star schema, why SARIMAX over Prophet, why Supabase) is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Run it yourself

> Tested on Windows 11, Python 3.11. Mac/Linux identical.

```bash
git clone https://github.com/meet-png/jodhpur-export-intelligence.git
cd jodhpur-export-intelligence
pip install -r requirements-pipeline.txt   # full env (root requirements.txt = slim dashboard deps)
cp .env.example .env            # fill COMTRADE_API_KEY + DATABASE_URL

python -m src.run_pipeline                 # full ETL → validate → load
python -m src.run_pipeline --skip-ingest   # reuse committed data, no API calls
python -m src.run_pipeline --dry-run       # every stage except the DB write
jupyter lab notebooks/                     # explore the 6 notebooks
```

A free [UN Comtrade Plus](https://comtradeplus.un.org/) key and free [Supabase](https://supabase.com/) project enable the full path. Without `DATABASE_URL` the orchestrator still ingests, cleans and validates — the DB stages skip cleanly.

## Tech stack

| Layer | Tools |
|---|---|
| Language | Python 3.11 |
| Data | pandas 2.2, numpy, pyarrow (Parquet) |
| APIs / retries | requests, tenacity (exponential backoff) |
| Validation | Hand-rolled GE-style suite — 20 expectations, JSON report (no GE scaffold; rationale in ARCHITECTURE.md) |
| Database | PostgreSQL 16 on Supabase (free tier, ap-south-1) |
| ORM / driver | SQLAlchemy 2.0, psycopg2 |
| Forecasting | statsmodels **SARIMAX** (Prophet was dropped — no CmdStan build dependency) |
| Clustering | scikit-learn (K-means, PCA) |
| Automation | GitHub Actions (weekly cron + manual dispatch) |
| Quality gates | ruff, gitleaks, nbstripout (pre-commit) |

## Repository layout

```
jodhpur-export-intelligence/
├── data/
│   ├── raw/                 # date-stamped API pulls (immutable, gitignored)
│   ├── external/            # curated, version-controlled regressor sources
│   └── processed/           # exports_clean.csv, rig_count_clean.csv, monsoon_clean.csv
├── notebooks/               # 01_EDA … 06_guar_model
├── src/
│   ├── ingest/              # comtrade_api.py, rig_count.py, monsoon.py
│   ├── transform/           # clean.py, validate.py
│   ├── load/                # schema.sql, init_db.py, load_db.py
│   └── run_pipeline.py      # 7-stage orchestrator, halt-on-failure
├── scripts/                 # smoke_test, validation_summary, helpers
├── .github/workflows/weekly-refresh.yml
├── docs/ARCHITECTURE.md
├── requirements.txt · .env.example · SECURITY.md · LICENSE
```

## Honest limitations

A portfolio project is more credible when it states what it *isn't*:

- **Data is country-monthly aggregates, not company shipments.** "Buyer risk" (NB05) scores each destination *market* as a buyer proxy — it is a documented heuristic, **not** a trained default model (no labelled defaults exist in public data).
- **Data ends Dec 2024.** UN Comtrade has a structural 6–18 month reporting lag; this is stamped in every notebook's provenance banner, not glossed over.
- **Forecast MAPE ≈ 25%** misses the PRD's <20% target. The models are directionally useful with wide, honest confidence bands — closing the gap needs weekly rig data and price/volume decomposition.
- **Rupee figures are decision-framing, not booked numbers.** Every headline ₹ figure ships with an FX sensitivity table; the opportunity figure is grade-adjusted specifically because the naïve version is misleading.

These constraints are the reason the analysis leads with *judgment* (debunking the seasonal assumption, stripping the Morocco artifact) rather than precision claims.

## Roadmap

- Power BI / Streamlit dashboard over `v_monthly_export` + `v_buyer_risk_register` (schema and write-backs already in place)
- Upgrade curated regressor sources from annual to true weekly Baker Hughes / monthly IMD series (drop-in; ingestion + schema already built)
- Expand the validation suite and `tests/`

## Security

Full policy in [`SECURITY.md`](SECURITY.md). Highlights: secrets only in gitignored `.env`; gitleaks pre-commit hook; pinned dependencies; parameterised SQL only; no PII (all sources are public trade statistics).

## Data sources

- **UN Comtrade Plus** — monthly trade flows, HS 2017 ([comtradeplus.un.org](https://comtradeplus.un.org/))
- **Baker Hughes** — North America Rotary Rig Count ([rigcount.bakerhughes.com](https://rigcount.bakerhughes.com/))
- **IMD** — Rajasthan south-west monsoon (% of Long Period Average)
- Research context: CRISIL Ratings 2023, FISME/GAME 2024, EPCH Annual Report 2024–25

## License

MIT — see [LICENSE](LICENSE).

---

*Built solo by [Meet Kabra](https://github.com/meet-png) as a portfolio project, with AI pair-programming assistance. Every architectural and analytical decision is documented in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and defensible in a technical interview.*
