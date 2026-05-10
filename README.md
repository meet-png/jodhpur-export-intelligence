# Jodhpur Export Intelligence System (JEIS)

> **An end-to-end data analytics platform for Jodhpur's ₹100 Cr+ export cluster — turning gut-feel decisions into data-driven ones.**

<!-- DASHBOARD SCREENSHOT GOES HERE — replace with real PNG after dashboard is built -->
<!-- ![JEIS Dashboard](docs/img/dashboard_overview.png) -->

📺 **[Live demo (90-second walkthrough)](https://youtu.be/TODO)** &nbsp;·&nbsp; 📊 **[1-page business brief (PDF)](docs/business_brief.pdf)** &nbsp;·&nbsp; 📓 **[Notebook tour](notebooks/)**

---

## TL;DR — three findings in one paragraph

> *(Will be filled with real numbers once models run. Placeholder targets from the PRD:)*
> Across 5 years of UN Comtrade + India shipment data, this analysis surfaces three quantified opportunities for Jodhpur's handicraft and guar gum exporters:
> **(1)** Demand peaks ~XX% above the annual average in September–November while production runs flat — leaving roughly 18–22% of peak revenue unrealised.
> **(2)** Of 40+ destination countries, K-means segmentation identifies a "Rising Stars" cluster (Gulf + ANZ) growing at XX% YoY versus a stagnating European core — a clear marketing-budget reallocation case.
> **(3)** Jodhpur FOB prices for HS 130232 (guar gum) and HS 442090 (wooden articles) trail Vietnamese/Moroccan equivalents by USD X.XX/kg — closing 30% of the gap is worth ₹XX–XX Cr annually across the cluster.

---

## Why I built this

I'm from Jodhpur. Growing up here, I watched export businesses around me — handicraft houses in Basni, guar processors near Boranada — generate hundreds of crores in revenue while operating with zero data infrastructure. Every decision (when to produce, where to sell, what price to quote, which buyer to trust) is made by memory and intuition.

That works until it doesn't. CRISIL documented in 2023 that handicraft exporters' working capital cycles stretch from 90 to 120+ days during weak demand periods. FISME quantified ₹7.34 lakh crore in delayed MSME payments nationally as of March 2024. EPCH data shows the September–November pre-Christmas peak is missed every year because production is flat.

This project is my attempt to close that gap with the tools that already exist for free: UN Comtrade, Baker Hughes, IMD, Python, Postgres, Power BI. No company data was used. Every input is public.

<!-- After your Basni/Boranada field visit, replace this paragraph with:
"I also went and talked to N exporters across Basni and Boranada before writing a line of code.
Their notes are in [`docs/field_notes.md`](docs/field_notes.md). The three pain points they kept
returning to are the three problems this system addresses."
This paragraph is omitted until those conversations actually happen. -->

The three pain points this system addresses come from the documented research above — and (once the field visit is complete) from direct conversations with exporters in Basni and Boranada captured in [`docs/field_notes.md`](docs/field_notes.md).

## What it does

JEIS is a fully automated, weekly-refreshing data pipeline that:

1. **Extracts** trade data from 5 public sources (UN Comtrade, Volza, Zauba, Baker Hughes, IMD)
2. **Transforms** it into a clean star schema in PostgreSQL (Supabase, free tier)
3. **Runs 5 analytical models:** demand forecasting (Prophet), market segmentation (K-means), price-gap benchmarking, buyer payment-risk scoring, and a guar commodity price model with rig-count + monsoon regressors
4. **Surfaces results** in a Power BI dashboard and a one-page strategic brief
5. **Refreshes itself every Sunday** via GitHub Actions, with email alerts on failure

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │       Public data sources            │
                    │  Comtrade · Volza · Zauba · Baker    │
                    │       Hughes · IMD                   │
                    └──────────────┬───────────────────────┘
                                   │ weekly pull
                                   ▼
   ┌────────────────────┐   ┌────────────────────────┐   ┌────────────────────┐
   │   src/ingest/      │──▶│  src/transform/        │──▶│   src/load/        │
   │  one .py per src   │   │  clean.py + validate.py│   │  schema.sql        │
   │  → data/raw/       │   │  → data/processed/     │   │  load_db.py        │
   │  date-stamped      │   │  Great Expectations    │   │  → Supabase PG16   │
   └────────────────────┘   └────────────────────────┘   └─────────┬──────────┘
                                                                   │
                                   ┌───────────────────────────────┴──────────┐
                                   ▼                                          ▼
                       ┌──────────────────────┐                   ┌──────────────────────┐
                       │  notebooks/  (6×)    │                   │   Power BI Desktop   │
                       │  Prophet · K-means · │                   │   5-panel dashboard  │
                       │  benchmark · risk    │                   │   .pbix in dashboard/│
                       └──────────────────────┘                   └──────────────────────┘
```

The whole pipeline is orchestrated by [`.github/workflows/pipeline.yml`](.github/workflows/pipeline.yml) which runs every Sunday at 23:30 IST.

For the full design rationale (why star schema, why Prophet over SARIMA, why Supabase over local Postgres), see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Run it yourself in 3 commands

> Tested on Windows 11 with Python 3.11.9. Mac/Linux works identically.

```bash
git clone https://github.com/<your-username>/jodhpur-export-intelligence.git
cd jodhpur-export-intelligence
pip install -r requirements.txt && cp .env.example .env  # then fill .env

python -m src.run_pipeline   # full ETL → analysis → load
jupyter lab notebooks/        # explore the 6 analytical notebooks
```

You'll need a free [UN Comtrade Plus](https://comtradeplus.un.org/) API key and a free [Supabase](https://supabase.com/) project. Both are documented in [`SETUP.md`](SETUP.md) with step-by-step screenshots.

## Tech stack

| Layer | Tools |
|---|---|
| **Language** | Python 3.11 |
| **Data wrangling** | pandas 2.2, numpy 1.26, pyarrow (Parquet) |
| **APIs / scraping** | httpx, requests, tenacity (retries), beautifulsoup4 |
| **Validation** | Great Expectations 0.18 |
| **Database** | PostgreSQL 16 on Supabase (free tier, ap-south-1) |
| **ORM** | SQLAlchemy 2.0 |
| **Forecasting** | Facebook Prophet 1.1.5 |
| **Clustering** | scikit-learn 1.5 |
| **Dashboard** | Power BI Desktop |
| **Automation** | GitHub Actions (weekly cron + manual) |
| **Notebooks** | JupyterLab 4 |

## Repository layout

```
jodhpur-export-intelligence/
├── data/
│   ├── raw/              # date-stamped raw files, never modified
│   └── processed/        # exports_clean.csv, rig_count_clean.csv
├── notebooks/
│   ├── 01_EDA.ipynb
│   ├── 02_demand_forecast.ipynb
│   ├── 03_market_segmentation.ipynb
│   ├── 04_price_benchmark.ipynb
│   ├── 05_buyer_risk.ipynb
│   └── 06_guar_model.ipynb
├── src/
│   ├── ingest/           # one .py file per data source
│   ├── transform/        # clean.py, validate.py
│   ├── load/             # schema.sql, load_db.py
│   └── run_pipeline.py   # orchestrator
├── .github/workflows/pipeline.yml
├── dashboard/jodhpur_export_dashboard.pbix
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DATA_DICTIONARY.md
│   ├── business_brief.pdf
│   └── field_notes.md
├── tests/
├── requirements.txt
├── .env.example
└── README.md
```

## Acknowledgements & data sources

Real public data only — no scraping that violates any TOS, no proprietary feeds, no company data.

- **UN Comtrade Plus** — monthly trade flows, HS 2017 classification ([comtradeplus.un.org](https://comtradeplus.un.org/))
- **Volza** — buyer-level shipment records (free trial)
- **Baker Hughes** — North America Rotary Rig Count, weekly ([rigcount.bakerhughes.com](https://rigcount.bakerhughes.com/))
- **IMD / data.gov.in** — district-level monsoon rainfall
- **CRISIL Ratings 2023, FISME/GAME 2024, EPCH Annual Report 2024–25** — research context

## License

MIT — see [LICENSE](LICENSE).

---

*Built solo by [Meet Kabra](https://github.com/) over 15 days in May 2026 as a portfolio project. Built with AI pair-programming assistance — every architectural and analytical decision is documented in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and defensible in technical interviews.*
