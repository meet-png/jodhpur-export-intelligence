# Architecture & design decisions

This document is the *defensible* version of the project — the "why" behind every choice. If a reviewer asks "why did you do X instead of Y?", the answer lives here. Every claim below matches the code as shipped (this doc is kept in sync with the codebase — stale design docs are worse than none).

---

## 1. The problem framing

The PRD enumerates five pain points. They are not equally weighted. After triangulating across CRISIL 2023, FISME/GAME 2024, and EPCH 2024–25, the analysis prioritised two — and, importantly, **the analysis overturned one of the PRD's own assumptions** along the way:

1. **Demand mis-timing.** The PRD assumes a single cluster-wide Sep–Nov peak to plan production around. Notebook 02 shows that at the **cluster level the Sep–Nov window is −8.0% *below* the annual average** — because guar gum (≈85% of cluster FOB) runs a counter-cyclical oilfield demand pattern (−11.1% in Sep–Nov) that swamps the genuine handicraft pre-Christmas peak (+9.1%). The actionable conclusion is the *opposite* of the brief: run **two** production calendars, not one. Finding (and proving) that the stated assumption is wrong is the project's headline analytical result.

2. **Working capital trapped in receivables.** Revenue is dangerously concentrated — top 10 destinations = 76% of FOB, USA alone 34%. The buyer-risk module sizes the exposure: ₹363.8 Cr of historic revenue sits in markets scored High-risk.

The system is architected so the demand-forecasting and buyer-risk modules get the cleanest data and the most defensive engineering; the other modules are well-built but get proportionally less.

## 2. Data architecture

### 2.1 Storage: star schema in PostgreSQL (Supabase)

**Decision:** Kimball-style star schema — one fact table (`fact_shipment`) and four dimensions (country, product, company, time).

**Rejected alternatives & why:**

| Alternative | Why rejected |
|---|---|
| **Third-normal form (3NF)** | The workload is analytical aggregation, not transactional. 3NF forces 5–6-hop join paths for every aggregation and balloons query times. |
| **Data Vault 2.0** | Over-engineered. Hub-link-satellite shines with many conflicting source systems and heavy SCD needs. We have one canonical source per entity and slow-changing dimensions; the bookkeeping would dwarf the analytical value. |
| **Wide denormalised single table** | Destroys dimensional reuse — every aggregation re-encodes country labels, exploding storage and breaking referential integrity. |
| **dbt + One Big Table on DuckDB** | Modern and valid, but: (a) the resume value of "PostgreSQL + SQLAlchemy" is higher than "DuckDB" for Indian data roles in 2026; (b) Supabase deploys real Postgres free in `ap-south-1`; (c) the transform layer is ~400 LOC of Python — dbt's project scaffolding isn't justified yet. |

**SCD strategy:** Type 1 for `dim_country.cluster_label` and `dim_company.risk_score` — these are *recomputed* every weekly run from the latest data. The operational decision is always made on the current label; we don't need "what was country X's cluster last March?". A v2 needing history would add `effective_from / effective_to` + a CDC trigger.

### 2.2 Hosting: Supabase free tier

Real Postgres 16 in `ap-south-1`, zero Windows install pain, GitHub Actions can reach it (a local-only Postgres cannot — that's the killer feature), free tier covers our ~80 MB footprint. **Trade-off accepted:** it pauses after 7 days idle and the first query after a pause takes ~3 s — fine for a portfolio project, and the loader uses `pool_pre_ping=True` so the pause is transparent.

### 2.3 File layout: raw → processed → load

`data/raw/` → date-stamped, never modified (PRD FR-1). The audit trail. Re-runs always write a *new* file. This is the single most important data-engineering discipline here — interviewers probe for it because most beginners mutate raw files.

`data/processed/` → cleaned Parquet (canonical, ~10× smaller / ~3× faster than CSV) + CSV (human-inspectable, Git-diffable). The committed processed CSVs (`exports_clean.csv`, `rig_count_clean.csv`, `monsoon_clean.csv`, the price/segment/forecast summaries) are also what makes the live dashboard runnable with **no database** (§5).

`data/external/` → curated, version-controlled regressor source files (Baker Hughes annual rig averages, IMD Rajasthan monsoon) so the rig/monsoon ingestion is fully reproducible offline.

## 3. Pipeline design

### 3.1 Why a procedural Python orchestrator (not Airflow / Prefect / Dagster)?

The full pipeline runs in <15 minutes once a week. Airflow here is a forklift to carry one box — its scheduler/metastore/web-server complexity far exceeds the value at this scale.

`src/run_pipeline.py` runs seven stages in sequence — `comtrade → rig_count → monsoon → clean → validate → init_db → load_db` — with structured logging, halt-on-first-failure, and **graceful skip** of the DB stages when `DATABASE_URL` is absent (so it runs offline on a fresh clone). GitHub Actions (`.github/workflows/weekly-refresh.yml`, Sunday 02:00 UTC + manual dispatch) provides everything an Airflow scheduler would at this scale: cron, secrets, retries, a Gmail failure alert, free compute. If this ever grows to dozens of branching DAGs, *that* is when Airflow earns its keep.

### 3.2 Why a hand-rolled validation suite (not Great Expectations / Pandera)?

`src/transform/validate.py` is a hand-rolled, ~20-expectation suite using GE-vocabulary names (`expect_column_values_to_not_be_null`, …) and emitting a structured JSON report.

**Why not the full Great Expectations library?** For a single-table, single-suite pipeline, GE's context/datasource/checkpoint scaffolding is overhead with no analytical payoff, and it pins a heavy dependency tree. The hand-rolled suite is one readable file, runs every expectation regardless of earlier failures (exhaustive report, not fail-fast), and is trivially JSON-serialisable. The *observability story* is real but lighter than GE Data Docs: the validation JSON is published into the GitHub Actions run summary (`scripts/validation_summary.py`) and uploaded as a build artifact. If a future need for GE Data Docs HTML appears, the same expectations drop into a GE Checkpoint unchanged — the vocabulary was chosen deliberately to keep that path open. **Pandera** was rejected: lighter and elegant, but no report artifact and no CI-summary story.

### 3.3 Why halt-on-validation-fail (not warn-and-continue)?

PRD FR-2 mandates it. It is far worse to silently ship wrong numbers to an exporter making a ₹2 Cr production decision than to delay a refresh by a day. Failure mode: pipeline halts *before* the DB load, last good snapshot stays live, alert email fires within minutes.

## 4. Modelling decisions

### 4.1 Why SARIMAX for demand forecasting (and the Prophet story)

**We chose Prophet first. It failed in this environment, and that failure is itself a documented engineering decision.**

Prophet was the initial choice (Fourier yearly seasonality, multiplicative mode, holiday effects — all attractive for the handicraft series). On this Windows machine Prophet's CmdStan backend could not build: the available MinGW toolchain is 32-bit (`cc1plus.exe: sorry, unimplemented: 64-bit mode not compiled in`), so the Stan model never compiled (`AttributeError: 'Prophet' object has no attribute 'stan_backend'`). It also broke the Streamlit Cloud deploy. Rather than fight the build, the project **migrated entirely to `statsmodels` SARIMAX** — and SARIMAX turned out to be the better fit here anyway:

- **Exogenous regressors are first-class.** The guar model needs rig-count + monsoon as `exog`; SARIMAX takes them directly with interpretable coefficients. Prophet's regressor support is clumsier.
- **No system build dependency.** Ships in `statsmodels`, installs as a wheel everywhere — including the 4-package Streamlit deploy.
- **Right tool for an AR commodity series.** Guar FOB is strongly autocorrelated; SARIMAX models that explicitly via the AR/MA terms.

Specification: `SARIMAX(1,0,1)(1,1,0,12)`. `d=0` because the ADF test shows the guar series is stationary (p=0.023) — an earlier `d=1` caused numerical blow-up; the fix was driven by the test, not guesswork. **Rejected:** LSTM/Transformer — gross overkill for ~72 monthly points; the CV MAPE would be unstable and unexplainable in a business brief. Deep learning here would be technical theatre.

**Honest accuracy:** rolling-origin CV MAPE ≈ 25% (NB02 25.0%, NB06 24.9%), which **misses the PRD's own <20% target**. This is reported in the notebooks, the README, and the dashboard — not hidden. The models are directionally useful with wide, honest confidence bands; the value is decision-framing and uncertainty quantification, not false precision.

### 4.2 Why K-means with k=4 for market segmentation

k=4 is a *business* choice (Core / Rising Star / Declining / Untapped → four distinct sales actions), sanity-checked against the elbow plot, not chosen purely mathematically. `StandardScaler` before fitting (CAGR is a fraction, FOB is in 10⁶ USD — without scaling K-means becomes argmax on FOB alone). On the real data: 130 countries → **Core 65, Declining 44, Untapped 12, Rising Star 9**.

The notebook adds a finding the PRD didn't ask for: a **22-market "Core-but-Declining" watchlist** — markets the model labels Core (high revenue) that carry *negative* CAGR (USA −15.1%, China −22.6%, Netherlands −11.9%, UK −7.9%). They look safe on a revenue report and erode quietly; this is the commercially dangerous segment. **Rejected:** DBSCAN (every country needs a strategy — no noise label wanted), Hierarchical/GMM (no interpretability gain at this size).

### 4.3 Why a weighted score (not logistic regression) for buyer payment risk

There are **no labelled defaults** in public trade data — no `y` to fit. Supervised learning would be calibrating a model against itself. The honest approach is a transparent weighted score over five shipment-derived signals: revenue trend (30%), unit-price volatility (25%), shipment irregularity (20%), HS concentration (15%), outlier rate (10%). Each is min-max scaled to [0,10]; the weighted sum is banded by the **score distribution's 60th/85th percentiles** (so bands are always populated — fixed thresholds initially produced zero High-risk markets). Result: 132 markets scored, **Low 76 / Medium 36 / High 20**, ₹363.8 Cr of historic revenue in the High band.

Every component is explainable to the exporter looking at the dashboard — far more defensible in interview than a black box. **Future v2:** with real default labels (ECGC claims or exporter A/R ageing), upgrade to logistic regression / gradient boosting. The weighted score is the correct choice for the data we *have*, not a compromise.

### 4.4 Why rig count + monsoon as guar regressors

NCDEX guar futures price is what we'd want to predict — using it as a regressor is circular. Rig count (US oilfield-services demand — the primary industrial use of guar) and Rajasthan monsoon (acreage/yield) are *upstream causal drivers*, both public, both leading the price signal. They enter the SARIMAX as **z-scored exogenous regressors** so the coefficients are directly comparable. The model confirms **rig count dominates monsoon** — a ₹1,540 Cr swing between the +30% and −30% rig scenarios vs. a far smaller monsoon effect. **Known limitation (documented, not hidden):** the curated sources are *annual* averages broadcast to monthly grain — no within-year seasonality and no calibrated lead-lag yet. The ingestion + schema are built for weekly Baker Hughes / monthly IMD data; swapping the source files in is a drop-in upgrade worth an estimated 2–5 MAPE points.

## 5. Dashboard design

**Decision: a deployed Streamlit app, not Power BI.** The original plan was Power BI Desktop; it was changed deliberately. For a portfolio piece the single most valuable property is *a recruiter can click one link and interact* — a `.pbix` file a reviewer must download and open in Windows-only software fails that test. `streamlit_app.py` is deployed on Streamlit Community Cloud as a public URL with five tabs (Overview → Seasonality debunk → Segmentation + watchlist → Price benchmark → Guar forecast with a live rig-count slider).

Two engineering decisions make the deploy bulletproof:
- **It reads only the committed `data/processed/` CSVs** — no database, no API key — so it behaves identically locally and in the cloud, and never shows a Supabase-paused error to a recruiter.
- **The SARIMAX forecast is pre-computed offline across all 17 rig scenarios** into a CSV, so the heaviest tab is instant and the deploy needs just **four pure-wheel packages** (streamlit, pandas, numpy, plotly) — zero compile steps. This is why the runtime requirements are split from the pipeline requirements (§6).

## 6. Reproducibility & operations

- **Split, fully-pinned dependencies.** `requirements-pipeline.txt` is the exact-pinned full pipeline/dev/CI environment (tested on Python 3.11). Root `requirements.txt` is the slim runtime set Streamlit Cloud auto-installs, with lower bounds so it resolves on any Python the runner uses. Splitting runtime from dev deps is deliberate, recognised practice — and it's why the deploy stopped failing (the full set pinned `prophet`, which can't build on the Streamlit runner).
- **Idempotent SQL DDL** — re-running `schema.sql` / `init_db.py` never errors or duplicates.
- **Date-stamped raw files** — the pipeline can be reconstructed from any point.
- **`pipeline_run` audit table** — observability without a separate log aggregator.
- **Pre-commit gate** — gitleaks (secret scan), ruff (lint+format), nbstripout (no output blobs in git).

## 7. What we explicitly chose NOT to build

| Not built | Why not | When to add |
|---|---|---|
| Multi-tenancy / per-company partitioning | v1 is cluster-level; per-company needs buyer-name onboarding | v2, after first customer |
| Real-time streaming | Strategic decisions don't need <1-day latency | Trader-facing use cases |
| ML model registry (MLflow) | Two models, weekly retrain, no team — manual versioning suffices | >5 models or multiple practitioners |
| Power BI / native BI tool | A clickable deployed link beats a downloadable `.pbix` for a portfolio | If a client needs Power BI specifically |
| dbt | Transform is ~400 LOC of Python | If transform > ~2k LOC or a 2nd engineer joins |
| Auth / RBAC | All data is public; no PII | Day 1 of any productisation |
| Company-level buyer risk (logistic regression) | No labelled defaults exist publicly | When real default labels are available |

---

## 8. AI pair-programming disclosure

This project was built with substantial AI pair-programming. Architectural decisions were made by **the human author with AI as sounding-board and code generator**:

- The PRD was authored independently before the build.
- Trade-off analyses (star schema vs. Data Vault; SARIMAX vs. Prophet; weighted-score vs. logistic regression; Streamlit vs. Power BI; split requirements) converged on the author's reading of the source material and the real operational constraints.
- Real bugs surfaced and were debugged jointly and are documented honestly: Prophet/CmdStan being unbuildable here (→ SARIMAX migration), the Streamlit Cloud dependency failure (→ requirements split), `d=1` numerical instability (→ ADF-driven `d=0`), risk bands producing zero High markets (→ percentile banding), Supabase pooler vs. direct connection.
- Every notebook's "Findings" cell reflects the author's interpretation of the actual model output (the numbers in them are the verified run outputs, not placeholders).

Hiding AI use is dishonest; being honest about it — and showing judgment about *when the tool was wrong* (Prophet) — is itself the skill being demonstrated.
