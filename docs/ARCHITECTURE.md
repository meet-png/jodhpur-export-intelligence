# Architecture & design decisions

This document is the *defensible* version of the project — the "why" behind every choice. If a reviewer asks "why did you do X instead of Y?", the answer lives here.

> The PRD (`docs/PRD.md`) describes *what* the system does. This file describes *why it's built that way*.

---

## 1. The problem framing

The five pain points the PRD enumerates are not equally weighted. After triangulating across the CRISIL 2023, FISME/GAME 2024, EPCH 2024–25, and IIM-A Hindustan Gum case study, two pain points dominate the rupee impact:

1. **Demand blindness** — directly causes 18–22 % of peak revenue to be unrealised, because production cycles are not seasonally calibrated.
2. **Working capital trapped in receivables** — directly causes 12–16 % p.a. interest drag on ~₹24 Cr of stranded cash for a typical ₹100 Cr exporter.

The remaining three (price gap, market concentration, guar volatility) are real but secondary. The system is therefore architected so that the demand-forecasting and buyer-risk modules get the cleanest data and the most robust modelling — the other modules are well-built but get less defensive engineering.

## 2. Data architecture

### 2.1 Storage: star schema in PostgreSQL (Supabase)

**Decision:** Kimball-style star schema with one fact table (`fact_shipment`) and four dimensions (country, product, company, time).

**Rejected alternatives & why:**

| Alternative | Why rejected |
|---|---|
| **Third-normal form (3NF) OLTP schema** | The dominant workload is analytical aggregation, not transactional. 3NF would force Power BI into 5–6-hop join paths for every panel and balloon query times. |
| **Data Vault 2.0** | Over-engineered. Data Vault's hub-link-satellite pattern shines with multiple conflicting source systems and heavy SCD requirements. We have one canonical source per entity and slow-changing dimensions. The bookkeeping overhead would dwarf the analytical value. |
| **Wide denormalised single table** | Tempting for simplicity but destroys the dimensional reuse — every panel that needs country labels would re-encode them, exploding storage and breaking referential integrity. |
| **dbt + One Big Table on DuckDB** | Modern, valid, and arguably "more 2026". But: (a) Power BI needs ODBC, easier with Postgres; (b) the resume value of "PostgreSQL + SQLAlchemy" is currently higher than "DuckDB" for Indian Data Engineer roles in 2026; (c) Supabase deploys real Postgres for free in `ap-south-1`. |

**SCD strategy:** Type 1 for `dim_country.cluster_label` and `dim_company.risk_score` — these are *recomputed* on every weekly run from the latest data. We do not need historical reconstruction of "what was country X's cluster label last March?" — the operational decision is always made on the current label. If a future v2 needs SCD2 history, we'd add `effective_from / effective_to` columns and a CDC trigger.

### 2.2 Hosting: Supabase free tier

**Decision:** Supabase Postgres in `ap-south-1` (Mumbai), free tier.

**Why:**
- Real Postgres 15+ — same SQL dialect, same drivers, same dashboard tooling
- Zero local install pain on Windows (the most common failure mode for early-career devs)
- Free tier covers our footprint comfortably (500 MB storage; our full dataset is ~80 MB)
- GitHub Actions can connect to it (a local-only Postgres cannot — this is the killer feature)
- Pauses after 7 days idle, but resumes instantly on first connection

**Trade-off accepted:** Supabase free tier pauses after a week of inactivity. The first dashboard load after a pause takes ~3 seconds. Acceptable for a portfolio project; documented in the dashboard's loading state.

### 2.3 File layout: raw → processed → load

`data/raw/` → date-stamped, never modified (PRD FR-1). This is the audit trail. Re-runs always go to a *new* file. This is the single most important data-engineering discipline in the project — interviewers probe for it because most beginners mutate raw files.

`data/processed/` → cleaned Parquet + canonical CSV. Parquet is ~10× smaller and ~3× faster to load than CSV; CSV exists for human inspection and for Git diffability of the small reference files.

## 3. Pipeline design

### 3.1 Why a procedural Python orchestrator (not Airflow / Prefect / Dagster)?

The full pipeline runs in <15 minutes once a week. Adopting Airflow for this would be like buying a forklift to carry one box. The complexity cost (Airflow scheduler, metastore, web server, DAG file conventions) far exceeds the value at this scale.

`src/run_pipeline.py` is a 100-line orchestrator that calls each layer in sequence with structured logging and a single `pipeline_run` audit row. It's readable in one screen. If the project ever grows to dozens of DAGs with branching dependencies, *that* is when we migrate to Airflow.

GitHub Actions provides everything an Airflow scheduler would at this scale: cron triggers, secrets, retries, alerting, free hosted compute.

### 3.2 Why Great Expectations and not Pandera / pytest-only?

Great Expectations (GE) was chosen for one specific feature: **the auto-generated Data Docs HTML report**. After every pipeline run, GE produces a static HTML page showing every expectation, its pass/fail status, the offending rows on failure, and a historical pass-rate trend. This is gold for the Power BI staleness panel and for interviewer demos.

Pandera is lighter and arguably more elegant code-wise, but produces no Data Docs. pytest gives us validation but no observability story. GE wins on the *demo* dimension.

### 3.3 Why halt-on-validation-fail (not warn-and-continue)?

PRD FR-2 explicitly mandates this. The reasoning: it is much worse to silently ship wrong numbers to an exporter making a ₹2 Cr production decision than to delay a dashboard refresh by a day. Failure mode: pipeline halts, previous week's data stays live, alert email fires within 5 minutes.

## 4. Modelling decisions

### 4.1 Why Prophet for demand forecasting?

The handicraft demand series has three signatures Prophet handles natively:
- **Strong yearly seasonality** (Sept–Nov peak) — Prophet's Fourier-based yearly term captures this without manual feature engineering
- **Multiplicative seasonality** (the peak amplitude scales with the trend) — `seasonality_mode='multiplicative'`
- **Holiday effects** (Christmas, Diwali, US Black Friday) — addable via `add_country_holidays`

**Rejected:** SARIMA (statsmodels) — viable but requires manual `(p,d,q)(P,D,Q)s` selection per HS code; doesn't auto-handle multiplicative seasonality; harder to interpret for a business audience. We keep SARIMA in `requirements.txt` as an explicit fallback because Prophet's pystan dependency occasionally breaks on Windows.

**Rejected:** LSTM / Transformer-based forecasting — gross overkill for ~60 monthly observations per series. The cross-validation MAPE on this data would be unstable, and we'd be unable to explain the model in a business brief. Deep learning here is technical theatre, not engineering.

### 4.2 Why K-means with k=4 for market segmentation?

The PRD mandates four clusters: Core, Rising, Declining, Untapped. The elbow plot will be inspected in the notebook but k=4 is a *business* choice, not a purely mathematical one — the four labels map directly to four distinct sales-team actions.

**Standardisation:** `StandardScaler` before K-means (CAGR is a percentage, order-value is in millions of USD — scales differ by 10⁶). Without scaling, K-means becomes an argmax on order-value alone.

**Rejected:** DBSCAN (no need for noise label here — every country gets a strategy), Hierarchical clustering (slower, no interpretability gain at this size), GMM (overkill given how clear the natural groupings are).

### 4.3 Why a weighted score (not a logistic regression) for buyer payment risk?

We don't have *labelled* defaults. We have shipment patterns and an ECGC country risk overlay. With no `y` to fit on, supervised learning is not applicable; we'd be calibrating a model against ourselves.

The honest approach: a transparent weighted scoring function (recency, frequency, average order value, country risk overlay) with weights chosen from documented MSME credit research. This is the same technique credit bureaus use for thin-file SME scoring. It's also far more defensible in interview: every score component is explainable to the exporter using the dashboard.

**Future v2:** if we obtain actual default labels (e.g. via ECGC claim records or exporter-supplied A/R ageing), upgrade to logistic regression or gradient boosting. Current method is the right choice for the data we *have*.

### 4.4 Why rig count + monsoon as guar regressors (and not the NCDEX futures price directly)?

The NCDEX futures price is what we want to predict — using it as a regressor would be circular. Rig count and monsoon are *upstream causal drivers*: rig count drives oilfield-services demand for guar (the primary US use), monsoon drives Rajasthan guar acreage and yield. Both are publicly available, both lead the price signal by 6–10 weeks, both are documented in the IIM-A Hindustan Gum case.

**The 8-week lag on rig count** is a tunable assumption, calibrated empirically by running `df['guar_value'].corr(df['rig_count'].shift(n))` for n in 0..16 and picking the lag that maximises correlation. The notebook shows this calibration explicitly.

## 5. Dashboard design

The Power BI dashboard is *not* the project — the analysis is. The dashboard is the showcase of the analysis. Five panels arranged as an executive narrative: KPI strip → seasonality → geography → pricing → risk → forecast. Each panel answers exactly one business question (PRD §9).

**Why Power BI Desktop and not Tableau / Looker / Metabase?**
- It's free (Tableau Public is free but cloud-only and exposes data publicly)
- Native Postgres connector (Looker Studio struggles with non-Google sources)
- Demonstrably the most-asked BI tool in Indian Data Analyst job postings
- The .pbix file commits cleanly to GitHub; reviewers can open it locally

## 6. Reproducibility & operations

The "3 commands to run" claim (clone → install → run) is enforced by:
- Pinned `requirements.txt`
- A working `.env.example` with every required variable
- Idempotent SQL DDL (re-running `schema.sql` doesn't error)
- Date-stamped raw files (the pipeline can resume from any point)
- `pipeline_run` audit table — observability without a separate log aggregator

If any of those four claims breaks, the project loses its core differentiator.

## 7. What we explicitly chose NOT to build

Listed here so reviewers see the discipline.

| Not built | Why not | When to add |
|---|---|---|
| Multi-tenancy / per-company partitioning | v1 is cluster-level; per-company would require buyer name onboarding | v2, after first paying customer |
| Real-time streaming | Strategic decisions don't need <1-day latency | If we ever serve trader-facing use cases |
| ML model registry (MLflow) | Two models, retrained weekly, no team — manual versioning suffices | When >5 models or multiple practitioners |
| Web frontend | Power BI Desktop is the dashboard layer per PRD §3.2 | If we productise as SaaS |
| dbt | Transform layer is ~400 LOC of Python, doesn't justify dbt's project structure | If transform grows past ~2k LOC or onboards a 2nd engineer |
| Authentication / authorisation | All data is public; no PII | Day 1 of any productisation |

---

## 8. AI pair-programming disclosure

This project was built with substantial use of AI pair-programming (Claude). The architectural decisions documented above were made by **the human author, with AI as sounding-board and code generator.** Specifically:

- The PRD was authored independently before the build.
- Trade-off analyses (star schema vs. Data Vault, Prophet vs. SARIMA, weighted-score vs. logistic regression) were debated with AI but converged based on the human author's reading of the source material (CRISIL, FISME, IIM-A) and the operational constraints (Windows-beginner dev environment, free-tier infrastructure).
- All code was reviewed line-by-line by the human author. Bugs that surfaced during real execution (Comtrade rate-limit edge cases, Prophet pystan install on Windows, Supabase pooler vs. direct connection) were debugged jointly.
- Every notebook's "Findings" cell is in the human author's voice and reflects the author's own interpretation of the model output.

The author can defend any line of code in this repository. Where AI generated boilerplate (e.g. ETL plumbing, Great Expectations suite scaffolding), the human author understood and modified it before commit.

This disclosure is here because hiding AI use is dishonest, and being honest about it is a strength in 2026 — judgement about modern tools is itself a skill.
