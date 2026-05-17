"""Jodhpur Export Intelligence System — interactive dashboard.

A clickable summary of the analysis, written so a non-technical visitor
understands every screen in seconds. Reads only the version-controlled CSVs
in ``data/processed/`` so it runs identically on Streamlit Community Cloud
and on a fresh clone — no database, no API keys required.

Run locally:   streamlit run streamlit_app.py
Deploy:        share.streamlit.io  →  point at this repo  →  main file
               = streamlit_app.py  (no secrets needed)
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA = Path(__file__).parent / "data" / "processed"
HANDICRAFT_HS = ["440929", "442090", "330749"]
GUAR_HS = ["130232", "130239"]
PEAK_MONTHS = [9, 10, 11]
INR_PER_USD = 83.0

# Narrative page labels — numbered so a visitor reads them in story order.
P_HOME = "Start here"
P_PEAK = "1 · The peak that isn't"
P_MIRAGE = "2 · The ₹13,600 Cr mirage"
P_MARKETS = "3 · Which markets to defend"
P_FORECAST = "4 · The 12-month forecast"

st.set_page_config(
    page_title="Jodhpur Export Intelligence",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Cached data loaders (committed CSVs only — robust on a fresh clone)
# ---------------------------------------------------------------------------


@st.cache_data
def load_exports() -> pd.DataFrame:
    df = pd.read_csv(DATA / "exports_clean.csv", parse_dates=["shipment_date"])
    df["hs_code"] = df["hs_code"].astype(str).str.zfill(6)
    return df


@st.cache_data
def load_price_summary() -> pd.DataFrame:
    df = pd.read_csv(DATA / "price_benchmark_summary.csv")
    df["hs_code"] = df["hs_code"].astype(str).str.zfill(6)
    return df


@st.cache_data
def load_segments() -> pd.DataFrame:
    return pd.read_csv(DATA / "country_segments.csv")


@st.cache_data
def load_rig() -> pd.DataFrame:
    return pd.read_csv(DATA / "rig_count_clean.csv", parse_dates=["week_start_date"])


@st.cache_data
def load_monsoon() -> pd.DataFrame:
    return pd.read_csv(DATA / "monsoon_clean.csv")


@st.cache_data
def load_forecast() -> pd.DataFrame:
    # Pre-computed offline by the SARIMAX model (one row per rig-scenario ×
    # forecast month) so the deployed tab is instant — no model fit, no
    # statsmodels/scipy in the deploy. Regenerated when the pipeline reruns.
    return pd.read_csv(DATA / "guar_forecast_precomputed.csv", parse_dates=["month"])


def rs(cr: float) -> str:
    return f"₹{cr:,.0f} Cr"


def takeaway(text: str) -> None:
    """One plain-English 'what this means for the business' line per tab."""
    st.success(f"**What this means for the business:** {text}")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("📦 JEIS")
st.sidebar.caption("Jodhpur Export Intelligence System")
st.sidebar.markdown("**New here? Start at the top and read down — it's a story.**")
page = st.sidebar.radio(
    "Go to",
    [P_HOME, P_PEAK, P_MIRAGE, P_MARKETS, P_FORECAST],
)
st.sidebar.markdown("---")
st.sidebar.info(
    "**Data:** free public sources only — UN Comtrade (India exports, "
    "2019–2024), Baker Hughes rig count, IMD monsoon. 12,828 records, "
    "~177 countries.\n\n"
    "**Honesty note:** trade data is reported with a 6–18 month lag, so "
    "these are decision-framing figures, not booked numbers. Every "
    "assumption is shown, not hidden."
)
st.sidebar.markdown(
    "[GitHub repo](https://github.com/meet-png/jodhpur-export-intelligence) · "
    "Built by Meet Kabra"
)
# Runtime version readout — diagnostic. Charts render blank on Plotly 6;
# this line makes the deployed versions visible at a glance.
st.sidebar.caption(f"runtime: plotly {plotly.__version__} · streamlit {st.__version__}")


# ---------------------------------------------------------------------------
# Start here
# ---------------------------------------------------------------------------

if page == P_HOME:
    st.title("Jodhpur Export Intelligence System")
    st.markdown(
        "A small set of businesses in Jodhpur export **$2.5B+** of furniture "
        "and guar gum, but they run on gut feel — *when* to produce, *what* to "
        "price, *which* buyers to trust. This is a system that turns 6 years of "
        "**free public trade data** into clear answers, refreshes itself every "
        "week, and is live for you to click."
    )

    st.markdown("#### The three findings, in one line each")
    c1, c2, c3 = st.columns(3)
    c1.metric(
        "The seasonal 'peak' is a myth",
        "−8.0%",
        "the window everyone plans for is below average",
        delta_color="inverse",
    )
    c2.metric(
        "A ₹18,310 Cr opportunity, mostly fake",
        "₹4,711 Cr",
        "74% was a measurement error — removed on purpose",
        delta_color="inverse",
    )
    c3.metric(
        "Guar revenue swing in 12 months",
        "₹1,540 Cr",
        "driven by US oil drilling, not the monsoon",
    )

    st.markdown("---")
    st.markdown(
        "**How to use this dashboard:** open the numbered tabs on the left, "
        "**in order**. Each one is a single finding, explained in plain English "
        "with the chart that proves it.\n\n"
        "1. **The peak that isn't** — why the industry's main production "
        "assumption is wrong.\n"
        "2. **The ₹13,600 Cr mirage** — why most of a 'pricing opportunity' "
        "isn't real, and why saying so matters.\n"
        "3. **Which markets to defend** — which export markets to grow, fix, "
        "or quietly worry about.\n"
        "4. **The 12-month forecast** — what's coming for guar gum, and the one "
        "external signal that moves it (drag the slider)."
    )

    st.info(
        "**The honest part:** the forecasts are ~25% off on average. That is "
        "stated openly on every relevant screen, not buried. The goal here is "
        "good decisions with honest uncertainty — not a number that looks "
        "precise and isn't."
    )


# ---------------------------------------------------------------------------
# 1 · The peak that isn't
# ---------------------------------------------------------------------------

elif page == P_PEAK:
    st.title("1 · The peak that isn't")
    takeaway(
        "The whole cluster plans production for a September–November rush. "
        "The data shows that window is actually *below* the yearly average — "
        "so one cluster-wide production calendar is the wrong plan. Handicraft "
        "and guar need separate plans."
    )
    df = load_exports()

    def monthly(sub: pd.DataFrame) -> pd.Series:
        return sub.groupby(sub["shipment_date"].dt.to_period("M"))["fob_usd"].sum()

    def premium(sub: pd.DataFrame) -> float:
        m = monthly(sub)
        mi = m.index.month
        return m[mi.isin(PEAK_MONTHS)].mean() / m.mean() - 1

    agg = premium(df)
    hc = premium(df[df["hs_code"].isin(HANDICRAFT_HS)])
    gu = premium(df[df["hs_code"].isin(GUAR_HS)])

    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Whole cluster",
        f"{agg * 100:+.1f}%",
        "Sep–Nov vs the yearly average",
        delta_color="inverse",
    )
    c2.metric("Handicrafts only", f"{hc * 100:+.1f}%", "real pre-Christmas rush")
    c3.metric(
        "Guar gum only",
        f"{gu * 100:+.1f}%",
        "moves the opposite way — and it's the bigger business",
        delta_color="inverse",
    )

    bar = go.Figure(
        go.Bar(
            x=["Whole cluster", "Handicrafts", "Guar gum"],
            y=[agg * 100, hc * 100, gu * 100],
            marker_color=["#264653", "#2a9d8f", "#e76f51"],
            text=[f"{v * 100:+.1f}%" for v in (agg, hc, gu)],
            textposition="outside",
        )
    )
    bar.update_layout(
        title="Is Sep–Nov busier or slower than usual?",
        yaxis_title="Difference vs yearly average (%)",
        showlegend=False,
        height=420,
    )
    bar.add_hline(y=0, line_color="gray")
    st.plotly_chart(bar, use_container_width=True)
    st.caption(
        "How to read this: a bar **above 0** means busier than usual in "
        "Sep–Nov; **below 0** means slower. The whole-cluster bar is "
        "**negative** — the 'peak' everyone plans for does not exist at the "
        "cluster level. Handicrafts do rise; guar gum (the larger business) "
        "falls and drags the blend down."
    )

    df["month"] = df["shipment_date"].dt.month
    seg = df.assign(
        group=np.where(
            df["hs_code"].isin(GUAR_HS),
            "Guar gum",
            np.where(df["hs_code"].isin(HANDICRAFT_HS), "Handicrafts", "Other"),
        )
    )
    by_month = (
        seg[seg["group"] != "Other"]
        .groupby(["month", "group"])["fob_usd"]
        .sum()
        .reset_index()
    )
    line = px.line(
        by_month,
        x="month",
        y="fob_usd",
        color="group",
        markers=True,
        title="Monthly export value by product (Sep–Nov shaded)",
        color_discrete_map={"Guar gum": "#e76f51", "Handicrafts": "#2a9d8f"},
    )
    line.add_vrect(x0=8.5, x1=11.5, fillcolor="orange", opacity=0.12, line_width=0)
    line.update_layout(height=420, yaxis_title="Export value, 2019–2024 total (USD)")
    st.plotly_chart(line, use_container_width=True)
    st.caption(
        "How to read this: the orange band is the Sep–Nov window. Handicrafts "
        "(green) climb into it — the genuine Christmas-stocking rush. Guar (red, "
        "the much bigger line) drops in the same window. Averaging them hides "
        "both signals — which is exactly the mistake this project fixes."
    )


# ---------------------------------------------------------------------------
# 2 · The ₹13,600 Cr mirage
# ---------------------------------------------------------------------------

elif page == P_MIRAGE:
    st.title("2 · The ₹13,600 Cr mirage")
    takeaway(
        "A quick model says exporters are leaving ₹18,310 Cr on the table by "
        "underpricing. Most of that is an illusion caused by comparing "
        "different products. The real, defensible number is ₹4,711 Cr — and "
        "refusing to report the inflated one is the actual skill on show here."
    )
    ps = load_price_summary()

    long = ps.melt(
        id_vars="hs_code",
        value_vars=["india_usd_per_kg", "vietnam_usd_per_kg", "morocco_usd_per_kg"],
        var_name="reporter",
        value_name="usd_per_kg",
    )
    long["reporter"] = long["reporter"].str.replace("_usd_per_kg", "").str.title()
    fig = px.bar(
        long,
        x="hs_code",
        y="usd_per_kg",
        color="reporter",
        barmode="group",
        title="Average selling price per kg, 2019–2024 — India vs competitors",
        color_discrete_map={
            "India": "#e76f51",
            "Vietnam": "#2a9d8f",
            "Morocco": "#e9c46a",
        },
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "How to read this: each cluster of bars is one product (by trade code). "
        "Where India's bar is far below the others, India is selling the same "
        "code much cheaper. That looks like lost money — but read the warning "
        "below for why the Morocco bars are misleading."
    )

    raw = ps[["opp_vs_vietnam_inr_cr", "opp_vs_morocco_inr_cr"]].sum().sum()
    mor_guar = ps[ps["hs_code"].isin(GUAR_HS)]["opp_vs_morocco_inr_cr"].sum()
    adjusted = raw - mor_guar

    c1, c2, c3 = st.columns(3)
    c1.metric("Naïve 'opportunity'", rs(raw), "what a quick model claims")
    c2.metric(
        "Removed as fake",
        f"−{rs(mor_guar)}",
        "wrong-product comparison",
        delta_color="inverse",
    )
    c3.metric("Honest, defensible figure", rs(adjusted), "the only number to report")

    st.warning(
        "**Why most of it is removed:** Morocco's guar (trade code 130232) "
        f"sells for ~{ps[ps.hs_code == '130232'].morocco_usd_per_kg.iloc[0]:.0f}× "
        "India's — not because Morocco negotiates better, but because it ships "
        "food/pharmaceutical-grade guar while India ships industrial "
        "(oil-drilling) grade. Same trade code, completely different product. "
        "Counting that gap as a 'pricing opportunity' would inflate the number "
        "~4×. So it is stripped out — that judgment call is the point of this "
        "page."
    )

    st.subheader(
        "Does the rupee figure depend on the exchange rate? Yes — shown openly"
    )
    fx = pd.DataFrame(
        {
            "Exchange rate (₹ per $1)": [80, 83, 85, 87],
            "Opportunity (₹ Cr)": [
                round(adjusted * r / INR_PER_USD) for r in (80, 83, 85, 87)
            ],
        }
    )
    st.dataframe(fx, use_container_width=True, hide_index=True)
    st.caption(
        "How to read this: if the rupee weakens, the figure rises in step. We "
        "show the whole range instead of quoting one number as if the exchange "
        "rate were fixed — the assumption is visible, not buried."
    )


# ---------------------------------------------------------------------------
# 3 · Which markets to defend
# ---------------------------------------------------------------------------

elif page == P_MARKETS:
    st.title("3 · Which markets to defend")
    takeaway(
        "Across ~170 export markets, this groups every country into 4 strategy "
        "buckets — and surfaces the dangerous ones: big markets that look "
        "healthy on a revenue report but are quietly shrinking every year."
    )
    seg = load_segments()
    st.markdown(
        "An algorithm (K-means clustering) sorts the ~170 destination markets "
        "into **4 groups** by how big they are today and how fast they're "
        "growing or shrinking — so each gets a different sales strategy."
    )

    counts = seg["cluster_label"].value_counts()
    cols = st.columns(len(counts))
    for col, (label, n) in zip(cols, counts.items()):
        col.metric(label, f"{n} countries")

    plot = seg.dropna(subset=["cagr", "last_year_fob_usd"]).copy()
    plot["last_year_fob_musd"] = plot["last_year_fob_usd"] / 1e6
    fig = px.scatter(
        plot,
        x="cagr",
        y="last_year_fob_musd",
        color="cluster_label",
        size="total_fob_usd",
        hover_name="country_name",
        log_y=True,
        labels={
            "cagr": "Annual growth rate (negative = shrinking)",
            "last_year_fob_musd": "2024 export value (USD millions, log scale)",
        },
        title="Every export market — growth vs current size",
        height=480,
    )
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "How to read this: **right = growing, left = shrinking; higher = more "
        "revenue today; bigger dot = bigger total customer.** The danger zone "
        "is the **top-left**: large markets that are declining. Hover any dot "
        "for the country."
    )

    st.subheader("⚠️ The watchlist nobody else flags")
    st.markdown(
        "These markets look **safe** on a revenue report — they're still big. "
        "But they shrink every year. A normal dashboard would never surface "
        "them; this one does, because that's where revenue quietly leaks."
    )
    watch = (
        seg[(seg["cluster_label"] == "Core") & (seg["cagr"] < 0)]
        .sort_values("last_year_fob_usd", ascending=False)
        .copy()
    )
    watch["2024 export value"] = (watch["last_year_fob_usd"] / 1e6).round(1).astype(
        str
    ) + " M USD"
    watch["Annual growth"] = (watch["cagr"] * 100).round(1).astype(str) + "%"
    st.dataframe(
        watch[["country_name", "Annual growth", "2024 export value"]].rename(
            columns={"country_name": "Market"}
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        f"How to read this: {len(watch)} large 'healthy' markets are actually "
        "declining year on year (negative growth). These deserve an "
        "executive-level account review before they're lost — the single most "
        "actionable list in the project."
    )


# ---------------------------------------------------------------------------
# 4 · The 12-month forecast
# ---------------------------------------------------------------------------

elif page == P_FORECAST:
    st.title("4 · The 12-month forecast")
    takeaway(
        "Guar gum is ~85% of the cluster's money. This predicts the next 12 "
        "months — and shows the swing depends far more on US oil-drilling "
        "activity than on the Indian monsoon. Drag the slider to see it move."
    )
    df = load_exports()
    fc = load_forecast()

    guar = (
        df[df["hs_code"].isin(GUAR_HS)]
        .groupby(df["shipment_date"].dt.to_period("M").dt.to_timestamp())["fob_usd"]
        .sum()
    )

    st.markdown(
        "**Drag the slider:** it sets how busy US oil drilling is next year. "
        "The forecast redraws instantly. The gap it opens up — about "
        "**₹1,540 Cr** between a strong and a weak drilling year — is the whole "
        "point: this is the lever guar exporters should watch."
    )
    scenario = st.slider(
        "US oil-drilling activity vs the 2024 average", -40, 40, 0, 5, format="%d%%"
    )

    sc = fc[fc["scenario_pct"] == scenario].sort_values("month")
    base = fc[fc["scenario_pct"] == 0].sort_values("month")

    c1, c2, c3 = st.columns(3)
    c1.metric("Normal year (forecast)", rs(base["yhat_usd"].sum() * INR_PER_USD / 1e7))
    c2.metric(
        f"Your scenario ({scenario:+d}% drilling)",
        rs(sc["yhat_usd"].sum() * INR_PER_USD / 1e7),
        rs((sc["yhat_usd"].sum() - base["yhat_usd"].sum()) * INR_PER_USD / 1e7),
    )
    c3.metric("Bigger lever", "US drilling", "more than the monsoon")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=guar.index,
            y=guar / 1e6,
            name="Actual (past)",
            mode="markers",
            marker=dict(color="#264653", size=4),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=sc["month"],
            y=sc["yhat_usd"] / 1e6,
            name=f"Forecast ({scenario:+d}% drilling)",
            line=dict(color="#2a9d8f", width=3),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=list(sc["month"]) + list(sc["month"][::-1]),
            y=list(sc["ci_high_usd"] / 1e6) + list(sc["ci_low_usd"][::-1] / 1e6),
            fill="toself",
            fillcolor="rgba(42,157,143,0.15)",
            line=dict(width=0),
            name="Realistic range",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=base["month"],
            y=base["yhat_usd"] / 1e6,
            name="Normal year",
            line=dict(color="gray", dash="dot"),
        )
    )
    fig.update_layout(
        title="Guar gum exports — next 12 months",
        yaxis_title="Monthly export value (USD millions)",
        height=480,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "How to read this: dark dots are real past data; the green line is the "
        "forecast for your slider setting; the shaded band is the realistic "
        "range (not a guarantee); the dotted grey line is a normal year for "
        "comparison.\n\n"
        "Under the hood it's a SARIMAX model (a standard statistical "
        "time-series method). Its average error is **~25%** — stated openly "
        "rather than hidden, because an honest range beats a falsely precise "
        "number."
    )
