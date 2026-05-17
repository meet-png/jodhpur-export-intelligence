"""Jodhpur Export Intelligence System — interactive dashboard.

A clickable summary of the five analytical notebooks, built for a recruiter
to explore in 60 seconds. Reads only the version-controlled CSVs in
``data/processed/`` so it runs identically on Streamlit Community Cloud and
on a fresh clone — no database, no API keys required.

Run locally:   streamlit run streamlit_app.py
Deploy:        share.streamlit.io  →  point at this repo  →  main file
               = streamlit_app.py  (no secrets needed)
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA = Path(__file__).parent / "data" / "processed"
HANDICRAFT_HS = ["440929", "442090", "330749"]
GUAR_HS = ["130232", "130239"]
PEAK_MONTHS = [9, 10, 11]
INR_PER_USD = 83.0

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


def rs(cr: float) -> str:
    return f"₹{cr:,.0f} Cr"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("📦 JEIS")
st.sidebar.caption("Jodhpur Export Intelligence System")
page = st.sidebar.radio(
    "View",
    [
        "Overview",
        "Seasonality (the debunk)",
        "Market segmentation",
        "Price benchmark",
        "Guar demand forecast",
    ],
)
st.sidebar.markdown("---")
st.sidebar.info(
    "**Data as-of:** Dec 2024\n\n"
    "Source: UN Comtrade (India, 5 HS codes, 2019–2024) + Baker Hughes rig "
    "count + IMD monsoon. ~177 destination countries, 12,828 rows.\n\n"
    "UN Comtrade has a structural 6–18 month reporting lag — figures are "
    "decision-framing, not booked numbers."
)
st.sidebar.markdown(
    "[GitHub repo](https://github.com/meet-png/jodhpur-export-intelligence) · "
    "Built by Meet Kabra"
)


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

if page == "Overview":
    st.title("Jodhpur Export Intelligence System")
    st.markdown(
        "An end-to-end analytics pipeline for Jodhpur's furniture & guar-gum "
        "export cluster — **built entirely on free public data**. "
        "Three findings that each change a decision:"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Sep–Nov 'peak' at cluster level",
        "−8.0%",
        "the assumed peak is actually a trough",
        delta_color="inverse",
    )
    c2.metric(
        "Grade-adjusted price opportunity",
        "₹4,711 Cr",
        "vs ₹18,310 Cr naïve (74% was an artifact)",
        delta_color="inverse",
    )
    c3.metric(
        "Guar forecast rig-count swing",
        "₹1,540 Cr",
        "US drilling > monsoon as the driver",
    )

    st.markdown("---")
    st.subheader("Why these are the headline findings")
    st.markdown(
        "- **The seasonal assumption the industry plans around is wrong at the "
        "cluster level.** Guar gum (83% of revenue) runs a counter-cyclical "
        "oilfield demand pattern that swamps the genuine handicraft "
        "pre-Christmas peak. The conclusion is the *opposite* of the brief: "
        "run two production calendars, not one.\n"
        "- **A naïve price-gap model says ₹18,310 Cr is on the table — ~74% is "
        "a measurement artifact.** Morocco's 'guar gum' sells at 19× India's "
        "price because it's food/pharma-grade: a different product under the "
        "same HS code. Refusing to report the inflated number is the point.\n"
        "- **Guar export value tracks US drilling more than the monsoon.** A "
        "SARIMAX model with exogenous regressors quantifies a ₹1,540 Cr swing "
        "between high- and low-rig scenarios."
    )

    st.info(
        "**Honest note:** the forecast models score MAPE ≈ 25%, which misses "
        "the project's own <20% target. That is reported, not hidden. The "
        "value here is decision-framing and uncertainty quantification, not "
        "false precision — explore the tabs to see the working."
    )


# ---------------------------------------------------------------------------
# Page: Seasonality
# ---------------------------------------------------------------------------

elif page == "Seasonality (the debunk)":
    st.title("The September–November 'peak' does not exist at cluster level")
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
        "Aggregate cluster",
        f"{agg * 100:+.1f}%",
        "Sep–Nov vs annual avg",
        delta_color="inverse",
    )
    c2.metric("Handicrafts only", f"{hc * 100:+.1f}%", "genuine pre-Christmas peak")
    c3.metric(
        "Guar gum only",
        f"{gu * 100:+.1f}%",
        "counter-cyclical, drags aggregate",
        delta_color="inverse",
    )

    bar = go.Figure(
        go.Bar(
            x=["Aggregate cluster", "Handicrafts", "Guar gum"],
            y=[agg * 100, hc * 100, gu * 100],
            marker_color=["#264653", "#2a9d8f", "#e76f51"],
            text=[f"{v * 100:+.1f}%" for v in (agg, hc, gu)],
            textposition="outside",
        )
    )
    bar.update_layout(
        title="Sep–Nov premium vs annual average",
        yaxis_title="Premium (%)",
        showlegend=False,
        height=420,
    )
    bar.add_hline(y=0, line_color="gray")
    st.plotly_chart(bar, use_container_width=True)

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
        title="Monthly export value by category (Sep–Nov shaded)",
        color_discrete_map={"Guar gum": "#e76f51", "Handicrafts": "#2a9d8f"},
    )
    line.add_vrect(x0=8.5, x1=11.5, fillcolor="orange", opacity=0.12, line_width=0)
    line.update_layout(height=420, yaxis_title="FOB USD (summed 2019–2024)")
    st.plotly_chart(line, use_container_width=True)
    st.caption(
        "Actionable conclusion: handicraft producers should ramp for Sep–Nov; "
        "guar producers should ignore that heuristic and track US rig counts "
        "(see the Guar forecast tab). One cluster-wide calendar destroys value."
    )


# ---------------------------------------------------------------------------
# Page: Market segmentation
# ---------------------------------------------------------------------------

elif page == "Market segmentation":
    st.title("Market segmentation — K-means, k = 4")
    seg = load_segments()

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
            "cagr": "5-yr revenue CAGR (recomputed)",
            "last_year_fob_musd": "2024 FOB (USD M, log)",
        },
        title="Destination markets — growth vs current revenue",
        height=480,
    )
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("⚠️ Core-but-Declining watchlist")
    st.markdown(
        "The commercially dangerous segment: markets the model labels **Core** "
        "(high revenue, established) that are *also shrinking*. They look safe "
        "on a revenue report and erode quietly."
    )
    watch = (
        seg[(seg["cluster_label"] == "Core") & (seg["cagr"] < 0)]
        .sort_values("last_year_fob_usd", ascending=False)
        .copy()
    )
    watch["2024 FOB"] = (watch["last_year_fob_usd"] / 1e6).round(1).astype(
        str
    ) + " M USD"
    watch["CAGR"] = (watch["cagr"] * 100).round(1).astype(str) + "%"
    st.dataframe(
        watch[["country_name", "CAGR", "2024 FOB"]].rename(
            columns={"country_name": "Market"}
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        f"{len(watch)} Core markets carry negative growth. cluster_label is the "
        "authoritative K-means output from notebook 03 (written back to "
        "Postgres); CAGR is recomputed here as a directional indicator."
    )


# ---------------------------------------------------------------------------
# Page: Price benchmark
# ---------------------------------------------------------------------------

elif page == "Price benchmark":
    st.title("Price benchmark — and the ₹13,599 Cr that isn't real")
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
        title="Average unit price 2019–2024 (USD/kg) — India vs competitors",
        color_discrete_map={
            "India": "#e76f51",
            "Vietnam": "#2a9d8f",
            "Morocco": "#e9c46a",
        },
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)

    raw = ps[["opp_vs_vietnam_inr_cr", "opp_vs_morocco_inr_cr"]].sum().sum()
    mor_guar = ps[ps["hs_code"].isin(GUAR_HS)]["opp_vs_morocco_inr_cr"].sum()
    adjusted = raw - mor_guar

    c1, c2, c3 = st.columns(3)
    c1.metric("Naïve opportunity", rs(raw))
    c2.metric(
        "Morocco-guar artifact",
        f"−{rs(mor_guar)}",
        "product-grade mismatch",
        delta_color="inverse",
    )
    c3.metric("Grade-adjusted (defensible)", rs(adjusted))

    st.warning(
        "**Why Morocco guar is stripped:** Morocco's HS 130232 unit price is "
        f"~{ps[ps.hs_code == '130232'].morocco_usd_per_kg.iloc[0]:.0f}× India's "
        "— not because Morocco negotiates better, but because it exports "
        "food/pharma-grade guar derivatives. Same HS code, different product. "
        "Including that gap would inflate the opportunity ~4×. The defensible "
        "number is the grade-adjusted one."
    )

    st.subheader("FX sensitivity of the grade-adjusted figure")
    fx = pd.DataFrame(
        {
            "USD/INR": [80, 83, 85, 87],
            "Opportunity (₹ Cr)": [
                round(adjusted * r / INR_PER_USD) for r in (80, 83, 85, 87)
            ],
        }
    )
    st.dataframe(fx, use_container_width=True, hide_index=True)
    st.caption(
        "Every headline rupee figure ships with this table — it makes the FX "
        "assumption explicit instead of burying it."
    )


# ---------------------------------------------------------------------------
# Page: Guar demand forecast
# ---------------------------------------------------------------------------

elif page == "Guar demand forecast":
    st.title("Guar demand forecast — SARIMAX + rig count + monsoon")
    df = load_exports()
    rig = load_rig()
    mon = load_monsoon()

    guar = (
        df[df["hs_code"].isin(GUAR_HS)]
        .groupby(df["shipment_date"].dt.to_period("M").dt.to_timestamp())["fob_usd"]
        .sum()
    )
    guar.index = pd.DatetimeIndex(guar.index, freq="MS")

    rig["m"] = rig["week_start_date"].dt.to_period("M").dt.to_timestamp()
    rig_monthly = (
        rig.groupby("m")["rig_count"].mean().reindex(guar.index).ffill().bfill()
    )
    mon_map = dict(zip(mon["year"], mon["lpa_pct"]))
    mon_monthly = pd.Series(
        {d: mon_map.get(d.year, 100.0) for d in guar.index}, name="lpa"
    )

    rig_mean, rig_std = rig_monthly.mean(), rig_monthly.std()
    mon_mean, mon_std = mon_monthly.mean(), mon_monthly.std()
    rig_2024 = rig[rig["week_start_date"].dt.year == 2024]["rig_count"].mean()

    @st.cache_resource
    def fit_model(_y: pd.Series, _exog: pd.DataFrame):
        from statsmodels.tsa.statespace.sarimax import SARIMAX

        return SARIMAX(
            _y,
            exog=_exog,
            order=(1, 0, 1),
            seasonal_order=(1, 1, 0, 12),
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(disp=False)

    exog = pd.DataFrame(
        {
            "rig_z": (rig_monthly - rig_mean) / rig_std,
            "monsoon_z": (mon_monthly - mon_mean) / mon_std,
        },
        index=guar.index,
    )
    with st.spinner("Fitting SARIMAX (cached after first run)…"):
        res = fit_model(guar, exog)

    st.markdown(
        "Move the slider: it changes the assumed US rig count and re-forecasts "
        "the next 12 months live. This is the ₹1,540 Cr swing, made tangible."
    )
    scenario = st.slider("US rig count vs 2024 average", -40, 40, 0, 5, format="%d%%")

    horizon = 12
    future = pd.date_range(
        guar.index[-1] + pd.DateOffset(months=1), periods=horizon, freq="MS"
    )

    def fcast(rig_mult: float):
        ex = pd.DataFrame(
            {
                "rig_z": (rig_2024 * rig_mult - rig_mean) / rig_std,
                "monsoon_z": (100.0 - mon_mean) / mon_std,
            },
            index=future,
        )
        f = res.get_forecast(horizon, exog=ex)
        return f.predicted_mean.clip(lower=0), f.conf_int(alpha=0.10).clip(lower=0)

    base_mean, _ = fcast(1.0)
    sc_mean, sc_ci = fcast(1 + scenario / 100)

    c1, c2, c3 = st.columns(3)
    c1.metric("Base 12-mo forecast", rs(base_mean.sum() * INR_PER_USD / 1e7))
    c2.metric(
        f"Scenario ({scenario:+d}% rigs)",
        rs(sc_mean.sum() * INR_PER_USD / 1e7),
        rs((sc_mean.sum() - base_mean.sum()) * INR_PER_USD / 1e7),
    )
    c3.metric("Driver", "Rig count > monsoon", "confirmed by model coefficients")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=guar.index,
            y=guar / 1e6,
            name="Actual",
            mode="markers",
            marker=dict(color="#264653", size=4),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=future,
            y=sc_mean / 1e6,
            name=f"Forecast ({scenario:+d}% rigs)",
            line=dict(color="#2a9d8f", width=3),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=list(future) + list(future[::-1]),
            y=list(sc_ci.iloc[:, 1] / 1e6) + list(sc_ci.iloc[:, 0][::-1] / 1e6),
            fill="toself",
            fillcolor="rgba(42,157,143,0.15)",
            line=dict(width=0),
            name="90% CI",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=future,
            y=base_mean / 1e6,
            name="Base (0%)",
            line=dict(color="gray", dash="dot"),
        )
    )
    fig.update_layout(
        title="Guar gum (HS 130232+130239) — 12-month forecast",
        yaxis_title="FOB USD (millions)",
        height=480,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "SARIMAX(1,0,1)(1,1,0,12) with z-scored rig-count + monsoon exogenous "
        "regressors. MAPE ≈ 25% on rolling-origin CV — wide but honest bands. "
        "Forecast window is data-relative (12 months from Dec 2024)."
    )
