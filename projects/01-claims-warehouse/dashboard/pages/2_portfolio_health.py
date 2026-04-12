"""Page 2 -- Portfolio Health Dashboard.

Displays claim-frequency trends, pure-premium evolution, loss ratios by
coverage type, and top-level KPI cards.
"""

import plotly.express as px
import streamlit as st

from utils.bq_client import query_bq, DATASET_REPORTS

st.header("Portfolio Health")
st.markdown(
    "Key underwriting metrics over time, broken down by coverage type. "
    "Use the filters to focus on specific lines of business or time periods."
)

try:
    sql = (
        f"SELECT * FROM `{DATASET_REPORTS}`.rpt_claim_frequency "
        "ORDER BY year, coverage_type"
    )
    df = query_bq(sql)

    # -- Filters ---------------------------------------------------------------
    coverage_options = sorted(df["coverage_type"].unique().tolist())
    year_min, year_max = int(df["year"].min()), int(df["year"].max())

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        selected_coverages = st.multiselect(
            "Coverage Type",
            options=coverage_options,
            default=coverage_options,
        )
    with col_f2:
        year_range = st.slider(
            "Year Range",
            min_value=year_min,
            max_value=year_max,
            value=(year_min, year_max),
        )

    mask = (
        df["coverage_type"].isin(selected_coverages)
        & df["year"].between(year_range[0], year_range[1])
    )
    filtered = df.loc[mask].copy()

    if filtered.empty:
        st.warning("No data matches the selected filters.")
        st.stop()

    # -- KPI cards (latest year, all selected coverages) -----------------------
    latest_year = filtered["year"].max()
    latest = filtered.loc[filtered["year"] == latest_year]

    total_claims = int(latest["claim_count"].sum()) if "claim_count" in latest.columns else "N/A"
    total_exposure = (
        round(latest["exposure_years"].sum(), 1)
        if "exposure_years" in latest.columns
        else "N/A"
    )
    portfolio_lr = (
        round(
            latest["total_paid"].sum() / latest["earned_premium"].sum(), 4
        )
        if {"total_paid", "earned_premium"}.issubset(latest.columns)
        and latest["earned_premium"].sum() > 0
        else "N/A"
    )
    avg_severity = (
        round(latest["total_paid"].sum() / latest["claim_count"].sum(), 2)
        if {"total_paid", "claim_count"}.issubset(latest.columns)
        and latest["claim_count"].sum() > 0
        else "N/A"
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"Claims ({latest_year})", f"{total_claims:,}" if isinstance(total_claims, int) else total_claims)
    k2.metric(f"Exposure Years ({latest_year})", f"{total_exposure:,.1f}" if isinstance(total_exposure, float) else total_exposure)
    k3.metric(f"Loss Ratio ({latest_year})", f"{portfolio_lr:.2%}" if isinstance(portfolio_lr, float) else portfolio_lr)
    k4.metric(f"Avg Severity ({latest_year})", f"${avg_severity:,.2f}" if isinstance(avg_severity, float) else avg_severity)

    st.divider()

    # -- Claim frequency trend -------------------------------------------------
    if "claim_frequency" in filtered.columns:
        fig_freq = px.line(
            filtered,
            x="year",
            y="claim_frequency",
            color="coverage_type",
            markers=True,
            title="Claim Frequency by Year and Coverage Type",
            labels={
                "year": "Year",
                "claim_frequency": "Claim Frequency",
                "coverage_type": "Coverage",
            },
        )
        fig_freq.update_layout(height=400)
        st.plotly_chart(fig_freq, use_container_width=True)

    # -- Pure premium trend ----------------------------------------------------
    if "pure_premium" in filtered.columns:
        fig_pp = px.line(
            filtered,
            x="year",
            y="pure_premium",
            color="coverage_type",
            markers=True,
            title="Pure Premium (MXN) by Year and Coverage Type",
            labels={
                "year": "Year",
                "pure_premium": "Pure Premium (MXN)",
                "coverage_type": "Coverage",
            },
        )
        fig_pp.update_layout(height=400)
        st.plotly_chart(fig_pp, use_container_width=True)

    # -- Loss ratio bar chart (latest year) ------------------------------------
    if "loss_ratio" in latest.columns:
        fig_lr = px.bar(
            latest.sort_values("loss_ratio", ascending=False),
            x="coverage_type",
            y="loss_ratio",
            title=f"Loss Ratio by Coverage Type ({latest_year})",
            labels={
                "coverage_type": "Coverage Type",
                "loss_ratio": "Loss Ratio",
            },
            text_auto=".2%",
            color="coverage_type",
        )
        fig_lr.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_lr, use_container_width=True)

except Exception as exc:
    st.error(
        f"Could not load portfolio health data from BigQuery. "
        f"Verify that rpt_claim_frequency exists in {DATASET_REPORTS}.\n\n"
        f"Error: {exc}"
    )
