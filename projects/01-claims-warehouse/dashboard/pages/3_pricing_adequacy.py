"""Page 3 -- Pricing Adequacy from GLM Model.

Visualises the output of the P06 pricing-ML scoring pipeline.  If the
model_scoring table does not yet exist in BigQuery the page shows an
informational message instead of crashing.
"""

import plotly.express as px
import streamlit as st

from utils.bq_client import query_bq, DATASET_ANALYTICS

st.header("Pricing Adequacy")
st.markdown(
    "Results from the GLM-based pricing model.  Each policy is scored with a "
    "predicted pure premium and compared against the actual premium charged to "
    "determine whether pricing is adequate, under-priced, or over-priced."
)

try:
    sql = f"SELECT * FROM `{DATASET_ANALYTICS}`.model_scoring"
    df = query_bq(sql)
except Exception:
    st.info(
        "The model_scoring table does not exist yet in BigQuery. "
        "Run the P06 pricing-ML pipeline and upload scoring data first."
    )
    st.stop()

# -- Scatter: predicted vs actual premium -------------------------------------
if {"predicted_pure_premium", "actual_premium", "pricing_assessment"}.issubset(df.columns):
    fig_scatter = px.scatter(
        df,
        x="predicted_pure_premium",
        y="actual_premium",
        color="pricing_assessment",
        opacity=0.5,
        title="Predicted Pure Premium vs Actual Premium",
        labels={
            "predicted_pure_premium": "Predicted Pure Premium (MXN)",
            "actual_premium": "Actual Premium (MXN)",
            "pricing_assessment": "Assessment",
        },
        color_discrete_map={
            "underpriced": "#d62728",
            "overpriced": "#2ca02c",
            "adequate": "#1f77b4",
        },
    )
    fig_scatter.update_layout(height=500)
    st.plotly_chart(fig_scatter, use_container_width=True)

# -- Pie chart: assessment distribution ---------------------------------------
if "pricing_assessment" in df.columns:
    counts = df["pricing_assessment"].value_counts().reset_index()
    counts.columns = ["assessment", "count"]
    fig_pie = px.pie(
        counts,
        names="assessment",
        values="count",
        title="Pricing Assessment Distribution",
        color="assessment",
        color_discrete_map={
            "underpriced": "#d62728",
            "overpriced": "#2ca02c",
            "adequate": "#1f77b4",
        },
    )
    fig_pie.update_layout(height=400)
    st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# -- Bar: adequacy ratio by age band -----------------------------------------
col_left, col_right = st.columns(2)

if "price_adequacy_ratio" in df.columns and "age_band" in df.columns:
    age_agg = (
        df.groupby("age_band", as_index=False)["price_adequacy_ratio"]
        .mean()
        .sort_values("price_adequacy_ratio", ascending=False)
    )
    fig_age = px.bar(
        age_agg,
        x="age_band",
        y="price_adequacy_ratio",
        title="Avg Price Adequacy Ratio by Age Band",
        labels={
            "age_band": "Age Band",
            "price_adequacy_ratio": "Avg Adequacy Ratio",
        },
        text_auto=".2f",
    )
    fig_age.update_layout(height=400)
    with col_left:
        st.plotly_chart(fig_age, use_container_width=True)

# -- Bar: adequacy ratio by state risk group ----------------------------------
if "price_adequacy_ratio" in df.columns and "state_risk_group" in df.columns:
    state_agg = (
        df.groupby("state_risk_group", as_index=False)["price_adequacy_ratio"]
        .mean()
        .sort_values("price_adequacy_ratio", ascending=False)
    )
    fig_state = px.bar(
        state_agg,
        x="state_risk_group",
        y="price_adequacy_ratio",
        title="Avg Price Adequacy Ratio by State Risk Group",
        labels={
            "state_risk_group": "State Risk Group",
            "price_adequacy_ratio": "Avg Adequacy Ratio",
        },
        text_auto=".2f",
    )
    fig_state.update_layout(height=400)
    with col_right:
        st.plotly_chart(fig_state, use_container_width=True)

# -- Table: top 20 most underpriced policies ----------------------------------
if {"price_adequacy_ratio", "pricing_assessment"}.issubset(df.columns):
    st.subheader("Top 20 Most Underpriced Policies")
    st.markdown(
        "Policies with the highest adequacy ratio (predicted / actual), "
        "indicating the largest gap between required and charged premium."
    )
    underpriced = (
        df.loc[df["pricing_assessment"] == "underpriced"]
        .nlargest(20, "price_adequacy_ratio")
    )
    display_cols = [
        c
        for c in [
            "policy_id",
            "coverage_type",
            "age_band",
            "state_risk_group",
            "predicted_pure_premium",
            "actual_premium",
            "price_adequacy_ratio",
        ]
        if c in underpriced.columns
    ]
    st.dataframe(underpriced[display_cols].reset_index(drop=True), use_container_width=True)
