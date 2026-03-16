"""Insurance Claims Analytics Dashboard -- main entry point.

A Streamlit multi-page app that connects to BigQuery and visualises
loss triangles, claim-frequency trends, pricing adequacy, and geographic
risk concentration for a synthetic Mexican insurance portfolio.
"""

import streamlit as st

st.set_page_config(
    page_title="Insurance Claims Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -- Sidebar ------------------------------------------------------------------
st.sidebar.title("Claims Analytics")
st.sidebar.markdown(
    """
    **Project 01 -- Claims Warehouse**

    Data source: BigQuery (`dev_claims_analytics`, `dev_claims_reports`)

    Region: `us-central1`

    Built with Streamlit, Plotly, and the Google Cloud BigQuery client.
    """
)

# -- Home page content --------------------------------------------------------
st.title("Insurance Claims Analytics Dashboard")
st.markdown(
    """
    An actuarial analytics platform built on BigQuery, visualising loss triangles,
    claim frequency trends, pricing adequacy, and geographic risk concentration
    for a Mexican insurance portfolio.

    **Data**: 100,000 policyholders | 160,000 policies | 125,000+ claims | 543,000+ payments

    Select a page from the sidebar to explore.
    """
)

# -- KPI cards ----------------------------------------------------------------
from utils.bq_client import query_bq, DATASET_ANALYTICS, DATASET_REPORTS  # noqa: E402

try:
    kpi_sql = f"""
    SELECT
        COUNT(DISTINCT claim_id)      AS total_claims,
        ROUND(SUM(total_paid), 2)     AS total_paid,
        ROUND(AVG(total_paid), 2)     AS avg_severity,
        COUNT(DISTINCT policyholder_id) AS unique_policyholders
    FROM `{DATASET_ANALYTICS}`.fct_claims
    """
    kpi = query_bq(kpi_sql)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Claims", f"{kpi['total_claims'].iloc[0]:,}")
    c2.metric("Total Paid (MXN)", f"${kpi['total_paid'].iloc[0]:,.2f}")
    c3.metric("Avg Severity (MXN)", f"${kpi['avg_severity'].iloc[0]:,.2f}")
    c4.metric("Unique Policyholders", f"{kpi['unique_policyholders'].iloc[0]:,}")
except Exception as exc:
    st.info(
        f"Could not load KPI summary from BigQuery. "
        f"Verify that the fct_claims table exists in {DATASET_ANALYTICS}.\n\n"
        f"Error: {exc}"
    )
