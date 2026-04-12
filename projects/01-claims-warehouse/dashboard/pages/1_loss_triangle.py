"""Page 1 -- Loss Development Triangle Heatmap.

Shows cumulative paid losses by accident year and development year, rendered
as a Plotly heatmap.  Below the heatmap a table of development factors
(column-over-column ratios) is displayed.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import query_bq, DATASET_REPORTS

st.header("Loss Development Triangle")
st.markdown(
    "Cumulative paid losses by accident year and development year. "
    "The staircase pattern shows how older years are more fully developed, "
    "while recent years still have significant IBNR (Incurred But Not Reported) reserves."
)

try:
    sql = (
        f"SELECT * FROM `{DATASET_REPORTS}`.rpt_loss_triangle "
        "ORDER BY accident_year"
    )
    df = query_bq(sql)

    # Identify development-year columns
    dev_cols = sorted(
        [c for c in df.columns if c.startswith("dev_year_")],
        key=lambda c: int(c.split("_")[-1]),
    )
    matrix = df[dev_cols].values.astype(float)
    y_labels = df["accident_year"].astype(str).tolist()
    x_labels = [f"Dev {c.split('_')[-1]}" for c in dev_cols]

    # -- Heatmap ---------------------------------------------------------------
    text_matrix = [
        [f"MXN {v:,.0f}" if pd.notna(v) and v > 0 else "" for v in row]
        for row in matrix
    ]

    fig = go.Figure(
        data=go.Heatmap(
            z=matrix,
            x=x_labels,
            y=y_labels,
            colorscale="Blues",
            text=text_matrix,
            texttemplate="%{text}",
            hovertemplate="AY %{y}, %{x}: %{z:,.0f} MXN<extra></extra>",
        )
    )
    fig.update_layout(
        title="Cumulative Paid Losses (MXN)",
        yaxis_title="Accident Year",
        xaxis_title="Development Year",
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)

    # -- Development factors ---------------------------------------------------
    st.subheader("Development Factors")
    st.markdown(
        "Column-over-column ratios indicating how losses develop from one "
        "period to the next. A factor of 1.00 means no additional development."
    )

    factors: dict[str, list[float | None]] = {}
    for i in range(1, len(dev_cols)):
        col_prev = matrix[:, i - 1]
        col_curr = matrix[:, i]
        ratios = []
        for prev, curr in zip(col_prev, col_curr):
            if pd.notna(prev) and pd.notna(curr) and prev > 0 and curr > 0:
                ratios.append(round(curr / prev, 4))
            else:
                ratios.append(None)
        factors[f"{x_labels[i-1]} -> {x_labels[i]}"] = ratios

    factors_df = pd.DataFrame(factors, index=y_labels)
    factors_df.index.name = "Accident Year"

    # Weighted-average factor row
    avg_row = {}
    for col_name, i in zip(factors.keys(), range(1, len(dev_cols))):
        col_prev = matrix[:, i - 1]
        col_curr = matrix[:, i]
        mask = (
            pd.notna(col_prev) & pd.notna(col_curr) & (col_prev > 0) & (col_curr > 0)
        )
        if mask.any():
            avg_row[col_name] = round(col_curr[mask].sum() / col_prev[mask].sum(), 4)
        else:
            avg_row[col_name] = None

    avg_series = pd.DataFrame(avg_row, index=["Weighted Avg"])
    factors_display = pd.concat([factors_df, avg_series])

    st.dataframe(factors_display, use_container_width=True)

    st.markdown(
        """
        ---
        **Reading the triangle**

        - Each row represents an *accident year* (when the loss event occurred).
        - Each column represents a *development year* (elapsed time since the accident).
        - Older accident years have values across more development periods because
          they have had more time to settle.
        - The staircase of missing cells in the lower-right corner is *IBNR*: losses
          that have occurred but are not yet fully paid.
        - Actuaries use the development factors to project ultimate losses and set
          reserves.
        """
    )

except Exception as exc:
    st.error(
        f"Could not load loss triangle from BigQuery. "
        f"Verify that rpt_loss_triangle exists in {DATASET_REPORTS}.\n\n"
        f"Error: {exc}"
    )
