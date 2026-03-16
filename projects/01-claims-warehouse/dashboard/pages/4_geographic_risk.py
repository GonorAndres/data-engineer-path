"""Page 4 -- Geographic Risk Map of Mexico.

Displays claim counts and average severity by Mexican state.  A choropleth
map is attempted first using a public GeoJSON of Mexican states; if that
fails the page falls back to a horizontal bar chart.
"""

import plotly.express as px
import streamlit as st

from utils.bq_client import query_bq, DATASET_ANALYTICS

st.header("Geographic Risk Concentration")
st.markdown(
    "Claim volume and average severity by Mexican state.  States with higher "
    "concentration of severe claims represent areas of elevated portfolio risk."
)

# -- Filters ------------------------------------------------------------------
try:
    meta_sql = f"""
    SELECT DISTINCT c.accident_year, p.state_code
    FROM `{DATASET_ANALYTICS}`.fct_claims c
    JOIN `{DATASET_ANALYTICS}`.dim_policyholder p
      ON c.policyholder_id = p.policyholder_id
    ORDER BY c.accident_year
    """
    meta = query_bq(meta_sql)

    year_min, year_max = int(meta["accident_year"].min()), int(meta["accident_year"].max())
    state_options = sorted(meta["state_code"].dropna().unique().tolist())
except Exception:
    year_min, year_max = 2020, 2025
    state_options = []

col_f1, col_f2 = st.columns(2)
with col_f1:
    year_range = st.slider(
        "Accident Year Range",
        min_value=year_min,
        max_value=year_max,
        value=(year_min, year_max),
    )
with col_f2:
    selected_states = st.multiselect(
        "State (leave empty for all)",
        options=state_options,
        default=[],
    )

# -- Main query ---------------------------------------------------------------
state_filter = ""
if selected_states:
    state_list = ", ".join(f"'{s}'" for s in selected_states)
    state_filter = f"AND p.state_code IN ({state_list})"

sql = f"""
SELECT
    p.state_code,
    COUNT(c.claim_id)               AS claim_count,
    ROUND(AVG(c.total_paid), 2)     AS avg_severity,
    ROUND(SUM(c.total_paid), 2)     AS total_paid
FROM `{DATASET_ANALYTICS}`.fct_claims c
JOIN `{DATASET_ANALYTICS}`.dim_policyholder p
  ON c.policyholder_id = p.policyholder_id
WHERE c.accident_year BETWEEN {year_range[0]} AND {year_range[1]}
  {state_filter}
GROUP BY p.state_code
ORDER BY claim_count DESC
"""

try:
    df = query_bq(sql)
except Exception as exc:
    st.error(
        f"Could not load geographic data from BigQuery. "
        f"Verify that fct_claims and dim_policyholder exist in {DATASET_ANALYTICS}.\n\n"
        f"Error: {exc}"
    )
    st.stop()

if df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# -- KPI cards ----------------------------------------------------------------
k1, k2, k3 = st.columns(3)
k1.metric("States with Claims", f"{df['state_code'].nunique()}")
k2.metric("Total Claims", f"{df['claim_count'].sum():,}")
k3.metric("Total Paid (MXN)", f"${df['total_paid'].sum():,.2f}")

st.divider()

# -- Mexico GeoJSON mapping ---------------------------------------------------
# Mexican state abbreviations to state names (for GeoJSON matching).
STATE_NAMES = {
    "AGU": "Aguascalientes", "BCN": "Baja California", "BCS": "Baja California Sur",
    "CAM": "Campeche", "CHP": "Chiapas", "CHH": "Chihuahua",
    "COA": "Coahuila de Zaragoza", "COL": "Colima", "CDMX": "Ciudad de Mexico",
    "DUR": "Durango", "GUA": "Guanajuato", "GRO": "Guerrero",
    "HID": "Hidalgo", "JAL": "Jalisco", "MEX": "Mexico",
    "MIC": "Michoacan de Ocampo", "MOR": "Morelos", "NAY": "Nayarit",
    "NLE": "Nuevo Leon", "OAX": "Oaxaca", "PUE": "Puebla",
    "QUE": "Queretaro", "ROO": "Quintana Roo", "SLP": "San Luis Potosi",
    "SIN": "Sinaloa", "SON": "Sonora", "TAB": "Tabasco",
    "TAM": "Tamaulipas", "TLA": "Tlaxcala", "VER": "Veracruz de Ignacio de la Llave",
    "YUC": "Yucatan", "ZAC": "Zacatecas",
    # Alternate abbreviations that the synthetic data may use
    "NL": "Nuevo Leon", "QRO": "Queretaro", "QROO": "Quintana Roo",
    "MICH": "Michoacan de Ocampo", "CHIS": "Chiapas", "CHIH": "Chihuahua",
    "COAH": "Coahuila de Zaragoza", "DF": "Ciudad de Mexico",
}

df["state_name"] = df["state_code"].map(STATE_NAMES).fillna(df["state_code"])

# -- Charts --------------------------------------------------------------------
col_map, col_bar = st.columns([3, 2])

# Bar chart (always shown -- reliable fallback)
with col_bar:
    fig_bar = px.bar(
        df.sort_values("claim_count", ascending=True).tail(20),
        x="claim_count",
        y="state_code",
        orientation="h",
        title="Top 20 States by Claim Count",
        labels={"claim_count": "Claim Count", "state_code": "State"},
        text_auto=True,
        color="avg_severity",
        color_continuous_scale="Reds",
    )
    fig_bar.update_layout(height=550, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_bar, use_container_width=True)

# Attempt choropleth map
with col_map:
    GEOJSON_URL = (
        "https://raw.githubusercontent.com/angelnmara/geojson/master/mexicoHigh.json"
    )
    try:
        import json
        import urllib.request

        @st.cache_data(ttl=86400)
        def _load_geojson(url: str):
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.loads(resp.read().decode())

        mx_geo = _load_geojson(GEOJSON_URL)

        fig_map = px.choropleth(
            df,
            geojson=mx_geo,
            locations="state_name",
            featureidkey="properties.name",
            color="claim_count",
            color_continuous_scale="Blues",
            title="Claims by State",
            hover_data={"avg_severity": ":,.2f", "total_paid": ":,.2f"},
        )
        fig_map.update_geos(fitbounds="locations", visible=False)
        fig_map.update_layout(height=550, margin={"l": 0, "r": 0, "t": 40, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)

    except Exception:
        st.info(
            "Choropleth map could not be rendered (GeoJSON download may have "
            "failed). The bar chart on the right provides the same ranking."
        )

# -- Severity table ------------------------------------------------------------
st.subheader("State-Level Detail")
st.dataframe(
    df[["state_code", "state_name", "claim_count", "avg_severity", "total_paid"]]
    .sort_values("total_paid", ascending=False)
    .reset_index(drop=True)
    .style.format(
        {"avg_severity": "${:,.2f}", "total_paid": "${:,.2f}", "claim_count": "{:,}"}
    ),
    use_container_width=True,
)
