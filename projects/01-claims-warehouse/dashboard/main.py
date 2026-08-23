"""Insurance Claims Analytics Dashboard -- FastAPI application."""

import asyncio
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware
from utils.analytics import CANONICAL_HOST, POSTHOG_SNIPPET
from utils.bq_client import DATASET_ANALYTICS, DATASET_REPORTS, query_bq

log = logging.getLogger("dashboard")

_WARM_QUERIES = [
    f"SELECT COUNT(DISTINCT claim_id) AS c FROM `{DATASET_ANALYTICS}`.fct_claims",
    f"SELECT * FROM `{DATASET_REPORTS}`.rpt_loss_triangle ORDER BY accident_year",
    f"SELECT * FROM `{DATASET_REPORTS}`.rpt_claim_frequency ORDER BY year, coverage_type",
    f"""SELECT p.state_code, c.accident_year, COUNT(c.claim_id) AS claim_count,
        ROUND(AVG(c.total_paid),2) AS avg_severity, ROUND(SUM(c.total_paid),2) AS total_paid
        FROM `{DATASET_ANALYTICS}`.fct_claims c
        JOIN `{DATASET_ANALYTICS}`.dim_policyholder p ON c.policyholder_id=p.policyholder_id
        GROUP BY p.state_code, c.accident_year ORDER BY p.state_code, c.accident_year""",
]


def _warm_cache():
    for sql in _WARM_QUERIES:
        try:
            query_bq(sql)
        except Exception as exc:
            log.warning("Cache warm failed: %s", exc)


# Set in the deploy workflow only once data-engineer.gonor.me resolves. Unset
# means "serve on whatever host asked", which is what local development and the
# window before DNS propagates both need -- an unconditional redirect here would
# take the dashboard down for as long as the new hostname was not answering.
REDIRECT_TO_CANONICAL = os.getenv("CANONICAL_REDIRECT_HOST", "").strip()

# PostHog ingestion endpoints. `/ingest/static/*` serves the library itself and
# lives on a different host than the event endpoint.
POSTHOG_API_ORIGIN = "https://us.i.posthog.com"
POSTHOG_ASSET_ORIGIN = "https://us-assets.i.posthog.com"


@asynccontextmanager
async def lifespan(app_: FastAPI):
    loop = asyncio.get_event_loop()
    loop.run_in_executor(ThreadPoolExecutor(1), _warm_cache)
    app_.state.http = httpx.AsyncClient(timeout=10.0, follow_redirects=False)
    try:
        yield
    finally:
        await app_.state.http.aclose()


app = FastAPI(title="Claims Analytics", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

STATE_NAMES = {
    "AGU": "Aguascalientes", "BCN": "Baja California",
    "BCS": "Baja California Sur", "CAM": "Campeche", "CHP": "Chiapas",
    "CHH": "Chihuahua", "COA": "Coahuila", "COL": "Colima",
    "CDMX": "Ciudad de México", "DUR": "Durango", "GUA": "Guanajuato",
    "GRO": "Guerrero", "HID": "Hidalgo", "JAL": "Jalisco",
    "MEX": "México", "MIC": "Michoacán", "MOR": "Morelos",
    "NAY": "Nayarit", "NLE": "Nuevo León", "OAX": "Oaxaca",
    "PUE": "Puebla", "QUE": "Querétaro", "ROO": "Quintana Roo",
    "SLP": "San Luis Potosí", "SIN": "Sinaloa", "SON": "Sonora",
    "TAB": "Tabasco", "TAM": "Tamaulipas", "TLA": "Tlaxcala",
    "VER": "Veracruz", "YUC": "Yucatán", "ZAC": "Zacatecas",
    "NL": "Nuevo León", "QRO": "Querétaro", "QROO": "Quintana Roo",
    "MICH": "Michoacán", "CHIS": "Chiapas",
    "CHIH": "Chihuahua", "COAH": "Coahuila",
    "DF": "Ciudad de México",
    "AGS": "Aguascalientes", "BC": "Baja California",
    "DGO": "Durango", "GTO": "Guanajuato", "HGO": "Hidalgo",
    "TAMS": "Tamaulipas", "TLAX": "Tlaxcala",
}


def _clean(obj):
    """Make obj JSON-serializable (handle NaN, numpy types, ndarray)."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        v = float(obj)
        return None if math.isnan(v) or math.isinf(v) else v
    if isinstance(obj, np.ndarray):
        return _clean(obj.tolist())
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    return obj


def _ctx(active_page: str, **extra):
    return {
        "active_page": active_page,
        "posthog_snippet": POSTHOG_SNIPPET,
        "canonical_host": CANONICAL_HOST,
        **extra,
    }


# Paths that must answer on whatever hostname asked for them, never redirect.
#
#   /ingest  -- a 301 on a POST loses the body in most clients, which would
#               silently drop analytics events from any page still loaded on a
#               run.app hostname.
#   /health  -- an operational endpoint. Uptime checks and the post-deploy smoke
#               test address a revision directly and need its own answer, not a
#               redirect to whatever the canonical host currently serves.
NO_REDIRECT_PREFIXES = ("/ingest", "/health")


def _is_canary_host(host: str) -> bool:
    """True for a Cloud Run tagged-revision hostname.

    Tagged revisions are addressed as `<tag>---<service>-<hash>.a.run.app`. That
    triple hyphen cannot occur in a service name -- Cloud Run rejects consecutive
    hyphens -- so it is an unambiguous marker.

    These have to be exempt. The deploy pipeline ships each revision with no
    traffic, smoke-tests it on its tagged URL, and only then moves traffic over.
    Redirecting that URL would send the smoke test to the *currently live*
    revision, so every deploy would test the old code and pass while shipping
    anything at all.
    """
    return "---" in host


@app.middleware("http")
async def canonical_host_redirect(request: Request, call_next):
    """301 the Cloud Run hostnames onto the custom domain.

    Cloud Run keeps answering on both `<service>-<hash>-<region>.a.run.app` and
    `<service>-<project-number>.<region>.run.app` after a domain mapping is added --
    the mapping is an extra route in, never a replacement. Left alone that splits
    traffic across three hostnames and scatters the search ranking. Matching the
    `.run.app` suffix covers both forms without enumerating them.
    """
    if REDIRECT_TO_CANONICAL and not request.url.path.startswith(NO_REDIRECT_PREFIXES):
        host = request.url.hostname or ""
        if host.endswith(".run.app") and not _is_canary_host(host):
            target = request.url.replace(scheme="https", netloc=REDIRECT_TO_CANONICAL)
            return RedirectResponse(str(target), status_code=301)
    return await call_next(request)


@app.api_route("/ingest/{path:path}", methods=["GET", "POST", "OPTIONS"])
async def posthog_proxy(request: Request, path: str):
    """Same-origin reverse proxy for PostHog.

    Requests to `us.i.posthog.com` are on every adblock list, and blocking them
    costs both the events and the lazily-loaded session-replay recorder. A
    first-party path is not recognisable as analytics and survives.
    """
    origin = POSTHOG_ASSET_ORIGIN if path.startswith("static/") else POSTHOG_API_ORIGIN
    url = f"{origin}/{path}"
    # Host would name this service, and Accept-Encoding invites a compressed body
    # that we would hand back with the wrong Content-Length.
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "accept-encoding", "content-length")
    }
    try:
        upstream = await request.app.state.http.request(
            request.method, url,
            content=await request.body(),
            headers=headers,
            params=request.query_params,
        )
    except httpx.HTTPError as exc:
        # Analytics must never take a page down with it.
        log.warning("PostHog proxy failed for %s: %s", path, exc)
        return Response(status_code=502)

    passthrough = {
        k: v for k, v in upstream.headers.items()
        if k.lower() in ("content-type", "cache-control", "access-control-allow-origin")
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=passthrough,
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/how-its-built", response_class=HTMLResponse)
async def how_its_built(request: Request):
    return templates.TemplateResponse(
        request, "how_its_built.html",
        context=_ctx("how_its_built"),
    )


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    kpis = None
    error = None
    try:
        sql = f"""
        SELECT
            COUNT(DISTINCT claim_id)        AS total_claims,
            ROUND(SUM(total_paid), 2)       AS total_paid,
            ROUND(AVG(total_paid), 2)       AS avg_severity,
            COUNT(DISTINCT policyholder_id)  AS unique_policyholders
        FROM `{DATASET_ANALYTICS}`.fct_claims
        """
        df = query_bq(sql)
        row = df.iloc[0]
        kpis = {
            "total_claims": int(row["total_claims"]),
            "total_paid": float(row["total_paid"]),
            "avg_severity": float(row["avg_severity"]),
            "unique_policyholders": int(row["unique_policyholders"]),
        }
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request, "home.html", context=_ctx("home", kpis=kpis, error=error)
    )


@app.get("/loss-triangle", response_class=HTMLResponse)
async def loss_triangle(request: Request):
    chart_data = None
    factors_data = None
    error = None
    try:
        sql = (
            f"SELECT * FROM `{DATASET_REPORTS}`.rpt_loss_triangle "
            "ORDER BY accident_year"
        )
        df = query_bq(sql)

        dev_cols = sorted(
            [c for c in df.columns if c.startswith("dev_year_")],
            key=lambda c: int(c.split("_")[-1]),
        )
        matrix = df[dev_cols].values.astype(float)
        y_labels = df["accident_year"].astype(str).tolist()
        x_labels = [f"Dev {c.split('_')[-1]}" for c in dev_cols]

        text_matrix = [
            [f"MXN {v:,.0f}" if pd.notna(v) and v > 0 else "" for v in row]
            for row in matrix
        ]

        chart_data = _clean({
            "z": matrix.tolist(),
            "x": x_labels,
            "y": y_labels,
            "text": text_matrix,
        })

        factors_rows = []
        for i in range(1, len(dev_cols)):
            col_prev = matrix[:, i - 1]
            col_curr = matrix[:, i]
            ratios = []
            for prev, curr in zip(col_prev, col_curr):
                if pd.notna(prev) and pd.notna(curr) and prev > 0 and curr > 0:
                    ratios.append(round(curr / prev, 4))
                else:
                    ratios.append(None)
            factors_rows.append({
                "label": f"{x_labels[i-1]} -> {x_labels[i]}",
                "ratios": ratios,
            })

        weighted_avgs = []
        for i in range(1, len(dev_cols)):
            col_prev = matrix[:, i - 1]
            col_curr = matrix[:, i]
            mask = (
                pd.notna(col_prev) & pd.notna(col_curr)
                & (col_prev > 0) & (col_curr > 0)
            )
            if mask.any():
                weighted_avgs.append(
                    round(col_curr[mask].sum() / col_prev[mask].sum(), 4)
                )
            else:
                weighted_avgs.append(None)

        factors_data = _clean({
            "y_labels": y_labels,
            "columns": [r["label"] for r in factors_rows],
            "rows": [r["ratios"] for r in factors_rows],
            "weighted_avgs": weighted_avgs,
        })

    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request, "loss_triangle.html",
        context=_ctx(
            "loss_triangle",
            chart_data=chart_data, factors_data=factors_data, error=error,
        ),
    )


@app.get("/portfolio-health", response_class=HTMLResponse)
async def portfolio_health(request: Request):
    dataset = None
    error = None
    try:
        sql = (
            f"SELECT * FROM `{DATASET_REPORTS}`.rpt_claim_frequency "
            "ORDER BY year, coverage_type"
        )
        df = query_bq(sql)
        records = df.to_dict(orient="records")
        dataset = _clean({
            "records": records,
            "coverages": sorted(df["coverage_type"].unique().tolist()),
            "year_min": int(df["year"].min()),
            "year_max": int(df["year"].max()),
            "columns": df.columns.tolist(),
        })
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request, "portfolio_health.html",
        context=_ctx("portfolio_health", dataset=dataset, error=error),
    )


@app.get("/pricing-adequacy", response_class=HTMLResponse)
async def pricing_adequacy(request: Request):
    chart_data = None
    error = None
    available = False
    try:
        _ds = "dev_pricing_ml"

        scatter_sql = f"""
        SELECT predicted_pure_premium, actual_premium, pricing_assessment
        FROM `{_ds}`.model_scoring
        WHERE RAND() < 2000.0 / (SELECT COUNT(*) FROM `{_ds}`.model_scoring)
        """
        scatter_df = query_bq(scatter_sql)
        available = True

        color_map = {
            "underpriced": "#EF4444",
            "adequate": "#3B82F6",
            "overpriced": "#10B981",
        }
        scatter = []
        for assessment in scatter_df["pricing_assessment"].unique():
            sub = scatter_df[scatter_df["pricing_assessment"] == assessment]
            scatter.append({
                "x": sub["predicted_pure_premium"].tolist(),
                "y": sub["actual_premium"].tolist(),
                "name": assessment,
                "color": color_map.get(assessment, "#94A3B8"),
            })

        pie_sql = f"""
        SELECT pricing_assessment, COUNT(*) AS cnt
        FROM `{_ds}`.model_scoring
        GROUP BY pricing_assessment
        """
        pie_df = query_bq(pie_sql)
        pie = {
            "labels": pie_df["pricing_assessment"].tolist(),
            "values": pie_df["cnt"].tolist(),
            "colors": [
                color_map.get(label, "#94A3B8")
                for label in pie_df["pricing_assessment"]
            ],
        }

        agg_sql = f"""
        SELECT
            age_band,
            state_risk_group,
            AVG(price_adequacy_ratio) AS avg_ratio
        FROM `{_ds}`.model_scoring
        GROUP BY age_band, state_risk_group
        """
        agg_df = query_bq(agg_sql)

        age_agg = (
            agg_df.groupby("age_band", as_index=False)["avg_ratio"]
            .mean()
            .sort_values("avg_ratio", ascending=False)
        )
        age_bar = {"x": age_agg["age_band"].tolist(), "y": age_agg["avg_ratio"].tolist()}

        state_agg = (
            agg_df.groupby("state_risk_group", as_index=False)["avg_ratio"]
            .mean()
            .sort_values("avg_ratio", ascending=False)
        )
        state_bar = {
            "x": state_agg["state_risk_group"].tolist(),
            "y": state_agg["avg_ratio"].tolist(),
        }

        top_sql = f"""
        SELECT policy_id, coverage_type, age_band, state_risk_group,
               predicted_pure_premium, actual_premium, price_adequacy_ratio
        FROM `{_ds}`.model_scoring
        WHERE pricing_assessment = 'underpriced'
        ORDER BY price_adequacy_ratio DESC
        LIMIT 20
        """
        top_df = query_bq(top_sql)
        underpriced_table = {
            "columns": top_df.columns.tolist(),
            "rows": top_df.values.tolist(),
        }

        chart_data = _clean({
            "scatter": scatter,
            "pie": pie,
            "age_bar": age_bar,
            "state_bar": state_bar,
            "underpriced_table": underpriced_table,
        })

    except Exception:
        available = False

    return templates.TemplateResponse(
        request, "pricing_adequacy.html",
        context=_ctx(
            "pricing_adequacy",
            chart_data=chart_data, available=available, error=error,
        ),
    )


@app.get("/geographic-risk", response_class=HTMLResponse)
async def geographic_risk(request: Request):
    dataset = None
    error = None
    try:
        sql = f"""
        SELECT
            p.state_code,
            c.accident_year,
            COUNT(c.claim_id)           AS claim_count,
            ROUND(AVG(c.total_paid), 2) AS avg_severity,
            ROUND(SUM(c.total_paid), 2) AS total_paid
        FROM `{DATASET_ANALYTICS}`.fct_claims c
        JOIN `{DATASET_ANALYTICS}`.dim_policyholder p
          ON c.policyholder_id = p.policyholder_id
        GROUP BY p.state_code, c.accident_year
        ORDER BY p.state_code, c.accident_year
        """
        df = query_bq(sql)

        df["state_name"] = df["state_code"].map(STATE_NAMES).fillna(df["state_code"])

        records = df.to_dict(orient="records")
        dataset = _clean({
            "records": records,
            "states": sorted(df["state_code"].dropna().unique().tolist()),
            "year_min": int(df["accident_year"].min()),
            "year_max": int(df["accident_year"].max()),
            "state_names": STATE_NAMES,
        })
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request, "geographic_risk.html",
        context=_ctx("geographic_risk", dataset=dataset, error=error),
    )
