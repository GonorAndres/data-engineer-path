"""A failed warehouse query renders a friendly state, never the raw error.

On 2026-09-13 every dev_claims_* table expired and the overview printed
"404 Not found: Table <project>:dev_claims_analytics.fct_claims ..." under a
green "BigQuery Connected" badge. These tests pin the replacement: a clear
state for the visitor, the real exception in the logs, and a marker the
smoke test and health check still treat as a failure.
"""

import pytest
from google.api_core import exceptions as gcp_exceptions

DATA_PAGES = ["/", "/loss-triangle", "/portfolio-health", "/geographic-risk"]


def _raiser(exc):
    def query(_sql):
        raise exc

    return query


@pytest.mark.parametrize("path", DATA_PAGES)
def test_missing_table_renders_refreshing_state(client, app_module, monkeypatch, path):
    monkeypatch.setattr(
        app_module,
        "query_bq",
        _raiser(gcp_exceptions.NotFound("Table test-project:dev_claims_analytics.fct_claims")),
    )
    resp = client.get(path)

    assert resp.status_code == 200
    assert 'data-warehouse-state="refreshing"' in resp.text
    assert "Data refreshing" in resp.text
    assert "Actualizando datos" in resp.text
    # The raw message names the project and dataset; it must not reach the page.
    assert "Not found" not in resp.text
    assert "test-project" not in resp.text
    assert "fct_claims" not in resp.text


@pytest.mark.parametrize("path", DATA_PAGES)
def test_other_failures_render_unavailable_state(client, app_module, monkeypatch, path):
    monkeypatch.setattr(app_module, "query_bq", _raiser(RuntimeError("quota exceeded")))
    resp = client.get(path)

    assert resp.status_code == 200
    assert 'data-warehouse-state="unavailable"' in resp.text
    assert "quota exceeded" not in resp.text


def test_failed_page_does_not_claim_a_connection(client, app_module, monkeypatch):
    """health-check.yml requires "BigQuery Connected"; a failing page must not print it."""
    monkeypatch.setattr(app_module, "query_bq", _raiser(gcp_exceptions.NotFound("x")))
    assert "BigQuery Connected" not in client.get("/").text


def test_failure_is_logged_with_traceback(client, app_module, monkeypatch, caplog):
    monkeypatch.setattr(app_module, "query_bq", _raiser(gcp_exceptions.NotFound("fct_claims")))
    with caplog.at_level("ERROR", logger="dashboard"):
        client.get("/")

    records = [r for r in caplog.records if "Warehouse query failed" in r.getMessage()]
    assert records and records[0].exc_info is not None


def test_static_page_still_claims_connection(client):
    """Methodology makes no query, so it keeps the normal badge."""
    assert "BigQuery Connected" in client.get("/methodology").text
