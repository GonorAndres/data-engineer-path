"""The redirect that keeps the dashboard on one hostname.

Cloud Run answers on two run.app hostnames forever -- a domain mapping adds a
route in, it never removes one -- so without this middleware the site is
reachable at three addresses and the traffic splits. Everything published before
the custom domain existed still points at run.app, so these are the tests that
keep those links alive.
"""

import pytest
from fastapi.testclient import TestClient

CANONICAL = "data-engineer.gonor.me"

# Both forms Cloud Run serves for one service.
RUN_APP_HOSTS = [
    "claims-dashboard-451451662791.us-central1.run.app",
    "claims-dashboard-d3qj5vwxtq-uc.a.run.app",
]


def _client(app_module, host, scheme="https"):
    return TestClient(app_module.app, base_url=f"{scheme}://{host}")


@pytest.mark.parametrize("host", RUN_APP_HOSTS)
def test_run_app_host_redirects_to_canonical(app_module, host):
    r = _client(app_module, host).get("/loss-triangle", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == f"https://{CANONICAL}/loss-triangle"


@pytest.mark.parametrize("host", RUN_APP_HOSTS)
def test_redirect_preserves_path_and_query(app_module, host):
    r = _client(app_module, host).get(
        "/geographic-risk?year=2025&state=JAL", follow_redirects=False
    )
    assert r.status_code == 301
    assert r.headers["location"] == (
        f"https://{CANONICAL}/geographic-risk?year=2025&state=JAL"
    )


def test_canonical_host_is_not_redirected(app_module):
    r = _client(app_module, CANONICAL).get("/health", follow_redirects=False)
    assert r.status_code == 200


def test_health_is_never_redirected(app_module):
    """Uptime checks and the post-deploy smoke test address a revision directly.

    If /health redirected, the smoke test would be answered by whatever the
    canonical host currently serves -- that is, the revision being replaced.
    """
    for host in RUN_APP_HOSTS:
        r = _client(app_module, host).get("/health", follow_redirects=False)
        assert r.status_code == 200, host
        assert r.json() == {"status": "ok"}


def test_tagged_revision_host_is_not_redirected(app_module):
    """A canary URL must reach the revision under test, not the live one.

    The deploy ships each revision with no traffic and smoke-tests it on its
    tagged URL. Redirecting that URL would make every deploy test the previous
    revision, pass, and promote code nobody exercised.
    """
    # Deliberately NOT /health: that path is exempt for its own reasons, so it
    # would pass whether or not the canary rule exists. An unrouted path
    # distinguishes them -- 404 means the request reached routing, 301 means the
    # middleware sent it away.
    canary = "canary---claims-dashboard-d3qj5vwxtq-uc.a.run.app"
    r = _client(app_module, canary).get("/not-a-route", follow_redirects=False)
    assert r.status_code == 404

    # Same path on an untagged run.app host must still redirect, or the
    # assertion above proves nothing about the canary rule specifically.
    other = _client(app_module, RUN_APP_HOSTS[1]).get(
        "/not-a-route", follow_redirects=False
    )
    assert other.status_code == 301


def test_ingest_is_never_redirected(app_module):
    """A 301 on a POST drops the body in most clients.

    Any page still loaded on a run.app hostname keeps posting analytics there;
    redirecting those would discard the events silently.
    """
    r = _client(app_module, RUN_APP_HOSTS[0]).post(
        "/ingest/e/", json={"event": "$pageview"}, follow_redirects=False
    )
    assert r.status_code != 301


def test_redirect_is_disabled_when_env_var_is_unset(app_module, monkeypatch):
    """Unset means serve on whatever host asked.

    That is what local development needs, and what kept the service up during
    the window before DNS for the custom domain had propagated.
    """
    monkeypatch.setattr(app_module, "REDIRECT_TO_CANONICAL", "")
    r = _client(app_module, RUN_APP_HOSTS[0]).get("/health", follow_redirects=False)
    assert r.status_code == 200
