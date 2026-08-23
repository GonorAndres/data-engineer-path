"""Test fixtures for the claims dashboard.

`utils.bq_client` raises at import time when no GCP project is configured, so
the environment has to be set before `main` is imported anywhere in the suite.
Nothing here touches BigQuery or the network.
"""

import os

import pytest

# Must precede `import main`: bq_client raises RuntimeError at module scope
# without one of these set. That behaviour is deliberate -- a missed CI env-var
# injection should fail loudly at boot rather than silently on every page load.
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("CANONICAL_REDIRECT_HOST", "data-engineer.gonor.me")


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Neutralise the startup cache warm.

    `lifespan` kicks `_warm_cache` onto an executor, which issues real BigQuery
    jobs. Any test entering the TestClient context manager -- which the proxy
    tests must, to get `app.state.http` -- would otherwise make network calls and
    have its result depend on ambient credentials.
    """
    import main

    monkeypatch.setattr(main, "_warm_cache", lambda: None)


@pytest.fixture
def app_module():
    import main

    return main


@pytest.fixture
def client(app_module):
    """A client on the canonical host, so nothing redirects by default."""
    from fastapi.testclient import TestClient

    return TestClient(app_module.app, base_url="https://data-engineer.gonor.me")
