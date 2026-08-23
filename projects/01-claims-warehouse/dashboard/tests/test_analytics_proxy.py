"""The same-origin PostHog proxy and the snippet it serves.

Requests to `us.i.posthog.com` are on every adblock list, and a block costs both
the events and the lazily-loaded replay recorder. Serving analytics from the
app's own origin is what survives. None of these tests touch the network.
"""

import httpx
import pytest
from fastapi.testclient import TestClient

CANONICAL = "data-engineer.gonor.me"


class _FakeUpstream:
    """Stands in for `app.state.http`, recording what the proxy forwarded."""

    def __init__(self, status=200, content=b"ok", headers=None, raises=None):
        self.status, self.content = status, content
        self.headers = headers or {"content-type": "text/plain", "set-cookie": "leak=1"}
        self.raises = raises
        self.calls = []

    async def request(self, method, url, content=None, headers=None, params=None):
        self.calls.append(
            {"method": method, "url": url, "headers": headers or {}, "body": content}
        )
        if self.raises:
            raise self.raises
        return httpx.Response(
            self.status, content=self.content, headers=self.headers
        )

    async def aclose(self):
        """`lifespan` closes the client on shutdown; stand in for that."""
        self.closed = True


@pytest.fixture
def proxied(app_module):
    def _run(fake, method="POST", path="/ingest/e/", **kw):
        with TestClient(app_module.app, base_url=f"https://{CANONICAL}") as c:
            c.app.state.http = fake
            return c.request(method, path, **kw)

    return _run


def test_event_path_goes_to_the_api_origin(app_module, proxied):
    fake = _FakeUpstream(content=b'{"status":"Ok"}')
    r = proxied(fake, json={"event": "$pageview"})
    assert r.status_code == 200
    assert fake.calls[0]["url"] == "https://us.i.posthog.com/e/"


def test_static_path_goes_to_the_asset_origin(app_module, proxied):
    """`/ingest/static/*` serves the library and lives on a different host.

    posthog-js loads itself from `api_host + "/static/array.js"`, so getting this
    split wrong means the library never loads and nothing is captured at all.
    """
    fake = _FakeUpstream(content=b"//array.js")
    r = proxied(fake, method="GET", path="/ingest/static/array.js")
    assert r.status_code == 200
    assert fake.calls[0]["url"] == "https://us-assets.i.posthog.com/static/array.js"


def test_hop_by_hop_headers_are_not_forwarded(app_module, proxied):
    """Host would name this service; Accept-Encoding invites a compressed body
    we would hand back with a Content-Length that no longer matches it."""
    fake = _FakeUpstream()
    proxied(fake, json={}, headers={"Accept-Encoding": "gzip", "X-Keep": "yes"})
    sent = {k.lower() for k in fake.calls[0]["headers"]}
    assert "host" not in sent
    assert "accept-encoding" not in sent
    assert "content-length" not in sent
    assert "x-keep" in sent


def test_only_safe_response_headers_come_back(app_module, proxied):
    """An upstream Set-Cookie must not be replayed onto our own origin."""
    fake = _FakeUpstream(headers={"content-type": "application/json", "set-cookie": "x=1"})
    r = proxied(fake, json={})
    assert r.headers["content-type"] == "application/json"
    assert "set-cookie" not in {k.lower() for k in r.headers}


def test_upstream_failure_never_takes_the_page_down(app_module, proxied):
    """Analytics is not load-bearing. A dead upstream returns 502 here and the
    rest of the dashboard keeps serving."""
    fake = _FakeUpstream(raises=httpx.ConnectError("boom"))
    r = proxied(fake, json={})
    assert r.status_code == 502


def test_request_body_is_forwarded_intact(app_module, proxied):
    fake = _FakeUpstream()
    proxied(fake, content=b'{"event":"$pageview"}')
    assert fake.calls[0]["body"] == b'{"event":"$pageview"}'


def test_snippet_meets_the_portfolio_standard(app_module):
    """All 13 portfolio sites report into one PostHog project, so `$host` cannot
    separate them. `app_id` is the only reliable separator; an untagged site's
    traffic is unattributable."""
    from utils.analytics import APP_ID, CANONICAL_HOST, POSTHOG_SNIPPET

    assert APP_ID == "claims-dashboard"
    assert CANONICAL_HOST == CANONICAL
    assert f"app_id: '{APP_ID}'" in POSTHOG_SNIPPET
    assert "capture_pageview: 'history_change'" in POSTHOG_SNIPPET
    # Derived from the hostname, never hardcoded: preview and per-deploy hosts
    # would otherwise stamp themselves 'production' and pollute the numbers.
    assert "location.hostname === CANONICAL_HOST ? 'production' : 'preview'" in POSTHOG_SNIPPET
    # Same-origin on whichever hostname served the page, so no CORS is needed.
    assert "location.origin + '/ingest'" in POSTHOG_SNIPPET
    assert "us.i.posthog.com" not in POSTHOG_SNIPPET.split("api_host")[1][:200]
