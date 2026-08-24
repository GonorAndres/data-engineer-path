"""The Methodology page, and the redirect left behind by the merge.

Methodology absorbed How It's Built on 2026-08-23: the pipeline diagram and the
six project cards moved into it, the two decisions that existed only there were
folded into GCP Choices with reversal conditions, and `/how-its-built` became a
301. The page queries nothing, so unlike every other route it must render even
while the warehouse is being rebuilt.

Both language blocks sit in the DOM at once -- `setLang()` toggles `hidden` on
`[data-lang]` elements rather than re-fetching -- which is what makes the
duplicate-id and anchor checks below load-bearing rather than pedantic.
"""

import re

import pytest


def test_methodology_renders(client):
    assert client.get("/methodology").status_code == 200


def test_methodology_ships_both_languages(client):
    """Spanish must be present *and* hidden.

    Present, because the toggle only reveals what is already in the DOM. Hidden,
    because a wrapper missing `class="hidden"` renders both languages stacked on
    first paint -- which looks like a content bug, not a CSS one, and so tends to
    get chased in the wrong file.

    Match the page's own `<div>` wrapper, not any `data-lang="es"` element: the
    sidebar in base.html is full of `<span data-lang="es" class="hidden">`, so a
    document-wide check passes even when the page body is broken. It did.
    """
    body = client.get("/methodology").text
    assert '<div data-lang="en">' in body
    assert '<div data-lang="es" class="hidden">' in body
    assert "La Regla de las Capas" in body


def test_methodology_is_marked_active_in_the_nav(client):
    """`active_page` has to match the string base.html compares against.

    A typo costs nothing at render time and silently leaves the sidebar
    highlighting nothing on this page.
    """
    body = client.get("/methodology").text
    assert re.search(r'href="/methodology" class="nav-link\s+active"', body)


def test_svg_element_ids_are_unique(client):
    """Duplicate SVG ids break the second diagram, browser-dependently.

    Three diagrams now ship twice each, once per language. An inline `<marker>`
    copied across without a suffix leaves two elements sharing one id;
    `url(#arrow)` then resolves to whichever the browser picked, and arrowheads
    disappear from one diagram on some engines and not others -- the kind of
    failure that does not reproduce on the machine that shipped it.
    """
    ids = re.findall(r'\sid="([^"]+)"', client.get("/methodology").text)
    duplicates = {i for i in ids if ids.count(i) > 1}
    assert not duplicates, f"duplicate ids: {sorted(duplicates)}"


def test_table_of_contents_anchors_all_resolve(client):
    """Every TOC entry must point at a heading that exists on the page.

    The anchors are generated from the headings, so a section renamed or removed
    without regenerating leaves a link that scrolls nowhere -- silent, and
    exactly the kind of thing nobody clicks before shipping.
    """
    body = client.get("/methodology").text
    ids = set(re.findall(r'<h3 id="([^"]+)"', body))
    anchors = set(re.findall(r'href="#([a-z0-9-]+)"', body))
    assert anchors, "no table-of-contents anchors found at all"
    assert anchors <= ids, f"anchors with no matching heading: {sorted(anchors - ids)}"


@pytest.mark.parametrize(
    "content",
    [
        "The Six Projects",          # the P01..P06 inventory carried over
        "Infrastructure as Code",    # the P04 card, with its corrected wording
        "FastAPI + Jinja2",          # a decision that existed only in How It's Built
        "BigQuery + GCS",            # ditto
    ],
)
def test_merge_did_not_drop_content(client, content):
    """Guards the merge itself.

    These four came from How It's Built, whose template no longer exists. If a
    later edit removes one there is nothing else in the repo still rendering it,
    so the loss would be permanent and invisible.
    """
    assert content in client.get("/methodology").text


def test_how_its_built_redirects_permanently(client):
    """The old path 301s rather than 404ing.

    Nothing external links it today -- checked against the blog and gonor.me --
    but it was live and indexable for months, and a 301 costs one route.
    """
    r = client.get("/how-its-built", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "/methodology"


def test_how_its_built_is_gone_from_the_sidebar(client):
    """One entry, not two. The merge is pointless if the nav still implies both."""
    assert 'href="/how-its-built"' not in client.get("/methodology").text
