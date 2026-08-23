"""The two prose pages: Methodology and How It's Built.

Neither queries BigQuery, so unlike every other route they must render even
while the warehouse is being rebuilt. Both also carry a full English block and a
full Spanish block in the DOM simultaneously -- `setLang()` toggles `hidden` on
`[data-lang]` elements rather than re-fetching -- which is what makes the
duplicate-id check below load-bearing rather than pedantic.
"""

import re

import pytest

PROSE_PAGES = ["/methodology", "/how-its-built"]


@pytest.mark.parametrize("path", PROSE_PAGES)
def test_prose_page_renders(client, path):
    r = client.get(path)
    assert r.status_code == 200


# A phrase that appears only in each page's own Spanish body -- never in the
# shared sidebar, which also carries `data-lang="es"` spans.
SPANISH_BODY_MARKER = {
    "/methodology": "Cuatro Disciplinas, Una Tabla",
    "/how-its-built": "Lo Que Se Construyó",
}


@pytest.mark.parametrize("path", PROSE_PAGES)
def test_prose_page_ships_both_languages(client, path):
    """Spanish must be present *and* hidden.

    Present, because the toggle only reveals what is already in the DOM. Hidden,
    because a wrapper missing `class="hidden"` renders both languages stacked on
    first paint -- which looks like a content bug, not a CSS one, and so tends to
    get chased in the wrong file.

    Match the page's own `<div>` wrapper, not any `data-lang="es"` element: the
    sidebar in base.html is full of `<span data-lang="es" class="hidden">`, so a
    document-wide check passes even when the page body is broken. It did.
    """
    body = client.get(path).text
    assert '<div data-lang="en">' in body
    assert '<div data-lang="es" class="hidden">' in body
    assert SPANISH_BODY_MARKER[path] in body


def test_methodology_is_marked_active_in_the_nav(client):
    """`active_page` has to match the string base.html compares against.

    A typo here costs nothing at render time and silently leaves the sidebar
    highlighting nothing on this page.
    """
    body = client.get("/methodology").text
    assert re.search(r'href="/methodology" class="nav-link\s+active"', body)


@pytest.mark.parametrize("path", PROSE_PAGES)
def test_svg_element_ids_are_unique(client, path):
    """Duplicate SVG ids break the second diagram, browser-dependently.

    Both language blocks are in the document at once, so an inline `<marker>`
    copied from English to Spanish without a suffix leaves two elements sharing
    one id. `url(#arrow)` then resolves to whichever the browser picked, and the
    arrowheads disappear from one diagram on some engines and not others -- the
    kind of failure that does not reproduce on the machine that shipped it.
    """
    ids = re.findall(r'\sid="([^"]+)"', client.get(path).text)
    duplicates = {i for i in ids if ids.count(i) > 1}
    assert not duplicates, f"duplicate ids in {path}: {sorted(duplicates)}"


def test_the_prose_pages_link_to_each_other(client):
    """They are a pair: inventory and rationale. Either alone reads as a gap."""
    assert '"/methodology"' in client.get("/how-its-built").text
    assert '"/how-its-built"' in client.get("/methodology").text
