"""Inject the PostHog snippet into Streamlit's static index.html.

Run once at Docker build time. Streamlit serves this file unchanged at
runtime, so the snippet ships baked into the image with zero per-request
cost and no iframe scoping issues.
"""

import pathlib
import sys

import streamlit

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from utils.analytics import POSTHOG_SNIPPET  # noqa: E402

index = pathlib.Path(streamlit.__file__).parent / "static" / "index.html"
html = index.read_text(encoding="utf-8")

marker = "posthog.init("
if marker in html:
    print(f"PostHog snippet already present in {index}; skipping.")
    sys.exit(0)

patched = html.replace("</head>", f"{POSTHOG_SNIPPET}</head>", 1)
if patched == html:
    print(f"ERROR: could not find </head> in {index}", file=sys.stderr)
    sys.exit(1)

index.write_text(patched, encoding="utf-8")
print(f"Patched {index} with PostHog snippet.")
