#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m venv .venv 2>/dev/null || true
source .venv/bin/activate
pip install -e ".[dev]" --quiet
python src/main.py "$@"
