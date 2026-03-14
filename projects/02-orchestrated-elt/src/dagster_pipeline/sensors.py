"""Dagster sensors for the insurance claims ELT pipeline.

``new_data_sensor`` watches a configurable directory for new CSV files,
simulating the pattern where an upstream system (e.g., GCS, SFTP) drops
files that should trigger a pipeline run.  When at least one new CSV is
detected, the sensor emits a ``RunRequest`` that materializes the full
asset graph.
"""

import os
from pathlib import Path
from typing import Optional

from dagster import (
    AssetSelection,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

from pipeline.config import WATCH_DIR

# We persist the set of already-seen filenames in the sensor cursor
# (a plain string).  The format is ``filename1|filename2|...``.
_CURSOR_SEPARATOR = "|"


def _parse_cursor(cursor: Optional[str]) -> set[str]:
    """Decode the cursor string into a set of known filenames."""
    if not cursor:
        return set()
    return set(cursor.split(_CURSOR_SEPARATOR))


def _encode_cursor(known: set[str]) -> str:
    """Encode a set of known filenames into a cursor string."""
    return _CURSOR_SEPARATOR.join(sorted(known))


@sensor(
    description=(
        "Watches the incoming/ directory for new CSV files.  "
        "When new files appear, triggers a full pipeline materialization.  "
        "Simulates GCS file-arrival events for local development."
    ),
    minimum_interval_seconds=30,
    asset_selection=AssetSelection.groups("claims_warehouse"),
)
def new_data_sensor(
    context: SensorEvaluationContext,
) -> Optional[RunRequest]:
    """Check for new CSV files and trigger a run if any are found.

    The sensor maintains a cursor that tracks which files have already
    been seen so the same drop does not trigger multiple runs.
    """
    watch_dir = WATCH_DIR

    if not watch_dir.exists():
        context.log.debug("Watch directory %s does not exist yet.", watch_dir)
        return None

    # List CSV files currently present.
    current_files: set[str] = {
        entry.name
        for entry in watch_dir.iterdir()
        if entry.is_file() and entry.suffix.lower() == ".csv"
    }

    if not current_files:
        context.log.debug("No CSV files in %s.", watch_dir)
        return None

    known_files = _parse_cursor(context.cursor)
    new_files = current_files - known_files

    if not new_files:
        context.log.debug("No new files since last check.")
        return None

    context.log.info(
        "Detected %d new CSV file(s): %s", len(new_files), sorted(new_files)
    )

    # Update cursor so these files are not re-triggered.
    all_known = known_files | current_files
    context.update_cursor(_encode_cursor(all_known))

    return RunRequest(
        run_key=f"new-csv-{'_'.join(sorted(new_files))}",
        run_config=RunConfig(),
        tags={"trigger": "new_data_sensor", "new_files": ",".join(sorted(new_files))},
    )
