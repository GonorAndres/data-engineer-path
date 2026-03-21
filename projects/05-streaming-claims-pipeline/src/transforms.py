"""DoFn transforms for the streaming claims pipeline.

Each transform is a single-responsibility DoFn:
- ParseAndValidateClaim: bytes -> validated dict (or dead letter)
- EnrichClaim: add processing metadata
- ExtractCoverageKey: key by coverage_type for GroupByKey
- ComputeStreamingSummary: windowed aggregation with pane tracking

Key difference from P03: these transforms are pane-aware. They track
EARLY/ON_TIME/LATE firing metadata that P03's batch transforms ignore.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.utils.windowed_value import PaneInfoTiming

logger = logging.getLogger(__name__)

_PIPELINE_VERSION = "0.1.0"

# Required fields for a valid claim
_REQUIRED_FIELDS = ("claim_id", "policy_id", "estimated_amount", "coverage_type")

# Valid coverage types
_VALID_COVERAGE_TYPES = {
    "auto_colision",
    "auto_robo_total",
    "auto_responsabilidad_civil",
    "gastos_medicos_mayores",
    "vida_individual",
    "hogar_incendio",
    "hogar_robo",
    "responsabilidad_civil_general",
}


# Map Beam PaneInfo timing constants to readable strings
def _pane_timing_str(timing) -> str:
    """Convert Beam PaneInfoTiming to a human-readable string."""
    timing_map = {
        PaneInfoTiming.EARLY: "EARLY",
        PaneInfoTiming.ON_TIME: "ON_TIME",
        PaneInfoTiming.LATE: "LATE",
        PaneInfoTiming.UNKNOWN: "UNKNOWN",
    }
    return timing_map.get(timing, "UNKNOWN")


class ParseAndValidateClaim(beam.DoFn):
    """Parse raw Pub/Sub bytes into a validated claim dict.

    Valid claims are yielded as TimestampedValue (using the event's
    'timestamp' field). Invalid claims go to the 'dead_letter' tagged output.

    Validation rules:
    - Must be valid UTF-8 JSON
    - Must have all required fields (claim_id, policy_id, estimated_amount, coverage_type)
    - estimated_amount must be > 0
    - accident_date must be parseable as a date
    - coverage_type must not be empty
    """

    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element):
        raw_str = ""
        try:
            # Decode bytes from Pub/Sub, or handle dict/str inputs for testing
            if isinstance(element, bytes):
                raw_str = element.decode("utf-8")
                record = json.loads(raw_str)
            elif isinstance(element, str):
                raw_str = element
                record = json.loads(raw_str)
            elif isinstance(element, dict):
                record = dict(element)
                raw_str = json.dumps(record)
            else:
                raw_str = str(element)
                record = json.loads(raw_str)

            # Validate required fields
            for field in _REQUIRED_FIELDS:
                if field not in record:
                    yield beam.pvalue.TaggedOutput(
                        self.DEAD_LETTER_TAG,
                        {
                            "raw_data": raw_str[:2000],
                            "error_reason": f"Missing required field: {field}",
                            "error_type": "validate",
                            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    return

            # Validate amount > 0
            amount = record.get("estimated_amount")
            if not isinstance(amount, (int, float)) or amount <= 0:
                yield beam.pvalue.TaggedOutput(
                    self.DEAD_LETTER_TAG,
                    {
                        "raw_data": raw_str[:2000],
                        "error_reason": f"Invalid estimated_amount: {amount}",
                        "error_type": "validate",
                        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return

            # Validate accident_date is parseable
            accident_date = record.get("accident_date", "")
            if accident_date:
                try:
                    datetime.fromisoformat(accident_date)
                except (ValueError, TypeError):
                    yield beam.pvalue.TaggedOutput(
                        self.DEAD_LETTER_TAG,
                        {
                            "raw_data": raw_str[:2000],
                            "error_reason": f"Invalid accident_date: {accident_date}",
                            "error_type": "validate",
                            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    return

            # Validate coverage_type is not empty and is a known type
            coverage = record.get("coverage_type", "")
            if not coverage or not coverage.strip():
                yield beam.pvalue.TaggedOutput(
                    self.DEAD_LETTER_TAG,
                    {
                        "raw_data": raw_str[:2000],
                        "error_reason": "Empty coverage_type",
                        "error_type": "validate",
                        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return

            if coverage not in _VALID_COVERAGE_TYPES:
                yield beam.pvalue.TaggedOutput(
                    self.DEAD_LETTER_TAG,
                    {
                        "raw_data": raw_str[:2000],
                        "error_reason": f"Unknown coverage_type: {coverage}",
                        "error_type": "validate",
                        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return

            # Parse event timestamp for windowing
            ts_str = record.get("timestamp")
            if ts_str:
                event_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                event_ts = datetime.now(timezone.utc)

            yield beam.window.TimestampedValue(record, event_ts.timestamp())

        except json.JSONDecodeError as exc:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "raw_data": raw_str[:2000] if raw_str else str(element)[:2000],
                    "error_reason": f"JSON parse error: {exc}",
                    "error_type": "parse",
                    "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "raw_data": raw_str[:2000] if raw_str else str(element)[:2000],
                    "error_reason": f"Unexpected error: {exc}",
                    "error_type": "unknown",
                    "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )


class EnrichClaim(beam.DoFn):
    """Add processing metadata to a validated claim.

    Adds:
    - processing_timestamp: when the pipeline processed this claim
    - validation_status: "valid" (only valid claims reach this DoFn)
    - pipeline_version: version string for lineage tracking
    """

    def process(self, element: dict[str, Any]):
        enriched = dict(element)
        enriched["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
        enriched["validation_status"] = "valid"
        enriched["pipeline_version"] = _PIPELINE_VERSION
        yield enriched


class ExtractCoverageKey(beam.DoFn):
    """Extract (coverage_type, full_claim_dict) key-value pairs.

    Unlike P03 which yielded (coverage_type, amount), this yields the full
    claim dict as the value. This enables richer aggregations in
    ComputeStreamingSummary (e.g., counting unique claim_ids, tracking
    min/max amounts alongside metadata).
    """

    def process(self, element: dict[str, Any]):
        coverage = element.get("coverage_type", "unknown")
        yield (coverage, element)


class ComputeStreamingSummary(beam.DoFn):
    """Aggregate claims per coverage type within a window, tracking pane timing.

    Uses WindowParam and PaneInfoParam to produce streaming-aware summaries.
    In ACCUMULATING mode, each firing includes ALL data seen so far for the
    window -- not just the delta since the last firing.

    Output includes:
    - pane_timing: EARLY / ON_TIME / LATE
    - firing_id: unique identifier per pane firing
    """

    def process(
        self,
        element,
        window_info=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        coverage_type, claims = element
        claims_list = list(claims)

        if not claims_list:
            return

        amounts = [float(c.get("estimated_amount", 0)) for c in claims_list]

        pane_timing = _pane_timing_str(pane_info.timing)
        firing_id = str(uuid.uuid4())

        yield {
            "window_start": window_info.start.to_utc_datetime().isoformat(),
            "window_end": window_info.end.to_utc_datetime().isoformat(),
            "coverage_type": coverage_type,
            "claim_count": len(claims_list),
            "total_amount_mxn": round(sum(amounts), 2),
            "avg_amount_mxn": round(sum(amounts) / len(amounts), 2),
            "min_amount_mxn": round(min(amounts), 2),
            "max_amount_mxn": round(max(amounts), 2),
            "pane_timing": pane_timing,
            "firing_id": firing_id,
        }
