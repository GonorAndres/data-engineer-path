"""Apache Beam batch pipeline for windowed claim aggregations.

Reads claims from BigQuery (streaming_claims table), applies fixed 1-hour
windows based on event timestamps, and writes hourly summaries to
claims_analytics.hourly_claim_summary.

IMPORTANT: This pipeline runs in BATCH mode only.
    Never start a Dataflow streaming job -- the batch execution is sufficient
    to prove Beam competence and costs $0.01/run vs $1,000+/month streaming.

Local execution (DirectRunner):
    python src/beam_pipeline.py \
        --runner DirectRunner \
        --project my-project

GCP execution (DataflowRunner, BATCH only):
    python src/beam_pipeline.py \
        --runner DataflowRunner \
        --project my-project \
        --temp_location gs://BUCKET/temp \
        --region us-central1 \
        --no_streaming

    WARNING: Do NOT remove --no_streaming. A streaming Dataflow job costs
    ~$1,000-2,000/month and cannot be stopped without manual intervention.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AfterWatermark

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BigQuery schemas
# ---------------------------------------------------------------------------

HOURLY_SUMMARY_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "coverage_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "claim_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "avg_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "min_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "max_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
    ]
}

DEAD_LETTER_SCHEMA = {
    "fields": [
        {"name": "raw_data", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


class ParseClaim(beam.DoFn):
    """Parse a claim record and assign event timestamp.

    Outputs to main output for valid claims, or to the 'dead_letter' tagged
    output for records that fail parsing.
    """

    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element: dict[str, Any]):
        try:
            # If element is a BQ row, it's already a dict
            if isinstance(element, bytes):
                record = json.loads(element.decode("utf-8"))
            elif isinstance(element, str):
                record = json.loads(element)
            else:
                record = dict(element)

            # Parse the event timestamp for windowing
            ts_str = record.get("timestamp") or record.get("processing_timestamp")
            if ts_str:
                event_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                event_ts = datetime.now(timezone.utc)

            yield beam.window.TimestampedValue(record, event_ts.timestamp())

        except Exception as exc:
            raw = str(element)[:2000]
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "raw_data": raw,
                    "error_reason": str(exc),
                    "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )


class ExtractCoverageKey(beam.DoFn):
    """Extract (coverage_type, estimated_amount) key-value pairs."""

    def process(self, element: dict[str, Any]):
        coverage = element.get("coverage_type", "unknown")
        amount = float(element.get("estimated_amount", 0))
        yield (coverage, amount)


class ComputeHourlySummary(beam.DoFn):
    """Aggregate amounts per coverage type within a window."""

    def process(self, element, window_info=beam.DoFn.WindowParam):
        coverage_type, amounts = element
        amounts_list = list(amounts)

        if not amounts_list:
            return

        yield {
            "window_start": window_info.start.to_utc_datetime().isoformat(),
            "window_end": window_info.end.to_utc_datetime().isoformat(),
            "coverage_type": coverage_type,
            "claim_count": len(amounts_list),
            "total_amount_mxn": round(sum(amounts_list), 2),
            "avg_amount_mxn": round(sum(amounts_list) / len(amounts_list), 2),
            "min_amount_mxn": round(min(amounts_list), 2),
            "max_amount_mxn": round(max(amounts_list), 2),
        }


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------


def build_pipeline(
    pipeline: beam.Pipeline,
    project: str,
    input_table: str = "",
    output_table: str = "",
    dlq_table: str = "",
) -> None:
    """Construct the Beam pipeline graph.

    Args:
        pipeline: The Beam Pipeline instance.
        project: GCP project ID.
        input_table: BigQuery source table (project:dataset.table).
        output_table: BigQuery destination for hourly summaries.
        dlq_table: BigQuery destination for dead-letter records.
    """
    if not input_table:
        input_table = f"{project}:claims_raw.streaming_claims"
    if not output_table:
        output_table = f"{project}:claims_analytics.hourly_claim_summary"
    if not dlq_table:
        dlq_table = f"{project}:claims_analytics.beam_dead_letters"

    # Read from BigQuery
    raw_claims = pipeline | "ReadFromBQ" >> beam.io.ReadFromBigQuery(table=input_table)

    # Parse and tag dead letters
    parsed = raw_claims | "ParseClaims" >> beam.ParDo(ParseClaim()).with_outputs(
        ParseClaim.DEAD_LETTER_TAG, main="valid"
    )

    valid_claims = parsed.valid
    dead_letters = parsed[ParseClaim.DEAD_LETTER_TAG]

    # Apply fixed 1-hour windows using event timestamps
    windowed = valid_claims | "Window1H" >> beam.WindowInto(
        window.FixedWindows(3600),  # 1 hour in seconds
        trigger=AfterWatermark(),
        accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
    )

    # Extract key-value pairs and group
    keyed = windowed | "ExtractKeys" >> beam.ParDo(ExtractCoverageKey())
    grouped = keyed | "GroupByCoverage" >> beam.GroupByKey()

    # Compute summaries
    summaries = grouped | "ComputeSummaries" >> beam.ParDo(ComputeHourlySummary())

    # Write summaries to BigQuery
    summaries | "WriteSummaries" >> WriteToBigQuery(
        table=output_table,
        schema=HOURLY_SUMMARY_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )

    # Write dead letters
    dead_letters | "WriteDLQ" >> WriteToBigQuery(
        table=dlq_table,
        schema=DEAD_LETTER_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def run(argv: list[str] | None = None) -> None:
    """Parse arguments and execute the pipeline.

    WARNING: The --no_streaming flag is critical. Removing it when using
    DataflowRunner will start a streaming job that costs ~$1,000-2,000/month
    and cannot be easily stopped.
    """
    parser = argparse.ArgumentParser(description="Beam batch pipeline for claim aggregations")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--input_table",
        default="",
        help="BigQuery input table (project:dataset.table). "
        "Default: PROJECT:claims_raw.streaming_claims",
    )
    parser.add_argument(
        "--output_table",
        default="",
        help="BigQuery output table. Default: PROJECT:claims_analytics.hourly_claim_summary",
    )
    parser.add_argument(
        "--no_streaming",
        action="store_true",
        default=True,
        help="Force batch mode (default: True). NEVER set to False with DataflowRunner.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Build pipeline options -- always batch
    options = PipelineOptions(pipeline_args)
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = False  # ALWAYS batch. Never streaming.

    logger.info(
        "Starting batch pipeline: project=%s runner=%s streaming=%s",
        known_args.project,
        options.get_all_options().get("runner", "DirectRunner"),
        standard_options.streaming,
    )

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            pipeline=p,
            project=known_args.project,
            input_table=known_args.input_table,
            output_table=known_args.output_table,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
