"""Main streaming pipeline for insurance claims processing.

This is a TRUE STREAMING pipeline -- the fundamental difference from P03.
P03 reads from BigQuery in batch mode; this pipeline reads from Pub/Sub
in streaming mode with:
- FixedWindows with configurable size (default 1 hour)
- AfterWatermark trigger with early (speculative) and late firings
- ACCUMULATING mode: each firing includes all data for the window
- Allowed lateness: late data accepted for a configurable period

Architecture:
    Pub/Sub -> Parse & Validate -> Enrich -> Window -> Key -> GroupByKey
           |                                                    |
           +-> Dead Letters (BQ)          Summaries -> BQ (hourly)

Local execution (DirectRunner):
    python src/streaming_pipeline.py \
        --runner DirectRunner \
        --input_subscription projects/PROJECT/subscriptions/SUB \
        --output_project PROJECT

GCP execution (DataflowRunner):
    python src/streaming_pipeline.py \
        --runner DataflowRunner \
        --input_subscription projects/PROJECT/subscriptions/SUB \
        --output_project PROJECT \
        --temp_location gs://BUCKET/temp \
        --region us-central1
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterProcessingTime,
    AfterWatermark,
)
from apache_beam.transforms.window import Duration, FixedWindows

from pipeline_options import StreamingClaimsPipelineOptions
from schemas import (
    STREAMING_DEAD_LETTERS_SCHEMA,
    STREAMING_HOURLY_SUMMARY_SCHEMA,
    get_dlq_table_spec,
    get_summary_table_spec,
)
from transforms import (
    ComputeStreamingSummary,
    EnrichClaim,
    ExtractCoverageKey,
    ParseAndValidateClaim,
)

logger = logging.getLogger(__name__)


def build_streaming_pipeline(
    pipeline: beam.Pipeline,
    options: StreamingClaimsPipelineOptions,
) -> None:
    """Construct the streaming pipeline graph.

    Args:
        pipeline: The Beam Pipeline instance (must be in streaming mode).
        options: Custom pipeline options with window/lateness/IO config.
    """
    output_project = options.output_project
    output_dataset = options.output_dataset

    summary_table = get_summary_table_spec(output_project, output_dataset)
    dlq_table = get_dlq_table_spec(output_project, output_dataset)

    # 1. Read from Pub/Sub (unbounded source -- this makes it streaming)
    raw = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
        subscription=options.input_subscription,
    )

    # 2. Parse, validate, route dead letters
    parsed = raw | "ParseAndValidate" >> beam.ParDo(
        ParseAndValidateClaim()
    ).with_outputs(ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid")

    valid_claims = parsed.valid
    dead_letters = parsed[ParseAndValidateClaim.DEAD_LETTER_TAG]

    # 3. Enrich with processing metadata
    enriched = valid_claims | "Enrich" >> beam.ParDo(EnrichClaim())

    # 4. Apply fixed windows with streaming triggers
    #    THIS IS THE KEY DIFFERENCE FROM P03:
    #    - AfterWatermark with early + late firings
    #    - ACCUMULATING mode (each firing has ALL data, not just delta)
    #    - Allowed lateness (accepts late data within window)
    windowed = enriched | "WindowInto" >> beam.WindowInto(
        FixedWindows(options.window_size_seconds),
        trigger=AfterWatermark(
            early=AfterProcessingTime(options.early_firing_interval_seconds),
            late=AfterCount(1),
        ),
        allowed_lateness=Duration(seconds=options.allowed_lateness_seconds),
        accumulation_mode=AccumulationMode.ACCUMULATING,
    )

    # 5. Key by coverage_type and group
    keyed = windowed | "ExtractKey" >> beam.ParDo(ExtractCoverageKey())
    grouped = keyed | "GroupByCoverage" >> beam.GroupByKey()

    # 6. Compute streaming-aware summaries (with pane timing)
    summaries = grouped | "ComputeSummaries" >> beam.ParDo(ComputeStreamingSummary())

    # 7. Write summaries to BigQuery
    summaries | "WriteSummaries" >> WriteToBigQuery(
        table=summary_table,
        schema=STREAMING_HOURLY_SUMMARY_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )

    # 8. Write dead letters to BigQuery
    dead_letters | "WriteDLQ" >> WriteToBigQuery(
        table=dlq_table,
        schema=STREAMING_DEAD_LETTERS_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )


def run(argv: list[str] | None = None) -> None:
    """Parse arguments and execute the streaming pipeline."""
    options = PipelineOptions(argv)
    streaming_opts = options.view_as(StreamingClaimsPipelineOptions)

    # Validate required options at runtime (not via argparse required=True,
    # because Beam globally parses all registered PipelineOptions subclasses
    # and required args break TestPipeline).
    if not streaming_opts.input_subscription:
        raise ValueError("--input_subscription is required")
    if not streaming_opts.output_project:
        raise ValueError("--output_project is required")

    # THE KEY DIFFERENCE FROM P03: streaming = True
    options.view_as(StandardOptions).streaming = True

    logger.info(
        "Starting streaming pipeline: subscription=%s output=%s:%s "
        "window=%ds lateness=%ds early_interval=%ds",
        streaming_opts.input_subscription,
        streaming_opts.output_project,
        streaming_opts.output_dataset,
        streaming_opts.window_size_seconds,
        streaming_opts.allowed_lateness_seconds,
        streaming_opts.early_firing_interval_seconds,
    )

    with beam.Pipeline(options=options) as p:
        build_streaming_pipeline(p, streaming_opts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
