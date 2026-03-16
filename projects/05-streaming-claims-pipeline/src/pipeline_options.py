"""Custom pipeline options for the streaming claims pipeline.

Extends Beam's PipelineOptions with streaming-specific parameters:
window size, allowed lateness, early firing intervals, and I/O coordinates.

Usage:
    options = PipelineOptions(argv)
    streaming_opts = options.view_as(StreamingClaimsPipelineOptions)
    window_size = streaming_opts.window_size_seconds
"""

from __future__ import annotations

from apache_beam.options.pipeline_options import PipelineOptions


class StreamingClaimsPipelineOptions(PipelineOptions):
    """Pipeline options specific to the streaming claims pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--window_size_seconds",
            type=int,
            default=3600,
            help="Fixed window size in seconds (default: 3600 = 1 hour)",
        )
        parser.add_argument(
            "--allowed_lateness_seconds",
            type=int,
            default=3600,
            help="How long after watermark to accept late data, in seconds (default: 3600)",
        )
        parser.add_argument(
            "--early_firing_interval_seconds",
            type=int,
            default=30,
            help="Interval for early (speculative) firings in seconds (default: 30)",
        )
        parser.add_argument(
            "--input_subscription",
            type=str,
            default="",
            help="Pub/Sub subscription path (projects/PROJECT/subscriptions/SUB)",
        )
        parser.add_argument(
            "--output_project",
            type=str,
            default="",
            help="GCP project ID for BigQuery output tables",
        )
        parser.add_argument(
            "--output_dataset",
            type=str,
            default="claims_analytics",
            help="BigQuery dataset for output tables (default: claims_analytics)",
        )
