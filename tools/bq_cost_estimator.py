#!/usr/bin/env python3
"""Estimate BigQuery query cost before running using dry-run mode.

This script uses the BigQuery dry-run API to estimate how many bytes a query
will scan, then converts that to a dollar cost at the on-demand rate ($6.25/TB).
This lets you catch expensive queries BEFORE they run and eat your budget.

Usage examples:
    python tools/bq_cost_estimator.py --query "SELECT * FROM dataset.table"
    python tools/bq_cost_estimator.py --file path/to/query.sql
    python tools/bq_cost_estimator.py --file query.sql --project my-project --location us-central1
"""

import argparse
import sys
from pathlib import Path

# BigQuery on-demand pricing: $6.25 per TB scanned (as of 2024).
# First 1 TB/month is free, but this estimator does not track cumulative usage.
COST_PER_TB_USD = 6.25
BYTES_PER_TB = 1024**4


def format_bytes(num_bytes: int) -> str:
    """Convert a byte count into a human-readable string (KB, MB, GB, TB).

    Args:
        num_bytes: Raw byte count from BigQuery dry-run response.

    Returns:
        A string like "45.2 MB" or "1.3 TB".
    """
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024**2:
        return f"{num_bytes / 1024:.1f} KB"
    elif num_bytes < 1024**3:
        return f"{num_bytes / 1024**2:.1f} MB"
    elif num_bytes < 1024**4:
        return f"{num_bytes / 1024**3:.2f} GB"
    else:
        return f"{num_bytes / 1024**4:.3f} TB"


def calculate_cost(num_bytes: int) -> float:
    """Calculate the on-demand cost in USD for scanning a given number of bytes.

    BigQuery bills in increments of bytes scanned, not bytes returned.
    The minimum charge per query is 10 MB even if less is scanned.

    Args:
        num_bytes: Bytes that will be scanned.

    Returns:
        Estimated cost in USD.
    """
    return (num_bytes / BYTES_PER_TB) * COST_PER_TB_USD


def generate_tip(num_bytes: int, cost: float) -> str:
    """Generate a human-friendly tip based on the query cost.

    Provides context so the user knows whether the query is cheap or expensive.

    Args:
        num_bytes: Bytes that will be scanned.
        cost: Estimated cost in USD.

    Returns:
        A tip string with context about the cost.
    """
    formatted = format_bytes(num_bytes)

    if cost < 0.01:
        return (
            f"Tip: This query scans {formatted}. BigQuery charges per TB scanned,\n"
            f"     so this query costs less than a penny."
        )
    elif cost < 1.00:
        return (
            f"Tip: This query scans {formatted}. That is under $1 -- reasonable\n"
            f"     for ad-hoc analysis but watch out if it runs repeatedly."
        )
    elif cost < 10.00:
        return (
            f"Tip: This query scans {formatted} (~${cost:.2f}). Consider adding\n"
            f"     filters (WHERE clauses) or selecting fewer columns to reduce cost."
        )
    else:
        return (
            f"WARNING: This query scans {formatted} (~${cost:.2f}). This is\n"
            f"     expensive. Use partitioned/clustered tables, add date filters,\n"
            f"     or consider materialized views to reduce cost."
        )


def run_dry_run(query: str, project: str | None, location: str) -> None:
    """Execute a BigQuery dry-run and print the cost estimate.

    A dry-run tells BigQuery to validate the query and compute bytes scanned
    without actually executing it. No data is read and no cost is incurred.

    Args:
        query: The SQL query string to estimate.
        project: GCP project ID. If None, uses the default from gcloud config.
        location: BigQuery processing location (e.g., "US", "us-central1").
    """
    # Import here so the script can show --help without requiring the SDK.
    # This also gives a cleaner error message when the package is missing.
    try:
        from google.cloud import bigquery
    except ImportError:
        print(
            "Error: google-cloud-bigquery is not installed.\n"
            "Install it with: pip install google-cloud-bigquery\n"
            "Or:              pip install -r tools/requirements.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build the client -- this is where auth errors surface.
    try:
        client = bigquery.Client(project=project, location=location)
    except Exception as exc:
        error_msg = str(exc).lower()
        if "credentials" in error_msg or "authentication" in error_msg or "auth" in error_msg:
            print(
                "Error: GCP authentication not configured.\n"
                "Run:   gcloud auth application-default login\n"
                "Then retry this command.",
                file=sys.stderr,
            )
        else:
            print(f"Error creating BigQuery client: {exc}", file=sys.stderr)
        sys.exit(1)

    # Configure the dry-run job.
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    try:
        query_job = client.query(query, job_config=job_config)
    except Exception as exc:
        error_msg = str(exc)

        # Provide targeted advice for common BigQuery errors.
        if "Not found" in error_msg:
            print(
                f"Error: Resource not found.\n"
                f"BigQuery says: {error_msg}\n\n"
                f"Check that the dataset and table names are correct and that\n"
                f"your project has access to them.",
                file=sys.stderr,
            )
        elif "Syntax error" in error_msg or "Unrecognized" in error_msg:
            print(
                f"Error: SQL syntax error.\n"
                f"BigQuery says: {error_msg}",
                file=sys.stderr,
            )
        elif "Access Denied" in error_msg or "permission" in error_msg.lower():
            print(
                f"Error: Permission denied.\n"
                f"BigQuery says: {error_msg}\n\n"
                f"Ensure your account has bigquery.jobs.create permission\n"
                f"on the project, and bigquery.tables.getData on the tables.",
                file=sys.stderr,
            )
        else:
            print(f"Error: {error_msg}", file=sys.stderr)
        sys.exit(1)

    # Extract results from the dry-run response.
    total_bytes = query_job.total_bytes_processed
    cost = calculate_cost(total_bytes)

    # Check whether the query would hit cache (dry-run always sets use_query_cache
    # to False, but we report the cache status from the job statistics).
    cache_hit = getattr(query_job, "cache_hit", None)
    cache_status = "Yes" if cache_hit else "No"

    # Truncate query preview to keep output readable.
    query_preview = query.strip().replace("\n", " ")
    if len(query_preview) > 100:
        query_preview = query_preview[:97] + "..."

    # Print the estimate in a clear, scannable format.
    print()
    print("BigQuery Cost Estimate")
    print("======================")
    print(f"Query:    {query_preview}")
    print(f"Bytes:    {format_bytes(total_bytes)}")
    print(f"Cost:     ${cost:.4f} (on-demand @ ${COST_PER_TB_USD}/TB)")
    print(f"Cached:   {cache_status}")
    print()
    print(generate_tip(total_bytes, cost))
    print()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list (defaults to sys.argv[1:] when None).

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Estimate BigQuery query cost using dry-run mode.",
        epilog=(
            "Examples:\n"
            '  %(prog)s --query "SELECT * FROM dataset.table"\n'
            "  %(prog)s --file path/to/query.sql\n"
            "  %(prog)s --file query.sql --project my-project --location us-central1\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Query source -- exactly one of these is required.
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        "--query",
        type=str,
        help='SQL query string (e.g., --query "SELECT * FROM dataset.table")',
    )
    query_group.add_argument(
        "--file",
        type=str,
        help="Path to a .sql file containing the query",
    )

    # Optional GCP configuration.
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="GCP project ID (defaults to gcloud config project)",
    )
    parser.add_argument(
        "--location",
        type=str,
        default="US",
        help='BigQuery processing location (default: "US")',
    )

    return parser.parse_args(argv)


def main() -> None:
    """Entry point: parse args, read query, run dry-run estimate."""
    args = parse_args()

    # Resolve the query text from --query or --file.
    if args.query:
        query = args.query
    else:
        sql_path = Path(args.file)
        if not sql_path.is_file():
            print(f"Error: SQL file not found: {sql_path}", file=sys.stderr)
            sys.exit(1)
        query = sql_path.read_text(encoding="utf-8").strip()
        if not query:
            print(f"Error: SQL file is empty: {sql_path}", file=sys.stderr)
            sys.exit(1)

    run_dry_run(query=query, project=args.project, location=args.location)


if __name__ == "__main__":
    main()
