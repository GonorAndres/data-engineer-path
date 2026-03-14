"""Query BigQuery claims warehouse and display results.

Reads the deployed warehouse and prints the same summary as the local
DuckDB pipeline for comparison. Uses maximum_bytes_billed as a safety
guardrail.

Usage:
    python scripts/query_bigquery.py --project YOUR_PROJECT_ID
    python scripts/query_bigquery.py --project YOUR_PROJECT_ID --env prod
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery


MAX_BYTES_BILLED = 10 * 1024 * 1024 * 1024  # 10 GB


def query(client: bigquery.Client, sql: str) -> list:
    """Execute a query with cost guardrail."""
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED,
    )
    return list(client.query(sql, job_config=job_config).result())


def main() -> None:
    parser = argparse.ArgumentParser(description="Query BigQuery claims warehouse")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    prefix = "dev_" if args.env == "dev" else ""

    # Claims summary
    rows = query(client, f"""
        SELECT
            COUNT(*) AS total_claims,
            COUNTIF(claim_status = 'open') AS open_claims,
            COUNTIF(claim_status = 'closed') AS closed_claims,
            ROUND(SUM(total_paid), 2) AS total_paid,
            ROUND(SUM(incurred_amount), 2) AS total_incurred,
            ROUND(AVG(total_paid), 2) AS avg_paid
        FROM `{args.project}.{prefix}claims_analytics.fct_claims`
    """)
    r = rows[0]
    print(f"Claims: {r.total_claims:,d} ({r.open_claims:,d} open, {r.closed_claims:,d} closed)")
    print(f"Total Paid: ${r.total_paid:,.2f} MXN")
    print(f"Total Incurred: ${r.total_incurred:,.2f} MXN")

    # Loss triangle
    print("\n--- Loss Triangle ---")
    rows = query(client, f"""
        SELECT * FROM `{args.project}.{prefix}claims_reports.rpt_loss_triangle`
        ORDER BY accident_year
    """)
    for r in rows:
        vals = [r.dev_year_0, r.dev_year_1, r.dev_year_2, r.dev_year_3, r.dev_year_4, r.dev_year_5]
        formatted = [f"{v:>14,.0f}" if v else f"{'':>14s}" for v in vals]
        print(f"  {r.accident_year}: {''.join(formatted)}")

    print("\nBigQuery query complete.")


if __name__ == "__main__":
    main()
