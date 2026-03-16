"""Model scoring pipeline for insurance pricing adequacy assessment.

Scores all eligible policies with the trained GLM and assesses whether
current premiums are adequate relative to predicted pure premiums.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from feature_engineering import get_training_data
from model_training import GLMResult, predict, save_predictions_to_duckdb


def score_all_policies(
    con: duckdb.DuckDBPyConnection,
    model_result: GLMResult,
    sql_dir: Path,
) -> dict:
    """Score all policies in the training set and run pricing adequacy SQL.

    Args:
        con: DuckDB connection with feature tables.
        model_result: Trained GLM model.
        sql_dir: Path to the sql/ directory.

    Returns:
        Summary dict with total_scored, avg_predicted_pp, avg_actual_premium,
        pct_underpriced, pct_overpriced, pct_adequate.
    """
    # Get all data (both train and test) for scoring
    for split_name in ["train", "test"]:
        X, _, _ = get_training_data(con, split_name)
        predictions = predict(model_result, X)

        # Get policy IDs for this split
        ids = con.execute(f"""
            SELECT policy_id FROM model_training_set
            WHERE split = '{split_name}'
            ORDER BY policy_id
        """).fetchall()
        policy_ids = [row[0] for row in ids]

        # Save to temp table
        if split_name == "train":
            save_predictions_to_duckdb(con, policy_ids, predictions)
        else:
            # Append test predictions
            data = list(zip(policy_ids, predictions.tolist()))
            con.executemany(
                "INSERT INTO model_predictions (policy_id, predicted_pure_premium) VALUES (?, ?)",
                data,
            )

    # Run scoring SQL
    scoring_sql = (sql_dir / "model" / "model_scoring.sql").read_text(encoding="utf-8")
    con.execute(scoring_sql)

    # Compute summary
    summary_row = con.execute("""
        SELECT
            COUNT(*) AS total_scored,
            ROUND(AVG(predicted_pure_premium), 2) AS avg_predicted_pp,
            ROUND(AVG(actual_premium), 2) AS avg_actual_premium,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'underpriced' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_underpriced,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'overpriced' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_overpriced,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'adequate' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_adequate
        FROM model_scoring
        WHERE pricing_assessment IS NOT NULL
    """).fetchone()

    return {
        "total_scored": summary_row[0],
        "avg_predicted_pp": float(summary_row[1]),
        "avg_actual_premium": float(summary_row[2]),
        "pct_underpriced": float(summary_row[3]),
        "pct_overpriced": float(summary_row[4]),
        "pct_adequate": float(summary_row[5]),
    }


def get_pricing_adequacy_report(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Get pricing adequacy breakdown by coverage type.

    Args:
        con: DuckDB connection with model_scoring table.

    Returns:
        List of dicts with coverage_type, policy_count, avg_adequacy_ratio,
        pct_underpriced, pct_overpriced, pct_adequate.
    """
    results = con.execute("""
        SELECT
            coverage_type,
            COUNT(*) AS policy_count,
            ROUND(AVG(price_adequacy_ratio), 4) AS avg_adequacy_ratio,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'underpriced' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_underpriced,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'overpriced' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_overpriced,
            ROUND(100.0 * SUM(CASE WHEN pricing_assessment = 'adequate' THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS pct_adequate
        FROM model_scoring
        WHERE pricing_assessment IS NOT NULL
        GROUP BY coverage_type
        ORDER BY coverage_type
    """).fetchall()

    return [
        {
            "coverage_type": row[0],
            "policy_count": row[1],
            "avg_adequacy_ratio": float(row[2]),
            "pct_underpriced": float(row[3]),
            "pct_overpriced": float(row[4]),
            "pct_adequate": float(row[5]),
        }
        for row in results
    ]


def print_scoring_report(
    summary: dict,
    by_coverage: list[dict],
) -> None:
    """Print formatted scoring report to console.

    Args:
        summary: Output of score_all_policies.
        by_coverage: Output of get_pricing_adequacy_report.
    """
    print("\n" + "=" * 70)
    print("PRICING ADEQUACY REPORT")
    print("=" * 70)

    print(f"\nTotal Policies Scored:   {summary['total_scored']:>8,d}")
    print(f"Avg Predicted Pure Prem: {summary['avg_predicted_pp']:>12,.2f} MXN")
    print(f"Avg Actual Premium:      {summary['avg_actual_premium']:>12,.2f} MXN")
    print(f"  Underpriced:           {summary['pct_underpriced']:>8.1f}%")
    print(f"  Overpriced:            {summary['pct_overpriced']:>8.1f}%")
    print(f"  Adequate:              {summary['pct_adequate']:>8.1f}%")

    print("\n--- By Coverage Type ---")
    header = (
        f"{'Coverage':<12s} {'Count':>7s} {'Avg A/E':>10s} "
        f"{'Under%':>8s} {'Over%':>8s} {'Adeq%':>8s}"
    )
    print(header)
    print("-" * len(header))
    for row in by_coverage:
        print(
            f"{row['coverage_type']:<12s} {row['policy_count']:>7,d} "
            f"{row['avg_adequacy_ratio']:>10.4f} "
            f"{row['pct_underpriced']:>8.1f} {row['pct_overpriced']:>8.1f} "
            f"{row['pct_adequate']:>8.1f}"
        )
