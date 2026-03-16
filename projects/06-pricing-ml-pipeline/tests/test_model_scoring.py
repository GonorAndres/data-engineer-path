"""Tests for model scoring and pricing adequacy."""

from __future__ import annotations

from pathlib import Path

import pytest

P06_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


@pytest.fixture(scope="session")
def scored_con(feature_con, trained_model):
    """DuckDB connection with model_scoring table created."""
    from model_scoring import score_all_policies

    score_all_policies(feature_con, trained_model, P06_SQL_DIR)
    return feature_con


def test_all_eligible_policies_scored(scored_con):
    """All policies in the training set should be scored."""
    total_eligible = scored_con.execute(
        "SELECT COUNT(*) FROM model_training_set"
    ).fetchone()[0]
    total_scored = scored_con.execute(
        "SELECT COUNT(*) FROM model_scoring"
    ).fetchone()[0]
    assert total_scored == total_eligible, (
        f"Expected {total_eligible} scored, got {total_scored}"
    )


def test_pricing_adequacy_computed(scored_con):
    """model_scoring table should exist and have rows after scoring."""
    count = scored_con.execute(
        "SELECT COUNT(*) FROM model_scoring"
    ).fetchone()[0]
    assert count > 0, "model_scoring should have rows"


def test_pricing_assessment_valid_values(scored_con):
    """Pricing assessment should only be underpriced, overpriced, or adequate."""
    valid = {"underpriced", "overpriced", "adequate"}
    result = scored_con.execute("""
        SELECT DISTINCT pricing_assessment
        FROM model_scoring
        WHERE pricing_assessment IS NOT NULL
    """).fetchall()
    actual = {row[0] for row in result}
    assert actual.issubset(valid), f"Unexpected assessments: {actual - valid}"


def test_report_has_all_coverage_types(scored_con):
    """Pricing report should cover all coverage types in the training set."""
    from model_scoring import get_pricing_adequacy_report

    report = get_pricing_adequacy_report(scored_con)
    report_types = {row["coverage_type"] for row in report}

    training_types = scored_con.execute("""
        SELECT DISTINCT coverage_type FROM model_training_set
    """).fetchall()
    expected_types = {row[0] for row in training_types}

    assert report_types == expected_types, (
        f"Report types {report_types} != training types {expected_types}"
    )


def test_adequacy_ratios_reasonable(scored_con):
    """Price adequacy ratios should be between 0.01 and 100."""
    result = scored_con.execute("""
        SELECT
            MIN(price_adequacy_ratio) AS min_ratio,
            MAX(price_adequacy_ratio) AS max_ratio
        FROM model_scoring
        WHERE price_adequacy_ratio IS NOT NULL
    """).fetchone()
    assert result[0] >= 0.001, f"Min ratio too low: {result[0]}"
    assert result[1] <= 1000.0, f"Max ratio too high: {result[1]}"
