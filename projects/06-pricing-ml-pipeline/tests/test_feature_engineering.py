"""Tests for feature engineering SQL transforms."""

from __future__ import annotations


def test_feat_policy_experience_created(feature_con):
    """feat_policy_experience table exists and has rows."""
    count = feature_con.execute(
        "SELECT COUNT(*) FROM feat_policy_experience"
    ).fetchone()[0]
    assert count > 0, "feat_policy_experience should have rows"


def test_one_row_per_policy(feature_con):
    """feat_policy_experience has no duplicate policy_ids."""
    result = feature_con.execute("""
        SELECT COUNT(*) AS total, COUNT(DISTINCT policy_id) AS distinct_ids
        FROM feat_policy_experience
    """).fetchone()
    assert result[0] == result[1], "Should have one row per policy_id"


def test_exposure_years_non_negative(feature_con):
    """All exposure_years values should be >= 0."""
    negatives = feature_con.execute("""
        SELECT COUNT(*) FROM feat_policy_experience
        WHERE exposure_years < 0
    """).fetchone()[0]
    assert negatives == 0, "exposure_years should never be negative"


def test_claim_frequency_matches_manual(feature_con):
    """Verify claim_frequency = claim_count / exposure_years for a policy with claims."""
    row = feature_con.execute("""
        SELECT claim_count, exposure_years, claim_frequency
        FROM feat_policy_experience
        WHERE claim_count > 0 AND exposure_years > 0
        LIMIT 1
    """).fetchone()
    if row is not None:
        expected = row[0] / row[1]
        actual = row[2]
        assert abs(actual - expected) < 0.001, (
            f"claim_frequency mismatch: {actual} vs {expected}"
        )


def test_has_large_loss_flag_correct(feature_con):
    """has_large_loss should be True when max_severity > 200000."""
    wrong = feature_con.execute("""
        SELECT COUNT(*) FROM feat_policy_experience
        WHERE (max_severity > 200000 AND has_large_loss = FALSE)
           OR (max_severity <= 200000 AND has_large_loss = TRUE)
    """).fetchone()[0]
    assert wrong == 0, "has_large_loss flag should match max_severity > 200000"


def test_feat_risk_segments_created(feature_con):
    """feat_risk_segments table exists and has rows."""
    count = feature_con.execute(
        "SELECT COUNT(*) FROM feat_risk_segments"
    ).fetchone()[0]
    assert count > 0, "feat_risk_segments should have rows"


def test_valid_age_bands(feature_con):
    """Age bands should be one of the expected values."""
    expected_bands = {"18-25", "26-35", "36-45", "46-55", "56-65", "65+"}
    result = feature_con.execute("""
        SELECT DISTINCT age_band FROM feat_risk_segments
    """).fetchall()
    actual_bands = {row[0] for row in result}
    assert actual_bands.issubset(expected_bands), (
        f"Unexpected age bands: {actual_bands - expected_bands}"
    )


def test_premium_bands_are_quartiles(feature_con):
    """Premium bands should be Q1, Q2, Q3, Q4."""
    expected = {"Q1", "Q2", "Q3", "Q4"}
    result = feature_con.execute("""
        SELECT DISTINCT premium_band FROM feat_risk_segments
    """).fetchall()
    actual = {row[0] for row in result}
    assert actual == expected, f"Expected {expected}, got {actual}"


def test_model_training_set_has_train_test_split(feature_con):
    """model_training_set should have both train and test rows."""
    splits = feature_con.execute("""
        SELECT DISTINCT split FROM model_training_set
    """).fetchall()
    split_values = {row[0] for row in splits}
    assert "train" in split_values, "Should have train split"
    assert "test" in split_values, "Should have test split"


def test_excluded_cancelled_policies(feature_con):
    """model_training_set should not contain cancelled policies."""
    cancelled = feature_con.execute("""
        SELECT COUNT(*)
        FROM model_training_set mts
        JOIN feat_policy_experience fpe ON mts.policy_id = fpe.policy_id
        WHERE fpe.policy_status = 'cancelled'
    """).fetchone()[0]
    assert cancelled == 0, "Cancelled policies should be excluded from training set"
