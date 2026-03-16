"""Tests for GLM model training."""

from __future__ import annotations

import numpy as np


def test_model_fits_without_error(trained_model):
    """Model should fit without raising exceptions."""
    assert trained_model is not None
    assert trained_model.model is not None


def test_model_has_coefficients(trained_model):
    """Model should have non-empty coefficient dict."""
    assert len(trained_model.coefficients) > 0


def test_coefficients_are_finite(trained_model):
    """All coefficients should be finite numbers."""
    for name, coef in trained_model.coefficients.items():
        assert np.isfinite(coef), f"Coefficient {name} is not finite: {coef}"


def test_predictions_non_negative(trained_model, test_data):
    """Predictions should be non-negative (pure premium cannot be negative)."""
    from model_training import predict

    X_test, _, _ = test_data
    preds = predict(trained_model, X_test)
    assert np.all(preds >= -1e-6), f"Found negative predictions: min={preds.min()}"


def test_aic_is_finite(trained_model):
    """AIC should be a finite number."""
    assert np.isfinite(trained_model.aic), f"AIC is not finite: {trained_model.aic}"


def test_predict_returns_correct_shape(trained_model, test_data):
    """Predictions should have the same length as test data."""
    from model_training import predict

    X_test, y_test, _ = test_data
    preds = predict(trained_model, X_test)
    assert len(preds) == len(y_test), (
        f"Prediction shape mismatch: {len(preds)} vs {len(y_test)}"
    )


def test_predictions_reasonable_range(trained_model, test_data):
    """Predictions should be in a reasonable range (0 to 1,000,000 MXN)."""
    from model_training import predict

    X_test, _, _ = test_data
    preds = predict(trained_model, X_test)
    assert preds.max() < 1_000_000, f"Max prediction too high: {preds.max()}"


def test_predictions_saved_to_duckdb(feature_con, trained_model, test_data):
    """Predictions should be saveable to DuckDB."""
    from model_training import predict, save_predictions_to_duckdb

    X_test, _, _ = test_data
    preds = predict(trained_model, X_test)

    # Get policy IDs
    ids = feature_con.execute("""
        SELECT policy_id FROM model_training_set WHERE split = 'test'
    """).fetchall()
    policy_ids = [row[0] for row in ids]

    count = save_predictions_to_duckdb(
        feature_con, policy_ids, preds, table_name="test_predictions_check"
    )
    assert count == len(policy_ids)

    # Clean up
    feature_con.execute("DROP TABLE IF EXISTS test_predictions_check")
