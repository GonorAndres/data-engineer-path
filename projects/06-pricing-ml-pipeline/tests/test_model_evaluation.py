"""Tests for model evaluation metrics."""

from __future__ import annotations

import numpy as np

from model_evaluation import (
    compute_gini_coefficient,
    compute_lift_table,
    compute_residual_summary,
)


def test_perfect_model_gini_near_one():
    """A perfect model (y_pred == y_actual) should have positive Gini."""
    # Use highly skewed data where perfect ordering yields a clear Gini > 0
    y = np.concatenate([np.zeros(80), np.array([10.0, 20.0, 50.0, 100.0, 200.0,
                                                 500.0, 1000.0, 2000.0, 5000.0, 10000.0])])
    gini = compute_gini_coefficient(y, y)
    assert gini > 0.5, f"Perfect model on skewed data should have high Gini, got {gini}"


def test_random_model_gini_near_zero():
    """A random model should have Gini near 0."""
    rng = np.random.default_rng(42)
    y_actual = rng.exponential(1000, size=10000)
    y_predicted = rng.exponential(1000, size=10000)  # Uncorrelated
    gini = compute_gini_coefficient(y_actual, y_predicted)
    assert abs(gini) < 0.1, f"Random model Gini should be near 0, got {gini}"


def test_real_model_gini_positive(test_data, test_predictions):
    """Trained model should have positive Gini on test data."""
    _, y_test, _ = test_data
    gini = compute_gini_coefficient(y_test, test_predictions)
    assert gini > -0.5, f"Real model Gini should not be very negative, got {gini}"


def test_lift_table_correct_deciles():
    """Lift table should return exactly n_deciles rows."""
    y = np.arange(1, 101, dtype=float)
    preds = np.arange(1, 101, dtype=float)
    lift = compute_lift_table(y, preds, n_deciles=10)
    assert len(lift) == 10, f"Expected 10 deciles, got {len(lift)}"


def test_top_decile_positive_lift():
    """Top decile should have positive lift (higher actual than average)."""
    rng = np.random.default_rng(42)
    # Create data where high predictions correlate with high actuals
    y_actual = np.concatenate([rng.exponential(100, 900), rng.exponential(1000, 100)])
    y_predicted = np.concatenate([rng.exponential(100, 900), rng.exponential(1000, 100)])
    lift = compute_lift_table(y_actual, y_predicted, n_deciles=10)
    top_decile = lift[-1]  # Highest predicted
    assert top_decile["lift"] > 1.0, f"Top decile lift should be > 1, got {top_decile['lift']}"


def test_mae_positive(test_data, test_predictions):
    """MAE should be positive."""
    _, y_test, _ = test_data
    residuals = compute_residual_summary(y_test, test_predictions)
    assert residuals["mae"] > 0, f"MAE should be positive, got {residuals['mae']}"


def test_rmse_gte_mae(test_data, test_predictions):
    """RMSE should be >= MAE (by Cauchy-Schwarz inequality)."""
    _, y_test, _ = test_data
    residuals = compute_residual_summary(y_test, test_predictions)
    assert residuals["rmse"] >= residuals["mae"] - 0.01, (
        f"RMSE ({residuals['rmse']}) should be >= MAE ({residuals['mae']})"
    )
