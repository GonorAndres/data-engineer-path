"""GLM model training for insurance pricing.

Trains Tweedie, Poisson, and Gamma GLMs using statsmodels for
pure premium, frequency, and severity modeling respectively.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import statsmodels.api as sm

import duckdb


@dataclass
class GLMResult:
    """Container for fitted GLM results."""

    model: Any  # statsmodels GLMResultsWrapper
    coefficients: dict[str, float]
    aic: float
    deviance: float
    feature_names: list[str]
    family: str


def _add_intercept(X: np.ndarray) -> np.ndarray:
    """Prepend a column of ones to X for the intercept term.

    Unlike sm.add_constant, this always adds the column regardless of
    whether the data already contains a near-constant column (e.g.,
    exposure_years ~ 1.0).
    """
    ones = np.ones((X.shape[0], 1))
    return np.hstack([ones, X])


def train_pure_premium_model(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    exposure: np.ndarray | None = None,
) -> GLMResult:
    """Train a Tweedie GLM for pure premium prediction.

    The Tweedie distribution with power=1.5 handles the mixed nature of
    pure premium data (point mass at zero + continuous positive values),
    which is standard in actuarial pricing.

    Args:
        X: Feature matrix (n_samples, n_features).
        y: Target pure premium values.
        feature_names: Names for each feature column.
        exposure: Optional exposure weights.

    Returns:
        GLMResult with the fitted model.
    """
    X_const = _add_intercept(X)
    family = sm.families.Tweedie(var_power=1.5)

    kwargs: dict[str, Any] = {}
    if exposure is not None:
        kwargs["exposure"] = exposure

    model = sm.GLM(y, X_const, family=family, **kwargs)
    result = model.fit(maxiter=100, method="irls")

    coef_names = ["const"] + list(feature_names)
    coefficients = dict(zip(coef_names, result.params))

    return GLMResult(
        model=result,
        coefficients=coefficients,
        aic=float(result.aic),
        deviance=float(result.deviance),
        feature_names=list(feature_names),
        family="Tweedie(p=1.5)",
    )


def train_frequency_model(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    exposure: np.ndarray | None = None,
) -> GLMResult:
    """Train a Poisson GLM for claim frequency.

    Uses log link function. If exposure is provided, it is used as an
    offset term (log(exposure)), which is standard actuarial practice
    for rate models.

    Args:
        X: Feature matrix.
        y: Claim count target.
        feature_names: Feature column names.
        exposure: Optional exposure in policy-years.

    Returns:
        GLMResult with the fitted Poisson model.
    """
    X_const = _add_intercept(X)
    family = sm.families.Poisson(link=sm.families.links.Log())

    kwargs: dict[str, Any] = {}
    if exposure is not None:
        kwargs["offset"] = np.log(np.maximum(exposure, 1e-10))

    model = sm.GLM(y, X_const, family=family, **kwargs)
    result = model.fit(maxiter=100, method="irls")

    coef_names = ["const"] + list(feature_names)
    coefficients = dict(zip(coef_names, result.params))

    return GLMResult(
        model=result,
        coefficients=coefficients,
        aic=float(result.aic),
        deviance=float(result.deviance),
        feature_names=list(feature_names),
        family="Poisson",
    )


def train_severity_model(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
) -> GLMResult:
    """Train a Gamma GLM for claim severity.

    Only fits on observations where y > 0 (claims with positive loss).
    The Gamma distribution is appropriate for modeling positive continuous
    claim amounts.

    Args:
        X: Feature matrix.
        y: Severity target (must contain positive values).
        feature_names: Feature column names.

    Returns:
        GLMResult with the fitted Gamma model.
    """
    # Filter to positive severity only
    mask = y > 0
    X_pos = X[mask]
    y_pos = y[mask]

    if len(y_pos) == 0:
        raise ValueError("No positive severity observations to fit.")

    X_const = _add_intercept(X_pos)
    family = sm.families.Gamma(link=sm.families.links.Log())

    model = sm.GLM(y_pos, X_const, family=family)
    result = model.fit(maxiter=100, method="irls")

    coef_names = ["const"] + list(feature_names)
    coefficients = dict(zip(coef_names, result.params))

    return GLMResult(
        model=result,
        coefficients=coefficients,
        aic=float(result.aic),
        deviance=float(result.deviance),
        feature_names=list(feature_names),
        family="Gamma",
    )


def predict(model_result: GLMResult, X: np.ndarray) -> np.ndarray:
    """Generate predictions from a fitted GLM.

    Args:
        model_result: Fitted GLMResult from any training function.
        X: Feature matrix (without constant -- added internally).

    Returns:
        Array of predicted values.
    """
    X_const = _add_intercept(X)
    return model_result.model.predict(X_const)


def save_predictions_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    policy_ids: list[int],
    predictions: np.ndarray,
    table_name: str = "model_predictions",
) -> int:
    """Save model predictions to a DuckDB table.

    Args:
        con: DuckDB connection.
        policy_ids: List of policy IDs corresponding to predictions.
        predictions: Predicted pure premium values.
        table_name: Name for the predictions table.

    Returns:
        Number of rows inserted.
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            policy_id INTEGER,
            predicted_pure_premium DOUBLE
        )
    """)

    # Insert predictions in batch
    data = list(zip(policy_ids, predictions.tolist()))
    con.executemany(
        f"INSERT INTO {table_name} (policy_id, predicted_pure_premium) VALUES (?, ?)",
        data,
    )

    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return count
