"""Model evaluation metrics for insurance pricing GLMs.

Computes Gini coefficient, lift tables, residual summaries, and
coefficient analysis for actuarial model validation.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error

import duckdb

from model_training import GLMResult


def compute_gini_coefficient(
    y_actual: np.ndarray,
    y_predicted: np.ndarray,
) -> float:
    """Compute the Gini coefficient using the ordered Lorenz curve approach.

    The Gini coefficient measures how well the model discriminates between
    high and low risk policies. A perfect model has Gini = 1, random = 0.

    Args:
        y_actual: Actual pure premium values.
        y_predicted: Predicted pure premium values.

    Returns:
        Gini coefficient between -1 and 1.
    """
    n = len(y_actual)
    if n == 0:
        return 0.0

    # Sort by predicted values (ascending)
    order = np.argsort(y_predicted)
    y_sorted = y_actual[order]

    # Compute Lorenz curve
    cumulative = np.cumsum(y_sorted)
    total = cumulative[-1]

    if total == 0:
        return 0.0

    lorenz = cumulative / total

    # Gini from Lorenz curve: 1 - 2 * area under Lorenz
    # Area under Lorenz using trapezoidal rule
    area = np.sum(lorenz) / n
    gini = 1.0 - 2.0 * area

    return float(gini)


def compute_lift_table(
    y_actual: np.ndarray,
    y_predicted: np.ndarray,
    n_deciles: int = 10,
) -> list[dict]:
    """Compute lift table by predicted decile.

    Groups policies by predicted pure premium decile and compares
    predicted vs actual values. Essential for validating pricing models.

    Args:
        y_actual: Actual pure premium values.
        y_predicted: Predicted pure premium values.
        n_deciles: Number of groups (default 10).

    Returns:
        List of dicts with decile, avg_predicted, avg_actual, lift, count.
    """
    n = len(y_actual)
    if n == 0:
        return []

    overall_avg = float(np.mean(y_actual))
    if overall_avg == 0:
        overall_avg = 1e-10  # Avoid division by zero

    # Sort by predicted values
    order = np.argsort(y_predicted)
    y_actual_sorted = y_actual[order]
    y_pred_sorted = y_predicted[order]

    # Split into deciles
    decile_size = n // n_deciles
    table: list[dict] = []

    for i in range(n_deciles):
        start = i * decile_size
        if i == n_deciles - 1:
            end = n  # Last decile gets remainder
        else:
            end = (i + 1) * decile_size

        actual_slice = y_actual_sorted[start:end]
        pred_slice = y_pred_sorted[start:end]

        avg_actual = float(np.mean(actual_slice))
        avg_predicted = float(np.mean(pred_slice))
        lift = avg_actual / overall_avg

        table.append({
            "decile": i + 1,
            "avg_predicted": round(avg_predicted, 2),
            "avg_actual": round(avg_actual, 2),
            "lift": round(lift, 4),
            "count": len(actual_slice),
        })

    return table


def compute_residual_summary(
    y_actual: np.ndarray,
    y_predicted: np.ndarray,
) -> dict:
    """Compute residual summary statistics.

    Args:
        y_actual: Actual values.
        y_predicted: Predicted values.

    Returns:
        Dict with mae, rmse, mape, mean_residual, std_residual,
        median_absolute_error.
    """
    residuals = y_predicted - y_actual
    abs_residuals = np.abs(residuals)

    mae = float(mean_absolute_error(y_actual, y_predicted))
    rmse = float(np.sqrt(mean_squared_error(y_actual, y_predicted)))

    # MAPE: avoid division by zero
    nonzero_mask = y_actual != 0
    if np.any(nonzero_mask):
        mape = float(np.mean(np.abs(residuals[nonzero_mask] / y_actual[nonzero_mask])))
    else:
        mape = 0.0

    return {
        "mae": round(mae, 2),
        "rmse": round(rmse, 2),
        "mape": round(mape, 4),
        "mean_residual": round(float(np.mean(residuals)), 2),
        "std_residual": round(float(np.std(residuals)), 2),
        "median_absolute_error": round(float(np.median(abs_residuals)), 2),
    }


def compute_coefficient_summary(model_result: GLMResult) -> list[dict]:
    """Extract coefficient summary from a fitted GLM.

    Sorted by absolute coefficient value (descending) to highlight
    the most influential rating factors.

    Args:
        model_result: Fitted GLMResult.

    Returns:
        List of dicts with feature, coefficient, std_error, z_value, p_value.
    """
    summary = model_result.model.summary2().tables[1]
    coef_names = ["const"] + list(model_result.feature_names)

    coefficients: list[dict] = []
    for i, name in enumerate(coef_names):
        if i < len(summary):
            row = summary.iloc[i]
            coefficients.append({
                "feature": name,
                "coefficient": round(float(row["Coef."]), 6),
                "std_error": round(float(row["Std.Err."]), 6),
                "z_value": round(float(row["z"]), 4),
                "p_value": round(float(row["P>|z|"]), 6),
            })

    # Sort by absolute coefficient value descending
    coefficients.sort(key=lambda x: abs(x["coefficient"]), reverse=True)
    return coefficients


def run_sql_evaluation(
    con: duckdb.DuckDBPyConnection,
    sql_dir: Path,
) -> None:
    """Run the SQL-based model evaluation and print results.

    Args:
        con: DuckDB connection with model_predictions table.
        sql_dir: Path to the sql/ directory.
    """
    filepath = sql_dir / "evaluation" / "model_evaluation.sql"
    sql = filepath.read_text(encoding="utf-8")
    con.execute(sql)

    results = con.execute("""
        SELECT split, coverage_type, policy_count,
               avg_actual_pp, avg_predicted_pp, avg_residual,
               mae, rmse, ae_ratio
        FROM model_evaluation
        ORDER BY split, coverage_type
    """).fetchall()

    print("\n--- Segment Evaluation (SQL) ---")
    header = (
        f"{'Split':<8s} {'Coverage':<12s} {'Count':>7s} "
        f"{'Avg Actual':>12s} {'Avg Pred':>12s} {'MAE':>10s} "
        f"{'RMSE':>10s} {'A/E':>8s}"
    )
    print(header)
    print("-" * len(header))
    for row in results:
        ae_str = f"{row[8]:.4f}" if row[8] is not None else "N/A"
        print(
            f"{row[0]:<8s} {row[1]:<12s} {row[2]:>7,d} "
            f"{row[3]:>12,.2f} {row[4]:>12,.2f} {row[5]:>10,.2f} "
            f"{row[6]:>10,.2f} {ae_str:>8s}"
        )


def print_evaluation_report(
    gini: float,
    lift_table: list[dict],
    residuals: dict,
    coefficients: list[dict],
) -> None:
    """Print a formatted evaluation report to console.

    Args:
        gini: Gini coefficient.
        lift_table: Output of compute_lift_table.
        residuals: Output of compute_residual_summary.
        coefficients: Output of compute_coefficient_summary.
    """
    print("\n" + "=" * 70)
    print("MODEL EVALUATION REPORT")
    print("=" * 70)

    # Gini
    print(f"\nGini Coefficient: {gini:.4f}")

    # Residual summary
    print("\n--- Residual Summary ---")
    for key, value in residuals.items():
        print(f"  {key:<25s} {value:>12}")

    # Lift table
    print("\n--- Lift Table (by predicted decile) ---")
    print(f"{'Decile':>7s} {'Count':>7s} {'Avg Pred':>12s} {'Avg Actual':>12s} {'Lift':>8s}")
    print("-" * 48)
    for row in lift_table:
        print(
            f"{row['decile']:>7d} {row['count']:>7d} "
            f"{row['avg_predicted']:>12,.2f} {row['avg_actual']:>12,.2f} "
            f"{row['lift']:>8.4f}"
        )

    # Top 10 coefficients
    print("\n--- Top 10 Model Coefficients ---")
    print(f"{'Feature':<40s} {'Coef':>12s} {'Std Err':>10s} {'z':>8s} {'p-value':>10s}")
    print("-" * 82)
    for row in coefficients[:10]:
        print(
            f"{row['feature']:<40s} {row['coefficient']:>12.6f} "
            f"{row['std_error']:>10.6f} {row['z_value']:>8.4f} "
            f"{row['p_value']:>10.6f}"
        )
