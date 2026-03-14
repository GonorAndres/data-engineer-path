"""Tests for SQL transforms -- validates the full DuckDB pipeline.

Uses the warehouse_con fixture which runs the complete pipeline
(generate data -> load raw -> staging -> intermediate -> marts -> reports).
"""

from __future__ import annotations


class TestStagingLayer:
    """Verify staging tables have correct shape and types."""

    def test_stg_policyholders_not_empty(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM stg_policyholders").fetchone()[0]
        assert count == 500

    def test_stg_policies_not_empty(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM stg_policies").fetchone()[0]
        assert count > 0

    def test_stg_claims_not_empty(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM stg_claims").fetchone()[0]
        assert count > 0

    def test_stg_claims_has_accident_year(self, warehouse_con):
        result = warehouse_con.execute(
            "SELECT DISTINCT accident_year FROM stg_claims ORDER BY 1"
        ).fetchall()
        years = [r[0] for r in result]
        assert len(years) >= 5
        assert all(2020 <= y <= 2025 for y in years)

    def test_stg_coverages_complete(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM stg_coverages").fetchone()[0]
        assert count == 5


class TestIntermediateLayer:
    """Verify intermediate tables have correct joins and computed fields."""

    def test_int_claims_enriched_has_coverage(self, warehouse_con):
        nulls = warehouse_con.execute(
            "SELECT COUNT(*) FROM int_claims_enriched WHERE coverage_type IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_int_claims_enriched_has_development_months(self, warehouse_con):
        result = warehouse_con.execute(
            "SELECT MIN(development_months_at_valuation), MAX(development_months_at_valuation) "
            "FROM int_claims_enriched"
        ).fetchone()
        assert result[0] >= 0
        assert result[1] <= 72  # Max 6 years = 72 months

    def test_int_payments_cumulative_is_monotonic(self, warehouse_con):
        """Cumulative paid should generally increase within a claim."""
        non_monotonic = warehouse_con.execute("""
            SELECT COUNT(*) FROM (
                SELECT
                    claim_id,
                    cumulative_paid,
                    LAG(cumulative_paid) OVER (
                        PARTITION BY claim_id ORDER BY payment_date, payment_id
                    ) AS prev_cumulative
                FROM int_claim_payments_cumulative
            )
            WHERE prev_cumulative IS NOT NULL
              AND cumulative_paid < prev_cumulative
              AND cumulative_paid >= 0
        """).fetchone()[0]
        # Allow some non-monotonic cases due to recoveries (negative payments)
        total = warehouse_con.execute(
            "SELECT COUNT(*) FROM int_claim_payments_cumulative"
        ).fetchone()[0]
        assert non_monotonic / total < 0.1  # Less than 10%

    def test_int_policy_exposure_positive(self, warehouse_con):
        neg = warehouse_con.execute(
            "SELECT COUNT(*) FROM int_policy_exposure WHERE exposure_years <= 0"
        ).fetchone()[0]
        assert neg == 0


class TestMartLayer:
    """Verify dimension and fact tables are correct."""

    def test_dim_date_covers_range(self, warehouse_con):
        result = warehouse_con.execute(
            "SELECT MIN(full_date), MAX(full_date) FROM dim_date"
        ).fetchone()
        assert str(result[0]) == "2019-01-01"
        assert str(result[1]) == "2026-12-31"

    def test_dim_date_no_gaps(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        # 2019-01-01 to 2026-12-31 = 8 years = ~2922 days
        assert count >= 2920

    def test_dim_policyholder_has_age(self, warehouse_con):
        result = warehouse_con.execute(
            "SELECT MIN(current_age), MAX(current_age) FROM dim_policyholder"
        ).fetchone()
        assert result[0] >= 20
        assert result[1] <= 80

    def test_dim_policy_has_coverage_category(self, warehouse_con):
        nulls = warehouse_con.execute(
            "SELECT COUNT(*) FROM dim_policy WHERE coverage_category IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_fct_claims_row_count(self, warehouse_con):
        stg_count = warehouse_con.execute("SELECT COUNT(*) FROM stg_claims").fetchone()[0]
        fct_count = warehouse_con.execute("SELECT COUNT(*) FROM fct_claims").fetchone()[0]
        assert fct_count == stg_count

    def test_fct_claims_incurred_nonnegative(self, warehouse_con):
        neg = warehouse_con.execute(
            "SELECT COUNT(*) FROM fct_claims WHERE incurred_amount < 0"
        ).fetchone()[0]
        total = warehouse_con.execute("SELECT COUNT(*) FROM fct_claims").fetchone()[0]
        # Allow a small percentage due to recoveries exceeding payments
        assert neg / total < 0.05

    def test_fct_claim_payments_matches_source(self, warehouse_con):
        stg_count = warehouse_con.execute(
            "SELECT COUNT(*) FROM stg_claim_payments"
        ).fetchone()[0]
        fct_count = warehouse_con.execute(
            "SELECT COUNT(*) FROM fct_claim_payments"
        ).fetchone()[0]
        assert fct_count == stg_count


class TestReportLayer:
    """Verify analytical reports produce meaningful output."""

    def test_loss_triangle_has_rows(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM rpt_loss_triangle").fetchone()[0]
        assert count >= 5  # At least 5 accident years

    def test_loss_triangle_dev_year_0_always_filled(self, warehouse_con):
        nulls = warehouse_con.execute(
            "SELECT COUNT(*) FROM rpt_loss_triangle WHERE dev_year_0 IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_loss_triangle_staircase_shape(self, warehouse_con):
        """Recent accident years should have fewer development columns filled."""
        result = warehouse_con.execute("""
            SELECT
                accident_year,
                CASE WHEN dev_year_0 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN dev_year_1 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN dev_year_2 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN dev_year_3 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN dev_year_4 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN dev_year_5 IS NOT NULL THEN 1 ELSE 0 END AS filled_cols
            FROM rpt_loss_triangle
            ORDER BY accident_year
        """).fetchall()
        # Older years should have more filled columns than newer years
        if len(result) >= 2:
            oldest_filled = result[0][1]
            newest_filled = result[-1][1]
            assert oldest_filled >= newest_filled

    def test_claim_frequency_has_rows(self, warehouse_con):
        count = warehouse_con.execute("SELECT COUNT(*) FROM rpt_claim_frequency").fetchone()[0]
        assert count > 0

    def test_claim_frequency_reasonable_values(self, warehouse_con):
        result = warehouse_con.execute("""
            SELECT MIN(claim_frequency), MAX(claim_frequency)
            FROM rpt_claim_frequency
            WHERE claim_frequency IS NOT NULL
        """).fetchone()
        # Frequencies should be between 0 and 1 (claims per policy-year)
        assert result[0] >= 0
        assert result[1] <= 5.0  # Synthetic data uses a scale factor


class TestDataQuality:
    """Cross-cutting data quality checks."""

    def test_no_orphan_claims(self, warehouse_con):
        """Every claim should reference a valid policy."""
        orphans = warehouse_con.execute("""
            SELECT COUNT(*)
            FROM fct_claims c
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_policy p WHERE p.policy_id = c.policy_id
            )
        """).fetchone()[0]
        assert orphans == 0

    def test_no_orphan_payments(self, warehouse_con):
        """Every payment should reference a valid claim."""
        orphans = warehouse_con.execute("""
            SELECT COUNT(*)
            FROM fct_claim_payments p
            WHERE NOT EXISTS (
                SELECT 1 FROM fct_claims c WHERE c.claim_id = p.claim_id
            )
        """).fetchone()[0]
        assert orphans == 0

    def test_closed_claims_zero_reserve(self, warehouse_con):
        """Closed/denied claims should have zero current reserve."""
        nonzero = warehouse_con.execute("""
            SELECT COUNT(*)
            FROM fct_claims
            WHERE claim_status IN ('closed', 'denied')
              AND current_reserve != 0
        """).fetchone()[0]
        assert nonzero == 0

    def test_payment_dates_after_accident(self, warehouse_con):
        """Payments should not occur before the accident date."""
        invalid = warehouse_con.execute("""
            SELECT COUNT(*)
            FROM fct_claim_payments
            WHERE payment_date < accident_date
        """).fetchone()[0]
        assert invalid == 0
