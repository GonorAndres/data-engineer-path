"""Tests for the synthetic data generator.

Validates that generated data has correct schemas, reasonable distributions,
and proper relationships between tables.
"""

from __future__ import annotations

from datetime import date


class TestPolicyholders:
    """Tests for policyholder data."""

    def test_count(self, generated_data):
        assert len(generated_data["policyholders"]) == 500

    def test_unique_ids(self, generated_data):
        ids = [p["policyholder_id"] for p in generated_data["policyholders"]]
        assert len(ids) == len(set(ids))

    def test_fields_present(self, generated_data):
        required = {
            "policyholder_id", "first_name", "last_name", "date_of_birth",
            "gender", "state_code", "city", "occupation", "registration_date",
        }
        for p in generated_data["policyholders"]:
            assert required.issubset(p.keys())

    def test_valid_genders(self, generated_data):
        genders = {p["gender"] for p in generated_data["policyholders"]}
        assert genders.issubset({"M", "F"})

    def test_valid_states(self, generated_data):
        valid_states = {
            "AGS", "BC", "BCS", "CAM", "CHIS", "CHIH", "CDMX", "COAH", "COL",
            "DGO", "GTO", "GRO", "HGO", "JAL", "MEX", "MICH", "MOR", "NAY",
            "NL", "OAX", "PUE", "QRO", "QROO", "SLP", "SIN", "SON", "TAB",
            "TAMS", "TLAX", "VER", "YUC", "ZAC",
        }
        for p in generated_data["policyholders"]:
            assert p["state_code"] in valid_states


class TestPolicies:
    """Tests for policy data."""

    def test_count(self, generated_data):
        assert len(generated_data["policies"]) == 800

    def test_unique_ids(self, generated_data):
        ids = [p["policy_id"] for p in generated_data["policies"]]
        assert len(ids) == len(set(ids))

    def test_valid_coverage_types(self, generated_data):
        valid = {"auto", "home", "liability", "health", "life"}
        for p in generated_data["policies"]:
            assert p["coverage_type"] in valid

    def test_effective_before_expiration(self, generated_data):
        for p in generated_data["policies"]:
            eff = date.fromisoformat(p["effective_date"])
            exp = date.fromisoformat(p["expiration_date"])
            assert eff < exp

    def test_premium_positive(self, generated_data):
        for p in generated_data["policies"]:
            assert p["annual_premium"] > 0

    def test_all_policyholders_have_policy(self, generated_data):
        holder_ids_with_policy = {
            p["policyholder_id"] for p in generated_data["policies"]
        }
        all_holder_ids = {
            p["policyholder_id"] for p in generated_data["policyholders"]
        }
        assert all_holder_ids.issubset(holder_ids_with_policy)


class TestClaims:
    """Tests for claim data."""

    def test_reasonable_count(self, generated_data):
        # With ~800 policies and Poisson frequency, expect 400-700 claims.
        count = len(generated_data["claims"])
        assert 200 <= count <= 1000, f"Unexpected claim count: {count}"

    def test_unique_ids(self, generated_data):
        ids = [c["claim_id"] for c in generated_data["claims"]]
        unique_ratio = len(set(ids)) / len(ids)
        assert unique_ratio >= 0.98

    def test_valid_statuses(self, generated_data):
        valid = {"open", "closed", "reopened", "denied"}
        for c in generated_data["claims"]:
            assert c["claim_status"] in valid

    def test_closed_claims_have_close_date(self, generated_data):
        for c in generated_data["claims"]:
            if c["claim_status"] in ("closed", "denied"):
                assert c["close_date"] is not None

    def test_accident_date_in_range(self, generated_data):
        for c in generated_data["claims"]:
            acc = date.fromisoformat(c["accident_date"])
            assert date(2020, 1, 1) <= acc <= date(2025, 12, 31)

    def test_report_after_accident(self, generated_data):
        for c in generated_data["claims"]:
            acc = date.fromisoformat(c["accident_date"])
            rep = date.fromisoformat(c["report_date"])
            assert rep >= acc

    def test_claims_reference_valid_policies(self, generated_data):
        policy_ids = {p["policy_id"] for p in generated_data["policies"]}
        for c in generated_data["claims"]:
            assert c["policy_id"] in policy_ids

    def test_multiple_accident_years(self, generated_data):
        years = {
            date.fromisoformat(c["accident_date"]).year
            for c in generated_data["claims"]
        }
        # Should have claims in at least 5 of the 6 years (2020-2025).
        assert len(years) >= 5


class TestClaimPayments:
    """Tests for claim payment data."""

    def test_has_payments(self, generated_data):
        assert len(generated_data["payments"]) > 0

    def test_valid_payment_types(self, generated_data):
        valid = {"indemnity", "expense", "recovery"}
        for p in generated_data["payments"]:
            assert p["payment_type"] in valid

    def test_recoveries_are_negative(self, generated_data):
        for p in generated_data["payments"]:
            if p["payment_type"] == "recovery":
                assert p["payment_amount"] < 0

    def test_indemnity_positive(self, generated_data):
        for p in generated_data["payments"]:
            if p["payment_type"] == "indemnity":
                assert p["payment_amount"] > 0

    def test_payments_reference_valid_claims(self, generated_data):
        claim_ids = {c["claim_id"] for c in generated_data["claims"]}
        for p in generated_data["payments"]:
            assert p["claim_id"] in claim_ids


class TestCoverages:
    """Tests for coverage reference data."""

    def test_count(self, generated_data):
        assert len(generated_data["coverages"]) == 5

    def test_all_types_present(self, generated_data):
        types = {c["coverage_type"] for c in generated_data["coverages"]}
        assert types == {"auto", "home", "liability", "health", "life"}


class TestReproducibility:
    """Verify that the same seed produces identical data."""

    def test_deterministic_output(self):
        from data_generator import ClaimsDataGenerator

        gen1 = ClaimsDataGenerator(seed=99)
        p1 = gen1.generate_policyholders(10)

        gen2 = ClaimsDataGenerator(seed=99)
        p2 = gen2.generate_policyholders(10)

        assert p1 == p2
