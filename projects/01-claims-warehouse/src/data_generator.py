"""Synthetic insurance data generator for the Claims Warehouse project.

Generates five interrelated CSV files that model a Mexican insurance company's
claims operations from 2020 through 2025.  The data is shaped for loss-triangle
analysis: older accident years are more fully developed, recent ones are not,
and an IBNR (incurred-but-not-reported) effect means the most recent periods
have fewer reported claims than the true frequency would suggest.

Actuarial distributions
-----------------------
* **Frequency** -- Poisson(lambda) per policy-year, lambda varies by coverage.
* **Severity** -- LogNormal(mu, sigma) in MXN, parameters vary by coverage.
* **Reporting delay** -- Exponential with mean depending on coverage category.
* **Development** -- Cumulative paid-to-ultimate percentages by dev year.

Usage::

    python src/data_generator.py                 # defaults
    python src/data_generator.py --output data/sample_data --seed 42
"""

from __future__ import annotations

import argparse
import csv
import math
import os
from datetime import date, timedelta
from typing import Any

import numpy as np
from faker import Faker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MEXICAN_STATES: list[str] = [
    "AGS",
    "BC",
    "BCS",
    "CAM",
    "CHIS",
    "CHIH",
    "CDMX",
    "COAH",
    "COL",
    "DGO",
    "GTO",
    "GRO",
    "HGO",
    "JAL",
    "MEX",
    "MICH",
    "MOR",
    "NAY",
    "NL",
    "OAX",
    "PUE",
    "QRO",
    "QROO",
    "SLP",
    "SIN",
    "SON",
    "TAB",
    "TAMS",
    "TLAX",
    "VER",
    "YUC",
    "ZAC",
]

# Higher-population states get more weight so the synthetic data resembles
# real Mexican insurance market concentration.
_HIGH_POP_STATES = {"CDMX", "MEX", "JAL", "NL", "GTO", "PUE", "VER", "CHIH"}

COVERAGE_TYPES: list[str] = ["auto", "home", "liability", "health", "life"]

VALUATION_DATE: date = date(2025, 12, 31)
START_DATE: date = date(2020, 1, 1)

# -- Claim frequency (annual Poisson lambda per policy) --------------------
FREQUENCY_LAMBDA: dict[str, float] = {
    "auto": 0.12,
    "home": 0.05,
    "liability": 0.03,
    "health": 0.20,
    "life": 0.005,
}

# -- Claim severity (LogNormal parameters, amounts in MXN) -----------------
SEVERITY_PARAMS: dict[str, tuple[float, float]] = {
    "auto": (10.5, 0.8),
    "home": (11.0, 1.0),
    "liability": (11.5, 1.2),
    "health": (10.0, 1.0),
    "life": (13.0, 0.5),
}

# -- Reporting delay (Exponential mean in days) -----------------------------
REPORTING_DELAY_MEAN: dict[str, float] = {
    "auto": 15.0,
    "home": 30.0,
    "liability": 45.0,
    "health": 15.0,
    "life": 30.0,
}

# -- Annual premium ranges (MXN) -------------------------------------------
PREMIUM_RANGE: dict[str, tuple[float, float]] = {
    "auto": (8_000.0, 25_000.0),
    "home": (5_000.0, 20_000.0),
    "liability": (3_000.0, 15_000.0),
    "health": (10_000.0, 40_000.0),
    "life": (15_000.0, 50_000.0),
}

# -- Deductible ranges (MXN) -----------------------------------------------
DEDUCTIBLE_RANGE: dict[str, tuple[float, float]] = {
    "auto": (3_000.0, 10_000.0),
    "home": (5_000.0, 20_000.0),
    "liability": (10_000.0, 50_000.0),
    "health": (2_000.0, 8_000.0),
    "life": (0.0, 0.0),
}

# -- Coverage limits (MXN) -------------------------------------------------
COVERAGE_LIMIT_RANGE: dict[str, tuple[float, float]] = {
    "auto": (200_000.0, 1_000_000.0),
    "home": (500_000.0, 5_000_000.0),
    "liability": (1_000_000.0, 10_000_000.0),
    "health": (500_000.0, 3_000_000.0),
    "life": (1_000_000.0, 5_000_000.0),
}

# -- Cumulative development factors (% of ultimate by dev year) -------------
# Index 0 = development year 0 (accident year), index n = dev year n.
DEVELOPMENT_PATTERNS: dict[str, list[float]] = {
    "auto": [0.40, 0.70, 0.85, 0.95, 1.00],
    "home": [0.30, 0.60, 0.80, 0.90, 0.95, 1.00],
    "liability": [0.20, 0.40, 0.60, 0.75, 0.85, 0.95, 1.00],
    "health": [0.50, 0.80, 0.95, 1.00],
    "life": [0.90, 1.00],
}

# -- Causes of loss by coverage type ----------------------------------------
CAUSE_OF_LOSS: dict[str, list[str]] = {
    "auto": [
        "collision",
        "theft",
        "vandalism",
        "hail_damage",
        "hit_and_run",
        "rollover",
    ],
    "home": [
        "fire",
        "water_damage",
        "theft",
        "earthquake",
        "hurricane",
        "structural_damage",
    ],
    "liability": [
        "bodily_injury",
        "property_damage",
        "professional_negligence",
        "product_liability",
    ],
    "health": [
        "hospitalization",
        "surgery",
        "emergency_room",
        "chronic_treatment",
        "maternity",
    ],
    "life": ["death_natural", "death_accidental"],
}

# -- Occupations (Mexican context) ------------------------------------------
OCCUPATIONS: list[str] = [
    "ingeniero",
    "medico",
    "abogado",
    "contador",
    "profesor",
    "comerciante",
    "servidor_publico",
    "obrero",
    "chofer",
    "arquitecto",
    "enfermero",
    "agricultor",
    "empresario",
    "programador",
    "actuario",
    "farmaceutico",
    "veterinario",
    "electricista",
    "mecanico",
    "estudiante",
]

# -- Coverage-type weights (market share in portfolio) ----------------------
# Auto and health dominate; life is a small slice.
COVERAGE_WEIGHTS: dict[str, float] = {
    "auto": 0.35,
    "home": 0.15,
    "liability": 0.08,
    "health": 0.32,
    "life": 0.10,
}


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


class ClaimsDataGenerator:
    """Generates synthetic insurance data with actuarial-realistic distributions.

    All randomness flows from a single ``numpy.random.Generator`` seeded at
    init time, ensuring full reproducibility.  A ``Faker`` instance seeded with
    the same value provides names, cities, and dates.

    The generator models a book of business where many policies have been
    continuously renewed across the 2020-2025 observation window.  Each policy
    row represents one annual term, so a policyholder who bought auto coverage
    in 2020 and renewed every year appears as multiple policy rows (one per
    term).  This approach naturally yields realistic exposure-years and claim
    volumes that align with the specified Poisson frequencies.

    Attributes:
        rng: NumPy random generator.
        fake: Faker instance with es_MX locale.
    """

    def __init__(self, seed: int = 42) -> None:
        self.rng: np.random.Generator = np.random.default_rng(seed)
        self.fake: Faker = Faker("es_MX")
        Faker.seed(seed)

    # -- helpers ------------------------------------------------------------

    def _random_date_between(self, start: date, end: date) -> date:
        """Return a uniformly random date in [start, end]."""
        delta_days = (end - start).days
        if delta_days <= 0:
            return start
        offset = int(self.rng.integers(0, delta_days + 1))
        return start + timedelta(days=offset)

    def _weighted_state(self) -> str:
        """Pick a Mexican state with higher weight for populous states."""
        weights = np.array([3.0 if s in _HIGH_POP_STATES else 1.0 for s in MEXICAN_STATES])
        weights /= weights.sum()
        idx = self.rng.choice(len(MEXICAN_STATES), p=weights)
        return MEXICAN_STATES[idx]

    @staticmethod
    def _round_mxn(value: float) -> float:
        """Round to two decimal places (MXN centavos)."""
        return round(value, 2)

    # -- 1. Policyholders ---------------------------------------------------

    def generate_policyholders(self, n: int = 500) -> list[dict[str, Any]]:
        """Generate *n* policyholder records.

        Names and cities come from Faker (es_MX).  Date of birth is
        uniformly distributed so that policyholders are 21--70 years old
        relative to :data:`START_DATE`.

        Args:
            n: Number of policyholders to generate.

        Returns:
            List of dicts, each representing one row of ``policyholders.csv``.
        """
        policyholders: list[dict[str, Any]] = []
        for i in range(1, n + 1):
            gender = self.rng.choice(["M", "F"])
            dob = self._random_date_between(
                START_DATE - timedelta(days=70 * 365),
                START_DATE - timedelta(days=21 * 365),
            )
            registration_date = self._random_date_between(
                START_DATE - timedelta(days=365),
                date(2024, 12, 31),
            )
            policyholders.append(
                {
                    "policyholder_id": i,
                    "first_name": (
                        self.fake.first_name_male()
                        if gender == "M"
                        else self.fake.first_name_female()
                    ),
                    "last_name": self.fake.last_name(),
                    "date_of_birth": dob.isoformat(),
                    "gender": gender,
                    "state_code": self._weighted_state(),
                    "city": self.fake.city(),
                    "occupation": self.rng.choice(OCCUPATIONS),
                    "registration_date": registration_date.isoformat(),
                }
            )
        return policyholders

    # -- 2. Policies --------------------------------------------------------

    def generate_policies(
        self,
        policyholders: list[dict[str, Any]],
        n: int = 800,
    ) -> list[dict[str, Any]]:
        """Generate *n* policies linked to existing policyholders.

        Policies are generated so that the book of business spans the full
        2020-2025 observation window.  Effective dates are biased toward
        earlier years (2020-2022) to ensure that many policies accumulate
        multiple exposure-years, producing a realistic claim volume when
        combined with the per-year Poisson frequencies.

        Each policyholder is guaranteed at least one policy.  Remaining
        policies are distributed randomly so some people hold multiple
        coverages.

        Args:
            policyholders: Output of :meth:`generate_policyholders`.
            n: Target number of policies (must be >= len(policyholders)).

        Returns:
            List of dicts, each representing one row of ``policies.csv``.
        """
        if n < len(policyholders):
            raise ValueError("n must be >= number of policyholders")

        # Assign one policy to each policyholder, then distribute the rest.
        holder_ids = [p["policyholder_id"] for p in policyholders]
        assignments: list[int] = list(holder_ids)
        extra = n - len(holder_ids)
        if extra > 0:
            assignments.extend(self.rng.choice(holder_ids, size=extra).tolist())
        self.rng.shuffle(assignments)  # type: ignore[arg-type]

        # Coverage types weighted by market share.
        cov_names = list(COVERAGE_WEIGHTS.keys())
        cov_probs = np.array([COVERAGE_WEIGHTS[c] for c in cov_names])
        cov_probs /= cov_probs.sum()

        policies: list[dict[str, Any]] = []
        for i, holder_id in enumerate(assignments, start=1):
            coverage = str(self.rng.choice(cov_names, p=cov_probs))

            # Bias effective dates toward earlier years so the portfolio
            # accumulates enough exposure for ~600 claims.  About 60% of
            # policies start in 2020-2021, 25% in 2022-2023, 15% in 2024-2025.
            year_roll = float(self.rng.random())
            if year_roll < 0.35:
                eff_date = self._random_date_between(date(2020, 1, 1), date(2020, 12, 31))
            elif year_roll < 0.60:
                eff_date = self._random_date_between(date(2021, 1, 1), date(2021, 12, 31))
            elif year_roll < 0.75:
                eff_date = self._random_date_between(date(2022, 1, 1), date(2022, 12, 31))
            elif year_roll < 0.87:
                eff_date = self._random_date_between(date(2023, 1, 1), date(2023, 12, 31))
            elif year_roll < 0.95:
                eff_date = self._random_date_between(date(2024, 1, 1), date(2024, 12, 31))
            else:
                eff_date = self._random_date_between(date(2025, 1, 1), date(2025, 6, 30))

            exp_date = eff_date + timedelta(days=365)

            prem_lo, prem_hi = PREMIUM_RANGE[coverage]
            ded_lo, ded_hi = DEDUCTIBLE_RANGE[coverage]
            lim_lo, lim_hi = COVERAGE_LIMIT_RANGE[coverage]

            # Determine status based on expiration relative to valuation date.
            if exp_date <= VALUATION_DATE:
                status = self.rng.choice(
                    ["expired", "renewed", "cancelled"],
                    p=[0.30, 0.60, 0.10],
                )
            else:
                status = str(self.rng.choice(["active", "cancelled"], p=[0.92, 0.08]))

            policies.append(
                {
                    "policy_id": i,
                    "policyholder_id": int(holder_id),
                    "policy_number": f"POL-{eff_date.year}-{i:05d}",
                    "coverage_type": coverage,
                    "effective_date": eff_date.isoformat(),
                    "expiration_date": exp_date.isoformat(),
                    "annual_premium": self._round_mxn(float(self.rng.uniform(prem_lo, prem_hi))),
                    "deductible": self._round_mxn(float(self.rng.uniform(ded_lo, ded_hi))),
                    "coverage_limit": self._round_mxn(float(self.rng.uniform(lim_lo, lim_hi))),
                    "status": str(status),
                }
            )
        return policies

    # -- 3. Claims ----------------------------------------------------------

    def generate_claims(
        self,
        policies: list[dict[str, Any]],
        target_claims: int = 600,
    ) -> list[dict[str, Any]]:
        """Generate claims using Poisson frequency per policy-year.

        For each policy the number of exposure years that overlap with
        [START_DATE, VALUATION_DATE] is computed, and claim counts are drawn
        from ``Poisson(lambda * exposure_years)``.

        A **portfolio scale factor** adjusts the effective lambda so the total
        claim count lands near *target_claims*.  This represents the fact that
        the 800 policy rows are a sample of a larger book of business; the
        scale factor is determined by a two-pass approach (first pass estimates
        expected count, second pass draws).

        An **IBNR effect** is applied: claims whose ``report_date`` would
        fall after :data:`VALUATION_DATE` are silently dropped, meaning
        recent accident years naturally have fewer recorded claims.

        Args:
            policies: Output of :meth:`generate_policies`.
            target_claims: Approximate number of claims to generate.

        Returns:
            List of dicts, each representing one row of ``claims.csv``.
        """
        # --- First pass: estimate expected claims to compute scale factor ---
        expected_total = 0.0
        for pol in policies:
            coverage = pol["coverage_type"]
            eff = date.fromisoformat(pol["effective_date"])
            exp = date.fromisoformat(pol["expiration_date"])
            obs_start = max(eff, START_DATE)
            obs_end = min(exp, VALUATION_DATE)
            if obs_start >= obs_end:
                continue
            exposure_years = (obs_end - obs_start).days / 365.25
            expected_total += FREQUENCY_LAMBDA[coverage] * exposure_years

        # Scale so E[claims] ~ target_claims (accounting for ~5% IBNR loss).
        if expected_total > 0:
            scale = (target_claims / expected_total) * 1.05
        else:
            scale = 1.0

        # --- Second pass: generate claims -----------------------------------
        claims: list[dict[str, Any]] = []
        claim_seq = 0

        for pol in policies:
            coverage = pol["coverage_type"]
            eff = date.fromisoformat(pol["effective_date"])
            exp = date.fromisoformat(pol["expiration_date"])

            # Clip to observation window.
            obs_start = max(eff, START_DATE)
            obs_end = min(exp, VALUATION_DATE)
            if obs_start >= obs_end:
                continue

            exposure_years = (obs_end - obs_start).days / 365.25
            lam = FREQUENCY_LAMBDA[coverage] * exposure_years * scale
            n_claims = int(self.rng.poisson(lam))

            for _ in range(n_claims):
                accident_date = self._random_date_between(obs_start, obs_end)

                # Reporting delay (Exponential).
                delay_days = max(
                    1,
                    int(self.rng.exponential(REPORTING_DELAY_MEAN[coverage])),
                )
                report_date = accident_date + timedelta(days=delay_days)

                # IBNR: drop claims not yet reported by valuation date.
                if report_date > VALUATION_DATE:
                    continue

                # Severity (LogNormal).
                mu, sigma = SEVERITY_PARAMS[coverage]
                ultimate = float(self.rng.lognormal(mu, sigma))
                ultimate = self._round_mxn(ultimate)

                claim_seq += 1
                claim_id = claim_seq
                claim_number = f"CLM-{accident_date.year}-{claim_id:05d}"

                # Initial reserve is an estimate -- add noise around ultimate.
                reserve_noise = float(self.rng.normal(1.0, 0.15))
                initial_reserve = self._round_mxn(ultimate * max(reserve_noise, 0.3))

                # Determine claim status.  Recent claims are more likely open.
                accident_age_days = (VALUATION_DATE - accident_date).days
                if accident_age_days < 180:
                    status_probs = [0.60, 0.30, 0.05, 0.05]
                elif accident_age_days < 730:
                    status_probs = [0.30, 0.53, 0.05, 0.12]
                else:
                    status_probs = [0.10, 0.75, 0.05, 0.10]
                claim_status = str(
                    self.rng.choice(
                        ["open", "closed", "reopened", "denied"],
                        p=status_probs,
                    )
                )

                # Close date -- only for closed / denied claims.
                close_date: str | None = None
                if claim_status in ("closed", "denied"):
                    # Close date between report and valuation.
                    close_lag = max(
                        30,
                        int(self.rng.exponential(180)),
                    )
                    proposed_close = report_date + timedelta(days=close_lag)
                    if proposed_close > VALUATION_DATE:
                        # Can't close after valuation -- revert to open.
                        claim_status = "open"
                    else:
                        close_date = proposed_close.isoformat()

                # current_reserve: 0 for closed/denied, positive otherwise.
                if claim_status in ("closed", "denied"):
                    current_reserve = 0.0
                else:
                    # Remaining reserve = initial_reserve minus what has
                    # conceptually been paid so far (estimated simply here;
                    # refined when payments are generated).
                    current_reserve = initial_reserve

                claims.append(
                    {
                        "claim_id": claim_id,
                        "policy_id": pol["policy_id"],
                        "claim_number": claim_number,
                        "accident_date": accident_date.isoformat(),
                        "report_date": report_date.isoformat(),
                        "close_date": close_date,
                        "claim_status": claim_status,
                        "cause_of_loss": str(self.rng.choice(CAUSE_OF_LOSS[coverage])),
                        "initial_reserve": initial_reserve,
                        "current_reserve": self._round_mxn(current_reserve),
                        # Stash for payment generation (not written to CSV).
                        "_ultimate": ultimate,
                        "_coverage_type": coverage,
                    }
                )

        return claims

    # -- 4. Claim Payments --------------------------------------------------

    def generate_claim_payments(
        self,
        claims: list[dict[str, Any]],
        policies: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Generate payment transactions following loss-development patterns.

        For each claim the ultimate amount is split across development years
        according to :data:`DEVELOPMENT_PATTERNS`.  Payments are only
        generated up to the number of development years that have elapsed
        between the accident date and :data:`VALUATION_DATE`, so recent
        claims show less development.

        About 8 % of indemnity payments are followed by a small negative
        ``recovery`` payment.  One ``expense`` payment is added per
        development year.

        The ``cumulative_paid`` column is a running total **per claim**.

        After this method runs it also back-patches ``current_reserve`` on
        each claim dict: for open/reopened claims the reserve equals the
        estimated outstanding (ultimate minus paid); for closed/denied
        claims the reserve is forced to 0.

        Args:
            claims: Output of :meth:`generate_claims`.
            policies: Output of :meth:`generate_policies` (unused but kept
                for interface symmetry and possible future enrichment).

        Returns:
            List of dicts, each representing one row of ``claim_payments.csv``.
        """
        payments: list[dict[str, Any]] = []
        payment_seq = 0

        for claim in claims:
            coverage = claim["_coverage_type"]
            pattern = DEVELOPMENT_PATTERNS[coverage]
            ultimate = claim["_ultimate"]
            accident_date = date.fromisoformat(claim["accident_date"])
            report_date = date.fromisoformat(claim["report_date"])

            # How many full development years have elapsed?
            elapsed_years = (VALUATION_DATE - accident_date).days / 365.25
            max_dev = min(int(math.floor(elapsed_years)), len(pattern) - 1)
            if max_dev < 0:
                continue

            cumulative_paid = 0.0
            prev_cum_pct = 0.0
            last_payment_date: date | None = None

            for dev_year in range(max_dev + 1):
                target_cum_pct = pattern[dev_year]
                incremental_pct = target_cum_pct - prev_cum_pct
                if incremental_pct <= 0:
                    prev_cum_pct = target_cum_pct
                    continue

                # Add jitter to incremental percentage (+/- 10 %).
                jitter = float(self.rng.normal(1.0, 0.05))
                incremental_amount = self._round_mxn(ultimate * incremental_pct * max(jitter, 0.5))

                # Payment date: within the development year, after report.
                dev_year_start = accident_date + timedelta(days=int(dev_year * 365.25))
                dev_year_end = accident_date + timedelta(days=int((dev_year + 1) * 365.25) - 1)
                pay_earliest = max(dev_year_start, report_date)
                pay_latest = min(dev_year_end, VALUATION_DATE)
                if pay_earliest > pay_latest:
                    prev_cum_pct = target_cum_pct
                    continue

                # One indemnity payment per development year.
                payment_date = self._random_date_between(pay_earliest, pay_latest)
                cumulative_paid = self._round_mxn(cumulative_paid + incremental_amount)
                payment_seq += 1
                payments.append(
                    {
                        "payment_id": payment_seq,
                        "claim_id": claim["claim_id"],
                        "payment_date": payment_date.isoformat(),
                        "payment_amount": self._round_mxn(incremental_amount),
                        "payment_type": "indemnity",
                        "cumulative_paid": cumulative_paid,
                    }
                )
                last_payment_date = payment_date

                # Expense payment (adjuster / legal fees): ~5-15 % of
                # the incremental indemnity, ~15 % of dev years.
                if self.rng.random() < 0.15:
                    expense_pct = float(self.rng.uniform(0.05, 0.15))
                    expense_amount = self._round_mxn(incremental_amount * expense_pct)
                    if expense_amount > 0:
                        expense_date = self._random_date_between(pay_earliest, pay_latest)
                        cumulative_paid = self._round_mxn(cumulative_paid + expense_amount)
                        payment_seq += 1
                        payments.append(
                            {
                                "payment_id": payment_seq,
                                "claim_id": claim["claim_id"],
                                "payment_date": expense_date.isoformat(),
                                "payment_amount": expense_amount,
                                "payment_type": "expense",
                                "cumulative_paid": cumulative_paid,
                            }
                        )
                        last_payment_date = max(last_payment_date or expense_date, expense_date)

                # Recovery (~3 % chance per dev year).
                if self.rng.random() < 0.03:
                    recovery_pct = float(self.rng.uniform(0.05, 0.20))
                    recovery_amount = self._round_mxn(-(incremental_amount * recovery_pct))
                    recovery_date = self._random_date_between(pay_earliest, pay_latest)
                    cumulative_paid = self._round_mxn(cumulative_paid + recovery_amount)
                    payment_seq += 1
                    payments.append(
                        {
                            "payment_id": payment_seq,
                            "claim_id": claim["claim_id"],
                            "payment_date": recovery_date.isoformat(),
                            "payment_amount": recovery_amount,
                            "payment_type": "recovery",
                            "cumulative_paid": cumulative_paid,
                        }
                    )
                    last_payment_date = max(last_payment_date or recovery_date, recovery_date)

                prev_cum_pct = target_cum_pct

            # -- Back-patch claim fields ------------------------------------
            # Ensure close_date is after last payment for closed claims.
            if claim["claim_status"] in ("closed", "denied"):
                claim["current_reserve"] = 0.0
                if last_payment_date and claim["close_date"]:
                    existing_close = date.fromisoformat(claim["close_date"])
                    if existing_close < last_payment_date:
                        claim["close_date"] = (
                            last_payment_date + timedelta(days=int(self.rng.integers(1, 30)))
                        ).isoformat()
                        # Clamp to valuation date.
                        if date.fromisoformat(claim["close_date"]) > VALUATION_DATE:
                            claim["close_date"] = VALUATION_DATE.isoformat()
            else:
                # Open / reopened: outstanding = ultimate - paid.
                # Ensure a positive reserve for open claims -- if development
                # has paid beyond the initial ultimate estimate, set a minimum
                # reserve equal to 5 % of the ultimate (re-estimation).
                outstanding = ultimate - cumulative_paid
                if outstanding <= 0:
                    outstanding = ultimate * 0.05
                claim["current_reserve"] = self._round_mxn(outstanding)

        return payments

    # -- 5. Coverages (static reference) ------------------------------------

    def generate_coverages(self) -> list[dict[str, Any]]:
        """Return static reference data for coverage types.

        Returns:
            List of dicts, each representing one row of ``coverages.csv``.
        """
        return [
            {
                "coverage_type": "auto",
                "coverage_category": "property_casualty",
                "description": ("Seguro de automovil: colision, robo, responsabilidad civil"),
            },
            {
                "coverage_type": "home",
                "coverage_category": "property_casualty",
                "description": ("Seguro de hogar: incendio, robo, desastres naturales"),
            },
            {
                "coverage_type": "liability",
                "coverage_category": "casualty",
                "description": (
                    "Seguro de responsabilidad civil: danos a terceros, negligencia profesional"
                ),
            },
            {
                "coverage_type": "health",
                "coverage_category": "health",
                "description": ("Seguro de gastos medicos: hospitalizacion, cirugia, emergencias"),
            },
            {
                "coverage_type": "life",
                "coverage_category": "life",
                "description": (
                    "Seguro de vida: muerte natural o accidental, beneficiarios designados"
                ),
            },
        ]

    # -- Orchestrator -------------------------------------------------------

    def generate_all(
        self,
        output_dir: str,
        *,
        n_policyholders: int = 500,
        n_policies: int | None = None,
        output_format: str = "csv",
    ) -> dict[str, int]:
        """Generate every table and write the results to files.

        Args:
            output_dir: Directory where the five data files will be created.
                Created automatically if it does not exist.
            n_policyholders: Number of policyholders to generate (default 500).
            n_policies: Number of policies.  If *None*, auto-computed as
                ``int(n_policyholders * 1.6)``.
            output_format: ``"csv"`` (default) or ``"parquet"``.

        Returns:
            Dict mapping file name to the number of rows written.
        """
        os.makedirs(output_dir, exist_ok=True)

        if n_policies is None:
            n_policies = int(n_policyholders * 1.6)
        target_claims = max(int(n_policyholders * 1.2), 100)

        policyholders = self.generate_policyholders(n=n_policyholders)
        policies = self.generate_policies(policyholders, n=n_policies)
        claims = self.generate_claims(policies, target_claims=target_claims)
        payments = self.generate_claim_payments(claims, policies)
        coverages = self.generate_coverages()

        # Strip internal keys (prefixed with "_") before writing.
        claims_clean = [{k: v for k, v in c.items() if not k.startswith("_")} for c in claims]

        ext = "parquet" if output_format == "parquet" else "csv"
        writer = self._write_parquet if output_format == "parquet" else self._write_csv

        tables: dict[str, list[dict[str, Any]]] = {
            f"policyholders.{ext}": policyholders,
            f"policies.{ext}": policies,
            f"claims.{ext}": claims_clean,
            f"claim_payments.{ext}": payments,
            f"coverages.{ext}": coverages,
        }

        row_counts: dict[str, int] = {}
        for filename, rows in tables.items():
            filepath = os.path.join(output_dir, filename)
            writer(filepath, rows)
            row_counts[filename] = len(rows)

        return row_counts

    @staticmethod
    def _write_csv(filepath: str, rows: list[dict[str, Any]]) -> None:
        """Write a list of dicts to a CSV file."""
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _write_parquet(filepath: str, rows: list[dict[str, Any]]) -> None:
        """Write rows to a Parquet file using PyArrow."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not rows:
            return
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, filepath)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for data generation."""
    parser = argparse.ArgumentParser(
        description=("Generate synthetic insurance claims data for the Claims Warehouse project."),
    )
    parser.add_argument(
        "--output",
        default=os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data",
            "sample_data",
        ),
        help="Output directory for CSV files (default: data/sample_data/)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--policyholders",
        type=int,
        default=500,
        help="Number of policyholders to generate (default: 500)",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for generated data (default: csv)",
    )
    args = parser.parse_args()

    generator = ClaimsDataGenerator(seed=args.seed)
    print(f"Generating data with seed={args.seed} ...")
    row_counts = generator.generate_all(
        args.output,
        n_policyholders=args.policyholders,
        output_format=args.output_format,
    )

    print(f"\nFiles written to: {os.path.abspath(args.output)}")
    print("-" * 45)
    for filename, count in row_counts.items():
        print(f"  {filename:<25s} {count:>6,d} rows")
    print("-" * 45)
    print("Done.")


if __name__ == "__main__":
    main()
