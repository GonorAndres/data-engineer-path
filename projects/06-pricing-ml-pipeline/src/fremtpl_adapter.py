"""Adapter to load freMTPL2 data into P01-compatible DuckDB tables.

Maps the French motor insurance dataset columns to match the P01 warehouse
schema so P06's feature engineering SQL runs unchanged.

freMTPL2 is a standard French motor third-party liability dataset with ~680K
policies. The frequency table has one row per policy; the severity table has
one row per claim (multiple claims possible per policy).

Column mapping strategy:
  - IDpol -> policy_id, policyholder_id (1:1 since one driver per policy)
  - BonusMalus -> derives annual_premium (higher BM = higher premium)
  - VehPower -> derives deductible and coverage_limit
  - DrivAge -> current_age, date_of_birth
  - Area -> state_code (mapped to Mexican state codes for consistency)
  - Region -> city
  - Exposure -> exposure_years, policy term
  - ClaimNb -> claim count (frequency table)
  - ClaimAmount -> individual claim amounts (severity table)
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

import duckdb

# Hugging Face direct download URLs for freMTPL2 data
# Original CASdatasets GitHub repo distributes as R package only;
# these CSVs are hosted on HuggingFace by mabilton/fremtpl2.
FREQ_URL = (
    "https://huggingface.co/datasets/mabilton/fremtpl2/resolve/main/freMTPL2freq.csv"
)
SEV_URL = (
    "https://huggingface.co/datasets/mabilton/fremtpl2/resolve/main/freMTPL2sev.csv"
)


def download_fremtpl2(dest_dir: Path) -> tuple[Path, Path]:
    """Download freMTPL2 frequency and severity CSVs if not present locally.

    Args:
        dest_dir: Directory to save the CSV files.

    Returns:
        Tuple of (freq_path, sev_path).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    freq_path = dest_dir / "freMTPL2freq.csv"
    sev_path = dest_dir / "freMTPL2sev.csv"

    for url, path in [(FREQ_URL, freq_path), (SEV_URL, sev_path)]:
        if path.exists():
            print(f"  Found cached {path.name} ({path.stat().st_size:,d} bytes)")
        else:
            print(f"  Downloading {path.name} from GitHub...")
            urllib.request.urlretrieve(url, path)
            print(f"  Saved {path.name} ({path.stat().st_size:,d} bytes)")

    return freq_path, sev_path


def load_fremtpl2(
    con: duckdb.DuckDBPyConnection,
    freq_path: Path,
    sev_path: Path,
) -> dict[str, int]:
    """Load freMTPL2 data into DuckDB tables matching P01's schema.

    Creates: dim_policy, dim_policyholder, fct_claims, int_policy_exposure,
             rpt_claim_frequency, dim_coverage.

    The key insight is that P06's feature SQL reads these exact table names
    with these exact columns. By creating them from freMTPL2 data, the rest
    of the pipeline runs unchanged.

    Args:
        con: DuckDB connection.
        freq_path: Path to freMTPL2freq.csv.
        sev_path: Path to freMTPL2sev.csv.

    Returns:
        Dict mapping table name to row count.
    """
    print("\n=== Loading freMTPL2 Data ===")

    # Step 1: Load raw CSVs into staging tables
    con.execute(f"""
        CREATE OR REPLACE TABLE fremtpl2_freq AS
        SELECT * FROM read_csv_auto('{freq_path}')
    """)
    freq_count = con.execute("SELECT COUNT(*) FROM fremtpl2_freq").fetchone()[0]
    print(f"  fremtpl2_freq (raw):   {freq_count:>10,d} rows")

    con.execute(f"""
        CREATE OR REPLACE TABLE fremtpl2_sev AS
        SELECT * FROM read_csv_auto('{sev_path}')
    """)
    sev_count = con.execute("SELECT COUNT(*) FROM fremtpl2_sev").fetchone()[0]
    print(f"  fremtpl2_sev (raw):    {sev_count:>10,d} rows")

    results: dict[str, int] = {}

    # Step 2: dim_policy -- one row per policy
    # Derive premium from BonusMalus (standard actuarial rating factor)
    # Derive deductible/limit from VehPower (proxy for vehicle value)
    con.execute("""
        CREATE OR REPLACE TABLE dim_policy AS
        SELECT
            CAST(IDpol AS INTEGER) AS policy_id,
            CAST(IDpol AS INTEGER) AS policyholder_id,
            'POL-' || LPAD(CAST(IDpol AS VARCHAR), 6, '0') AS policy_number,
            'auto' AS coverage_type,
            'property_casualty' AS coverage_category,
            'Motor third-party liability' AS coverage_description,
            DATE '2020-01-01' AS effective_date,
            DATE '2020-01-01' + INTERVAL (CAST(Exposure * 365 AS INTEGER)) DAY
                AS expiration_date,
            CAST(Exposure * 365 AS INTEGER) AS policy_term_days,
            CAST(500 + BonusMalus * 5 AS DECIMAL(14,2)) AS annual_premium,
            CAST(VehPower * 100 AS DECIMAL(14,2)) AS deductible,
            CAST(VehPower * 10000 AS DECIMAL(16,2)) AS coverage_limit,
            'active' AS status,
            -- Assign policy_year using hash-based split so we get train+test
            CASE
                WHEN hash(IDpol) % 5 = 0 THEN 2024
                ELSE 2020
            END AS policy_year,
            TRUE AS is_currently_active
        FROM fremtpl2_freq
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_policy").fetchone()[0]
    results["dim_policy"] = count
    print(f"  dim_policy:            {count:>10,d} rows")

    # Step 3: dim_policyholder -- one row per policy (1:1 mapping)
    # Map Area codes to Mexican state codes for consistency with P01 schema
    con.execute("""
        CREATE OR REPLACE TABLE dim_policyholder AS
        SELECT
            CAST(IDpol AS INTEGER) AS policyholder_id,
            'Policyholder' AS first_name,
            CAST(IDpol AS VARCHAR) AS last_name,
            'Policyholder ' || CAST(IDpol AS VARCHAR) AS full_name,
            DATE '2020-01-01' - INTERVAL (DrivAge) YEAR AS date_of_birth,
            DrivAge AS age_at_registration,
            DrivAge AS current_age,
            CASE WHEN IDpol % 2 = 0 THEN 'M' ELSE 'F' END AS gender,
            CASE Area
                WHEN 'A' THEN 'CDMX' WHEN 'B' THEN 'JAL' WHEN 'C' THEN 'NL'
                WHEN 'D' THEN 'GTO' WHEN 'E' THEN 'PUE' ELSE 'MEX'
            END AS state_code,
            Region AS city,
            'conductor' AS occupation,
            DATE '2020-01-01' AS registration_date
        FROM fremtpl2_freq
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_policyholder").fetchone()[0]
    results["dim_policyholder"] = count
    print(f"  dim_policyholder:      {count:>10,d} rows")

    # Step 4: fct_claims -- one row per claim from the severity table
    # Join with frequency to filter only policies that actually had claims
    con.execute("""
        CREATE OR REPLACE TABLE fct_claims AS
        WITH claim_amounts AS (
            SELECT
                IDpol,
                ClaimAmount,
                ROW_NUMBER() OVER (PARTITION BY IDpol ORDER BY ClaimAmount DESC)
                    AS claim_seq
            FROM fremtpl2_sev
        )
        SELECT
            ROW_NUMBER() OVER () AS claim_id,
            CAST(f.IDpol AS INTEGER) AS policy_id,
            CAST(f.IDpol AS INTEGER) AS policyholder_id,
            'CLM-' || LPAD(CAST(ROW_NUMBER() OVER () AS VARCHAR), 6, '0')
                AS claim_number,
            DATE '2020-06-15' AS accident_date,
            DATE '2020-06-20' AS report_date,
            'closed' AS status,
            CAST(s.ClaimAmount AS DECIMAL(16,2)) AS total_paid,
            CAST(s.ClaimAmount AS DECIMAL(16,2)) AS incurred_amount,
            0.00 AS total_reserved,
            'auto' AS coverage_type
        FROM fremtpl2_freq f
        INNER JOIN claim_amounts s ON f.IDpol = s.IDpol
        WHERE f.ClaimNb > 0
    """)
    count = con.execute("SELECT COUNT(*) FROM fct_claims").fetchone()[0]
    results["fct_claims"] = count
    print(f"  fct_claims:            {count:>10,d} rows")

    # Step 5: int_policy_exposure -- one row per policy with exposure metrics
    con.execute("""
        CREATE OR REPLACE TABLE int_policy_exposure AS
        SELECT
            CAST(IDpol AS INTEGER) AS policy_id,
            CAST(IDpol AS INTEGER) AS policyholder_id,
            'POL-' || LPAD(CAST(IDpol AS VARCHAR), 6, '0') AS policy_number,
            'auto' AS coverage_type,
            DATE '2020-01-01' AS effective_date,
            DATE '2020-01-01' + INTERVAL (CAST(Exposure * 365 AS INTEGER)) DAY
                AS expiration_date,
            CAST(500 + BonusMalus * 5 AS DECIMAL(14,2)) AS annual_premium,
            'active' AS status,
            DATE '2020-01-01' AS exposure_start,
            DATE '2020-01-01' + INTERVAL (CAST(Exposure * 365 AS INTEGER)) DAY
                AS exposure_end,
            CAST(Exposure * 365 AS INTEGER) AS exposure_days,
            Exposure AS exposure_years,
            CAST((500 + BonusMalus * 5) * Exposure AS DECIMAL(14,2))
                AS earned_premium,
            2020 AS exposure_year
        FROM fremtpl2_freq
    """)
    count = con.execute("SELECT COUNT(*) FROM int_policy_exposure").fetchone()[0]
    results["int_policy_exposure"] = count
    print(f"  int_policy_exposure:   {count:>10,d} rows")

    # Step 6: rpt_claim_frequency -- aggregated portfolio metrics
    # Used by feat_historical_benchmarks for benchmark features
    con.execute("""
        CREATE OR REPLACE TABLE rpt_claim_frequency AS
        SELECT
            2020 AS year,
            'auto' AS coverage_type,
            COUNT(*) AS policy_count,
            SUM(Exposure) AS exposure_years,
            SUM(CAST((500 + BonusMalus * 5) * Exposure AS DECIMAL))
                AS earned_premium,
            SUM(ClaimNb) AS claim_count,
            COALESCE(SUM(sev.total_claim), 0) AS total_paid,
            CASE
                WHEN SUM(ClaimNb) > 0
                THEN COALESCE(SUM(sev.total_claim), 0) / SUM(ClaimNb)
                ELSE 0
            END AS avg_severity,
            COALESCE(SUM(sev.total_claim), 0) AS total_incurred,
            SUM(ClaimNb) / SUM(Exposure) AS claim_frequency,
            COALESCE(SUM(sev.total_claim), 0) / SUM(Exposure) AS pure_premium,
            COALESCE(SUM(sev.total_claim), 0)
                / NULLIF(
                    SUM(CAST((500 + BonusMalus * 5) * Exposure AS DECIMAL)), 0
                ) AS loss_ratio
        FROM fremtpl2_freq f
        LEFT JOIN (
            SELECT IDpol, SUM(ClaimAmount) AS total_claim
            FROM fremtpl2_sev
            GROUP BY IDpol
        ) sev ON f.IDpol = sev.IDpol
    """)
    count = con.execute("SELECT COUNT(*) FROM rpt_claim_frequency").fetchone()[0]
    results["rpt_claim_frequency"] = count
    print(f"  rpt_claim_frequency:   {count:>10,d} rows")

    # Step 7: dim_coverage -- static reference table
    con.execute("""
        CREATE OR REPLACE TABLE dim_coverage AS
        SELECT
            1 AS coverage_key,
            'auto' AS coverage_type,
            'property_casualty' AS coverage_category,
            'Motor TPL' AS description
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_coverage").fetchone()[0]
    results["dim_coverage"] = count
    print(f"  dim_coverage:          {count:>10,d} rows")

    # Print summary statistics
    _print_dataset_summary(con)

    return results


def _print_dataset_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print key actuarial statistics from the loaded freMTPL2 data."""
    summary = con.execute("""
        SELECT
            COUNT(*) AS n_policies,
            ROUND(SUM(e.exposure_years), 1) AS total_exposure,
            COUNT(DISTINCT fc.policy_id) AS policies_with_claims,
            (SELECT COUNT(*) FROM fct_claims) AS total_claims,
            ROUND(AVG(dp.annual_premium), 2) AS avg_premium,
            ROUND(
                (SELECT SUM(total_paid) FROM fct_claims)
                / NULLIF(SUM(e.exposure_years), 0),
                2
            ) AS portfolio_pure_premium
        FROM dim_policy dp
        LEFT JOIN int_policy_exposure e ON dp.policy_id = e.policy_id
        LEFT JOIN (
            SELECT DISTINCT policy_id FROM fct_claims
        ) fc ON dp.policy_id = fc.policy_id
    """).fetchone()

    train_count = con.execute(
        "SELECT COUNT(*) FROM dim_policy WHERE policy_year <= 2023"
    ).fetchone()[0]
    test_count = con.execute(
        "SELECT COUNT(*) FROM dim_policy WHERE policy_year > 2023"
    ).fetchone()[0]

    print("\n  --- freMTPL2 Dataset Summary ---")
    print(f"  Policies:              {summary[0]:>10,d}")
    print(f"  Total exposure (yrs):  {summary[1]:>10,.1f}")
    print(f"  Policies with claims:  {summary[2]:>10,d}")
    print(f"  Total claim records:   {summary[3]:>10,d}")
    print(f"  Avg annual premium:    {summary[4]:>10,.2f}")
    print(f"  Portfolio pure premium:{summary[5]:>10,.2f}")
    print(f"  Train split (~80%):    {train_count:>10,d}")
    print(f"  Test split (~20%):     {test_count:>10,d}")
