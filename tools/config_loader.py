"""Centralized configuration loader for GCP project settings.

Loads configuration from environment variables, a YAML config file, or CLI
arguments.  Provides defaults for common GCP settings (project_id, region,
dataset names) and supports dev/prod environment prefixing that matches the
Terraform module pattern used in Project 04.

Usage as a module::

    from tools.config_loader import load_config

    cfg = load_config(config_path="config.yaml", environment="dev")
    print(cfg.get("gcp.project_id"))
    print(cfg.to_dict())

Usage from CLI::

    python tools/config_loader.py --config config.yaml --env dev
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, Any] = {
    "environment": "dev",
    "gcp": {
        "project_id": "my-project-id",
        "region": "us-central1",
    },
    "bigquery": {
        "datasets": {
            "raw": "claims_raw",
            "staging": "claims_staging",
            "intermediate": "claims_intermediate",
            "analytics": "claims_analytics",
            "reports": "claims_reports",
        },
    },
    "gcs": {
        "data_bucket": "claims-data-bucket",
    },
    "pubsub": {
        "claims_topic": "claims-events",
        "dlq_topic": "claims-events-dlq",
    },
}

# Environment variable mapping: env-var name -> dotted config key
_ENV_MAP: dict[str, str] = {
    "GCP_PROJECT_ID": "gcp.project_id",
    "GCP_REGION": "gcp.region",
    "BQ_DATASET_RAW": "bigquery.datasets.raw",
    "BQ_DATASET_STAGING": "bigquery.datasets.staging",
    "BQ_DATASET_INTERMEDIATE": "bigquery.datasets.intermediate",
    "BQ_DATASET_ANALYTICS": "bigquery.datasets.analytics",
    "BQ_DATASET_REPORTS": "bigquery.datasets.reports",
    "GCS_DATA_BUCKET": "gcs.data_bucket",
    "PUBSUB_CLAIMS_TOPIC": "pubsub.claims_topic",
    "PUBSUB_DLQ_TOPIC": "pubsub.dlq_topic",
    "CONFIG_ENVIRONMENT": "environment",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*, returning a new dict."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _set_dotted(data: dict, dotted_key: str, value: Any) -> None:
    """Set a value in a nested dict using a dotted key path."""
    keys = dotted_key.split(".")
    current = data
    for k in keys[:-1]:
        current = current.setdefault(k, {})
    current[keys[-1]] = value


def _get_dotted(data: dict, dotted_key: str, default: Any = None) -> Any:
    """Get a value from a nested dict using a dotted key path."""
    keys = dotted_key.split(".")
    current = data
    for k in keys:
        if not isinstance(current, dict) or k not in current:
            return default
        current = current[k]
    return current


# ---------------------------------------------------------------------------
# Config class
# ---------------------------------------------------------------------------

class Config:
    """Resolved configuration with dotted-key access."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Return a single config value by dotted path.

        Args:
            dotted_key: Dot-separated path, e.g. ``"gcp.project_id"``.
            default: Value returned when the key is missing.

        Returns:
            The resolved value or *default*.
        """
        return _get_dotted(self._data, dotted_key, default)

    def to_dict(self) -> dict[str, Any]:
        """Return the full resolved configuration as a plain dict."""
        return dict(self._data)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_config(
    config_path: str | Path | None = None,
    environment: str | None = None,
) -> Config:
    """Build a resolved Config by layering defaults, file, and env vars.

    Resolution order (last wins):
        1. Built-in defaults
        2. YAML config file (if provided and exists)
        3. Environment variables

    Non-prod environments automatically prefix BigQuery dataset names
    (e.g. ``dev_claims_raw``) to match the Terraform naming convention.

    Args:
        config_path: Path to a YAML config file.  Ignored when ``None``
            or if the file does not exist.
        environment: Override for the ``environment`` key.  When ``None``
            the value is taken from the config file or defaults.

    Returns:
        A fully resolved :class:`Config` instance.

    Raises:
        SystemExit: When required keys (``gcp.project_id``) are still at
            their placeholder defaults after resolution.
    """
    config = dict(_DEFAULTS)

    # --- Layer 2: YAML file ---
    if config_path is not None:
        path = Path(config_path)
        if path.is_file():
            if yaml is None:
                print(
                    "WARNING: PyYAML is not installed. "
                    "Install it with `pip install pyyaml` to load YAML configs.",
                    file=sys.stderr,
                )
            else:
                with open(path) as fh:
                    file_data = yaml.safe_load(fh) or {}
                config = _deep_merge(config, file_data)

    # --- Layer 3: environment variables ---
    for env_var, dotted_key in _ENV_MAP.items():
        value = os.environ.get(env_var)
        if value is not None:
            _set_dotted(config, dotted_key, value)

    # --- Environment override from caller ---
    if environment is not None:
        config["environment"] = environment

    # --- Prefix BigQuery datasets for non-prod environments ---
    env = config.get("environment", "dev")
    if env != "prod":
        datasets = config.get("bigquery", {}).get("datasets", {})
        config["bigquery"]["datasets"] = {
            name: f"{env}_{ds}" if not ds.startswith(f"{env}_") else ds
            for name, ds in datasets.items()
        }

    return Config(config)


def validate_config(cfg: Config) -> list[str]:
    """Check that required configuration values are present.

    Args:
        cfg: A resolved Config instance.

    Returns:
        A list of error messages.  Empty list means valid.
    """
    errors: list[str] = []
    project_id = cfg.get("gcp.project_id")
    if not project_id or project_id == "my-project-id":
        errors.append(
            "gcp.project_id is not set. "
            "Set GCP_PROJECT_ID env var or provide it in config.yaml."
        )
    return errors


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point: print resolved config as JSON."""
    parser = argparse.ArgumentParser(
        description="Resolve and display GCP project configuration.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml).",
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Environment override (dev, staging, prod).",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate that required config values are set.",
    )
    args = parser.parse_args()

    cfg = load_config(config_path=args.config, environment=args.env)

    if args.validate:
        errors = validate_config(cfg)
        if errors:
            for err in errors:
                print(f"ERROR: {err}", file=sys.stderr)
            sys.exit(1)
        print("Config is valid.")
    else:
        print(json.dumps(cfg.to_dict(), indent=2))


if __name__ == "__main__":
    main()
