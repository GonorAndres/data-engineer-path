#!/usr/bin/env python3
"""Deploy Dataform SQLX project to BigQuery using the Dataform API.

Uploads local SQLX files to a Dataform workspace, compiles them with
environment-specific overrides, and executes the workflow. Polls for
completion and prints a summary of created tables with row counts.

Usage:
    python scripts/deploy_dataform.py --project project-ad7a5be2-a1c7-4510-82d
    python scripts/deploy_dataform.py --project PROJECT_ID --env prod
    python scripts/deploy_dataform.py --project PROJECT_ID --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from types import ModuleType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_REGION = "us-central1"
DEFAULT_REPO_NAME = "claims-warehouse-dataform"
DEFAULT_ENV = "dev"
DEFAULT_VALUATION_DATE = "2025-12-31"
DEFAULT_MAX_BYTES_BILLED = "10737418240"
DATAFORM_DIR = Path(__file__).resolve().parent.parent / "dataform"
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 60
WORKSPACE_ID = "deploy-workspace"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------


def _import_dataform() -> ModuleType:
    """Lazily import the Dataform SDK.

    Returns the ``google.cloud.dataform_v1beta1`` module. If the package is
    not installed, prints install instructions and exits.

    Returns:
        The ``google.cloud.dataform_v1beta1`` module.
    """
    try:
        from google.cloud import dataform_v1beta1  # type: ignore[attr-defined]

        return dataform_v1beta1
    except ImportError:
        print(
            "Error: google-cloud-dataform is not installed.\n"
            "Install it with: pip install 'google-cloud-dataform>=0.5.0'\n"
            "Or:              pip install -e '.[gcp]'",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Repository & workspace helpers
# ---------------------------------------------------------------------------


def ensure_repository(
    client: object,
    project: str,
    region: str,
    repo_name: str,
) -> str:
    """Create a Dataform repository if it does not already exist.

    Args:
        client: A ``DataformClient`` instance.
        project: GCP project ID.
        region: GCP region (e.g. ``us-central1``).
        repo_name: Short repository name.

    Returns:
        The full resource name of the repository.
    """
    from google.api_core import exceptions as gcp_exceptions

    parent = f"projects/{project}/locations/{region}"
    repo_path = f"{parent}/repositories/{repo_name}"

    try:
        dataform = _import_dataform()
        request = dataform.CreateRepositoryRequest(
            parent=parent,
            repository=dataform.Repository(name=repo_path),
            repository_id=repo_name,
        )
        repo = client.create_repository(request=request)
        log.info("Created repository: %s", repo.name)
        return repo.name
    except gcp_exceptions.AlreadyExists:
        log.info("Repository already exists: %s", repo_path)
        return repo_path
    except Exception as exc:
        if "ALREADY_EXISTS" in str(exc):
            log.info("Repository already exists: %s", repo_path)
            return repo_path
        raise


def ensure_workspace(
    client: object,
    repo_name: str,
    workspace_id: str,
) -> str:
    """Create a workspace inside the repository if it does not already exist.

    Args:
        client: A ``DataformClient`` instance.
        repo_name: Full resource name of the parent repository.
        workspace_id: Short workspace identifier.

    Returns:
        The full resource name of the workspace.
    """
    from google.api_core import exceptions as gcp_exceptions

    workspace_path = f"{repo_name}/workspaces/{workspace_id}"

    try:
        dataform = _import_dataform()
        request = dataform.CreateWorkspaceRequest(
            parent=repo_name,
            workspace=dataform.Workspace(name=workspace_path),
            workspace_id=workspace_id,
        )
        ws = client.create_workspace(request=request)
        log.info("Created workspace: %s", ws.name)
        return ws.name
    except gcp_exceptions.AlreadyExists:
        log.info("Workspace already exists: %s", workspace_path)
        return workspace_path
    except Exception as exc:
        if "ALREADY_EXISTS" in str(exc):
            log.info("Workspace already exists: %s", workspace_path)
            return workspace_path
        raise


# ---------------------------------------------------------------------------
# File upload
# ---------------------------------------------------------------------------


def upload_dataform_files(
    client: object,
    workspace_name: str,
    dataform_dir: Path,
) -> int:
    """Upload every file from the local ``dataform/`` directory to the workspace.

    Skips directories, dotfiles, and ``node_modules``.

    Args:
        client: A ``DataformClient`` instance.
        workspace_name: Full resource name of the target workspace.
        dataform_dir: Local path to the ``dataform/`` directory.

    Returns:
        Number of files uploaded.
    """
    dataform = _import_dataform()
    count = 0

    for file_path in sorted(dataform_dir.rglob("*")):
        if not file_path.is_file():
            continue

        relative = file_path.relative_to(dataform_dir)
        relative_str = str(relative)

        # Skip dotfiles and node_modules.
        if any(part.startswith(".") for part in relative.parts):
            continue
        if "node_modules" in relative.parts:
            continue

        contents = file_path.read_bytes()

        request = dataform.WriteFileRequest(
            workspace=workspace_name,
            path=relative_str,
            contents=contents,
        )
        client.write_file(request=request)
        log.info("  Uploaded: %s", relative_str)
        count += 1

    log.info("Uploaded %d files to workspace.", count)
    return count


# ---------------------------------------------------------------------------
# Compile & execute
# ---------------------------------------------------------------------------


def compile_workspace(
    client: object,
    repo_name: str,
    workspace_name: str,
    project: str,
    region: str,
    env: str,
    valuation_date: str,
    max_bytes_billed: str,
) -> str:
    """Compile the workspace and return the compilation result resource name.

    Overrides ``defaultDatabase`` so the placeholder value in
    ``dataform.json`` (``your-gcp-project-id``) is replaced with the real
    project ID.

    Args:
        client: A ``DataformClient`` instance.
        repo_name: Full resource name of the repository.
        workspace_name: Full resource name of the workspace.
        project: GCP project ID used as ``default_database``.
        env: Environment tag (``dev`` or ``prod``).
        valuation_date: Valuation date string passed as a Dataform variable.
        max_bytes_billed: Cost-guard variable passed to Dataform.

    Returns:
        The full resource name of the compilation result.

    Raises:
        RuntimeError: If the compilation has errors.
    """
    dataform = _import_dataform()

    code_config = dataform.CodeCompilationConfig(
        default_database=project,
        default_location=region,
        vars={
            "env": env,
            "valuation_date": valuation_date,
            "max_bytes_billed": max_bytes_billed,
        },
    )

    request = dataform.CreateCompilationResultRequest(
        parent=repo_name,
        compilation_result=dataform.CompilationResult(
            workspace=workspace_name,
            code_compilation_config=code_config,
        ),
    )

    result = client.create_compilation_result(request=request)
    log.info("Compilation result: %s", result.name)

    # Check for compilation errors.
    if result.compilation_errors:
        log.error("Compilation failed with %d error(s):", len(result.compilation_errors))
        for err in result.compilation_errors:
            path = getattr(err, "path", "<unknown>")
            message = getattr(err, "message", str(err))
            log.error("  %s: %s", path, message)
        raise RuntimeError(
            f"Dataform compilation failed with {len(result.compilation_errors)} error(s)."
        )

    log.info("Compilation succeeded (no errors).")
    return result.name


def execute_workflow(
    client: object,
    repo_name: str,
    compilation_result_name: str,
) -> str:
    """Create a workflow invocation to execute the compiled Dataform project.

    Args:
        client: A ``DataformClient`` instance.
        repo_name: Full resource name of the repository.
        compilation_result_name: Resource name of the compilation result.

    Returns:
        The full resource name of the workflow invocation.
    """
    dataform = _import_dataform()

    request = dataform.CreateWorkflowInvocationRequest(
        parent=repo_name,
        workflow_invocation=dataform.WorkflowInvocation(
            compilation_result=compilation_result_name,
        ),
    )

    invocation = client.create_workflow_invocation(request=request)
    log.info("Workflow invocation created: %s", invocation.name)
    return invocation.name


def poll_workflow(
    client: object,
    invocation_name: str,
    poll_interval: int = POLL_INTERVAL_SECONDS,
    max_attempts: int = MAX_POLL_ATTEMPTS,
) -> str:
    """Poll the workflow invocation until it reaches a terminal state.

    Args:
        client: A ``DataformClient`` instance.
        invocation_name: Full resource name of the workflow invocation.
        poll_interval: Seconds between polling attempts.
        max_attempts: Maximum number of polling iterations.

    Returns:
        The terminal state name (``SUCCEEDED``).

    Raises:
        RuntimeError: If the workflow fails, is cancelled, or polling times out.
    """
    dataform = _import_dataform()
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}

    for attempt in range(1, max_attempts + 1):
        request = dataform.GetWorkflowInvocationRequest(name=invocation_name)
        invocation = client.get_workflow_invocation(request=request)
        state = invocation.state.name

        log.info("  Poll %d/%d -- state: %s", attempt, max_attempts, state)

        if state in terminal_states:
            if state == "SUCCEEDED":
                log.info("Workflow completed successfully.")
                return state
            raise RuntimeError(f"Workflow ended with state: {state}")

        time.sleep(poll_interval)

    raise RuntimeError(
        f"Workflow did not complete within {max_attempts * poll_interval}s "
        f"({max_attempts} attempts)."
    )


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(
    client: object,
    invocation_name: str,
    project: str,
    env: str,
) -> None:
    """Print a summary of executed workflow actions and BigQuery row counts.

    Args:
        client: A ``DataformClient`` instance.
        invocation_name: Full resource name of the workflow invocation.
        project: GCP project ID (used to query BigQuery row counts).
        env: Environment tag for display purposes.
    """
    dataform = _import_dataform()

    request = dataform.QueryWorkflowInvocationActionsRequest(name=invocation_name)
    actions_response = client.query_workflow_invocation_actions(request=request)

    tables: list[dict[str, str]] = []
    assertions: list[dict[str, str]] = []

    for action in actions_response:
        target = action.target
        entry = {
            "database": target.database,
            "schema": target.schema,
            "name": target.name,
            "state": action.state.name if hasattr(action.state, "name") else str(action.state),
        }
        # Assertions live in the assertion schema; everything else is a table.
        if "assertion" in target.schema.lower():
            assertions.append(entry)
        else:
            tables.append(entry)

    # -- Print tables --
    print("\n" + "=" * 70)
    print(f"  Dataform Deployment Summary  (env={env})")
    print("=" * 70)

    print(f"\nTables created/updated ({len(tables)}):")
    print(f"  {'Schema':<30s} {'Table':<30s} {'State':<12s}")
    print(f"  {'-' * 30} {'-' * 30} {'-' * 12}")
    for t in tables:
        print(f"  {t['schema']:<30s} {t['name']:<30s} {t['state']:<12s}")

    # -- Print assertions --
    if assertions:
        print(f"\nAssertions ({len(assertions)}):")
        print(f"  {'Schema':<30s} {'Assertion':<30s} {'State':<12s}")
        print(f"  {'-' * 30} {'-' * 30} {'-' * 12}")
        for a in assertions:
            print(f"  {a['schema']:<30s} {a['name']:<30s} {a['state']:<12s}")

    # -- Row counts via BigQuery --
    try:
        from google.cloud import bigquery

        bq_client = bigquery.Client(project=project)
        print("\nBigQuery row counts:")
        print(f"  {'Table':<55s} {'Rows':>12s}")
        print(f"  {'-' * 55} {'-' * 12}")
        for t in tables:
            full_table = f"{t['database']}.{t['schema']}.{t['name']}"
            try:
                table_ref = bq_client.get_table(full_table)
                print(f"  {full_table:<55s} {table_ref.num_rows:>12,d}")
            except Exception as row_exc:
                print(f"  {full_table:<55s} {'(error)':>12s}  {row_exc}")
    except ImportError:
        log.warning(
            "google-cloud-bigquery not installed -- skipping row counts."
        )

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]`` when ``None``).

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Deploy Dataform SQLX project to BigQuery via the Dataform API."
        ),
        epilog=(
            "Examples:\n"
            "  %(prog)s --project my-gcp-project\n"
            "  %(prog)s --project my-gcp-project --env prod\n"
            "  %(prog)s --project my-gcp-project --dry-run\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID.",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"GCP region (default: {DEFAULT_REGION}).",
    )
    parser.add_argument(
        "--repo-name",
        default=DEFAULT_REPO_NAME,
        help=f"Dataform repository name (default: {DEFAULT_REPO_NAME}).",
    )
    parser.add_argument(
        "--env",
        default=DEFAULT_ENV,
        choices=["dev", "prod"],
        help=f"Target environment (default: {DEFAULT_ENV}).",
    )
    parser.add_argument(
        "--valuation-date",
        default=DEFAULT_VALUATION_DATE,
        help=f"Valuation date variable (default: {DEFAULT_VALUATION_DATE}).",
    )
    parser.add_argument(
        "--dataform-dir",
        type=Path,
        default=DATAFORM_DIR,
        help=f"Path to local dataform/ directory (default: {DATAFORM_DIR}).",
    )
    parser.add_argument(
        "--workspace-id",
        default=WORKSPACE_ID,
        help=f"Workspace identifier (default: {WORKSPACE_ID}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compile only -- do not execute the workflow.",
    )

    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def deploy(args: argparse.Namespace) -> None:
    """Run the full Dataform deployment pipeline.

    Steps:
        1. Ensure the Dataform repository exists.
        2. Ensure a workspace exists inside the repository.
        3. Upload all local ``dataform/`` files to the workspace.
        4. Compile the workspace with environment overrides.
        5. (Unless ``--dry-run``) Execute the workflow and poll until done.
        6. Print a summary with BigQuery row counts.

    Args:
        args: Parsed CLI arguments.
    """
    dataform = _import_dataform()
    client = dataform.DataformClient()

    log.info("Starting Dataform deployment (env=%s, dry_run=%s)", args.env, args.dry_run)

    # 1. Repository
    repo_name = ensure_repository(client, args.project, args.region, args.repo_name)

    # 2. Workspace
    workspace_name = ensure_workspace(client, repo_name, args.workspace_id)

    # 3. Upload files
    file_count = upload_dataform_files(client, workspace_name, args.dataform_dir)
    if file_count == 0:
        raise RuntimeError(
            f"No files found in {args.dataform_dir}. "
            "Verify the --dataform-dir path."
        )

    # 4. Compile
    compilation_result_name = compile_workspace(
        client,
        repo_name,
        workspace_name,
        args.project,
        args.region,
        args.env,
        args.valuation_date,
        DEFAULT_MAX_BYTES_BILLED,
    )

    if args.dry_run:
        log.info("Dry-run complete. Skipping execution.")
        return

    # 5. Execute
    invocation_name = execute_workflow(client, repo_name, compilation_result_name)

    # 6. Poll & summarize
    poll_workflow(client, invocation_name)
    print_summary(client, invocation_name, args.project, args.env)


def main() -> None:
    """Entry point: parse arguments and run the deployment."""
    args = parse_args()

    try:
        deploy(args)
    except RuntimeError as exc:
        log.error("Deployment failed: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        log.warning("Deployment interrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
