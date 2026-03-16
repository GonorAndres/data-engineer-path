#!/usr/bin/env bash
# setup_dev_env.sh -- Bootstrap local dev environments for portfolio projects.
#
# Usage:
#   ./scripts/setup_dev_env.sh          # Set up ALL projects
#   ./scripts/setup_dev_env.sh 01       # Set up Project 01 only
#   ./scripts/setup_dev_env.sh 01 03    # Set up Projects 01 and 03
#
# For each project the script:
#   1. Creates a Python venv (.venv) if it does not already exist
#   2. Installs the project in editable mode with dev extras
#   3. Runs a quick smoke test (collect tests without executing)
#
# Safe to run repeatedly (idempotent).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECTS_DIR="${REPO_ROOT}/projects"

# Colors (disabled when stdout is not a terminal)
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    RED='\033[0;31m'
    RESET='\033[0m'
else
    GREEN='' YELLOW='' RED='' RESET=''
fi

log_ok()   { printf "${GREEN}[OK]${RESET}   %s\n" "$1"; }
log_warn() { printf "${YELLOW}[WARN]${RESET} %s\n" "$1"; }
log_err()  { printf "${RED}[FAIL]${RESET} %s\n" "$1"; }
log_info() { printf "       %s\n" "$1"; }

# -------------------------------------------------------------------------
# Discover projects
# -------------------------------------------------------------------------
discover_projects() {
    # Returns project directory names that contain a pyproject.toml.
    # Only considers directories matching NN-* (e.g., 01-claims-warehouse).
    for dir in "${PROJECTS_DIR}"/[0-9][0-9]-*/; do
        [ -f "${dir}/pyproject.toml" ] && basename "$dir"
    done
}

filter_projects() {
    # Given a list of requested numbers (e.g., 01 03), return matching dirs.
    local all_projects
    all_projects=$(discover_projects)
    local matched=""
    for num in "$@"; do
        # Pad to two digits
        num=$(printf "%02d" "$num")
        local found
        found=$(echo "$all_projects" | grep "^${num}-" || true)
        if [ -z "$found" ]; then
            log_warn "No project found matching number ${num}"
        else
            matched="${matched}${found}"$'\n'
        fi
    done
    echo "$matched" | sed '/^$/d'
}

# -------------------------------------------------------------------------
# Setup a single project
# -------------------------------------------------------------------------
setup_project() {
    local project_dir="$1"
    local project_path="${PROJECTS_DIR}/${project_dir}"
    local venv_path="${project_path}/.venv"
    local status="ok"

    printf "\n--- Setting up %s ---\n" "$project_dir"

    # Step 1: Create venv if needed
    if [ -d "$venv_path" ]; then
        log_info "Venv already exists at .venv"
    else
        log_info "Creating Python venv..."
        if python3 -m venv "$venv_path" 2>/dev/null; then
            log_ok "Venv created"
        else
            log_err "Failed to create venv"
            return 1
        fi
    fi

    # Step 2: Install dependencies
    log_info "Installing dependencies (pip install -e '.[dev]')..."
    if "${venv_path}/bin/pip" install --quiet --upgrade pip 2>/dev/null \
        && "${venv_path}/bin/pip" install --quiet -e "${project_path}[dev]" 2>/dev/null; then
        log_ok "Dependencies installed"
    else
        log_warn "pip install failed (missing extras or build errors)"
        status="partial"
    fi

    # Step 3: Smoke test -- collect tests without running
    if [ -d "${project_path}/tests" ]; then
        log_info "Collecting tests (smoke check)..."
        if "${venv_path}/bin/python" -m pytest "${project_path}/tests/" --co -q 2>/dev/null; then
            log_ok "Tests collected successfully"
        else
            log_warn "Test collection had issues (may need extra deps)"
            status="partial"
        fi
    else
        log_info "No tests/ directory found -- skipping smoke test"
    fi

    echo "$status"
}

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
main() {
    echo "============================================"
    echo "  Data Engineering Portfolio -- Dev Setup"
    echo "============================================"

    local projects
    if [ $# -gt 0 ]; then
        projects=$(filter_projects "$@")
    else
        projects=$(discover_projects)
    fi

    if [ -z "$projects" ]; then
        log_err "No projects found to set up."
        exit 1
    fi

    local total=0
    local succeeded=0
    local partial=0
    local failed=0

    while IFS= read -r project; do
        [ -z "$project" ] && continue
        total=$((total + 1))
        result=$(setup_project "$project" || echo "failed")
        case "$result" in
            *ok)       succeeded=$((succeeded + 1)) ;;
            *partial)  partial=$((partial + 1)) ;;
            *)         failed=$((failed + 1)) ;;
        esac
    done <<< "$projects"

    # Summary
    printf "\n============================================\n"
    printf "  Summary: %d projects processed\n" "$total"
    printf "    OK:      %d\n" "$succeeded"
    printf "    Partial: %d\n" "$partial"
    printf "    Failed:  %d\n" "$failed"
    printf "============================================\n"

    [ "$failed" -gt 0 ] && exit 1
    exit 0
}

main "$@"
