#!/usr/bin/env bash

set -u

# Always run from the repository root, including when launched by cron or launchd.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if ! cd "$SCRIPT_DIR"; then
    echo "[ERROR] Could not switch to script directory: $SCRIPT_DIR" >&2
    exit 1
fi

# Prefer a repository-local virtual environment before falling back to PATH.
PYTHON_CMD=""
for candidate in "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/venv/bin/python"; do
    if [[ -x "$candidate" ]]; then
        PYTHON_CMD="$candidate"
        break
    fi
done

if [[ -z "$PYTHON_CMD" ]] && command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="$(command -v python3)"
fi
if [[ -z "$PYTHON_CMD" ]] && command -v python >/dev/null 2>&1; then
    PYTHON_CMD="$(command -v python)"
fi

if [[ -z "$PYTHON_CMD" ]]; then
    echo "[ERROR] Python was not found. Install Python or add it to PATH." >&2
    exit 127
fi

if ! "$PYTHON_CMD" -c "import playwright" >/dev/null 2>&1; then
    echo "[ERROR] The selected Python interpreter does not have 'playwright' installed." >&2
    echo "[ERROR] Active interpreter: $PYTHON_CMD" >&2
    echo "[ERROR] Recommended fix: \"$PYTHON_CMD\" -m pip install -r requirements.txt" >&2
    echo "[ERROR] If you are using a fresh environment, also run: \"$PYTHON_CMD\" -m playwright install" >&2
    exit 1
fi

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    shift
fi

if (( $# > 0 )); then
    echo "Usage: $0 [--dry-run]" >&2
    exit 2
fi

run_step() {
    local step_number="$1"
    local step_name="$2"
    shift 2

    echo "[$step_number/4] Running $step_name..."
    if (( DRY_RUN == 1 )); then
        printf '[DRY RUN] "%s"' "$PYTHON_CMD"
        printf ' "%s"' "$@"
        printf '\n'
        return 0
    fi

    "$PYTHON_CMD" "$@"
    local status=$?
    if (( status != 0 )); then
        echo "[ERROR] $step_name failed with exit code $status." >&2
        exit "$status"
    fi
}

echo "================================================================"
echo "Starting pipeline: Playwright_test -> NHTSA_enrichment -> VehicleNormalization -> DataCleaning"
echo "Working directory: $PWD"
echo "Python command   : $PYTHON_CMD"
echo "================================================================"

run_step 1 "Playwright_test" "DataPipeline/Playwright_test.py"
# Writes normalized source fields only; no full API-response or raw-row JSON blobs.
run_step 2 "NHTSA_enrichment" "DataPipeline/NHTSA_enrichment.py" --resume --refresh-days 30 --rate-limit-delay 1.0

# Refresh and validate the shared EPA cache before cleaning imports it into SQLite.
run_step 3 "VehicleNormalization EPA refresh" "DataPipeline/VehicleNormalization.py"

# EPA was refreshed above, so import the validated cache without downloading it again.
run_step 4 "DataCleaning" "DataPipeline/DataCleaning.py" "--no-epa-refresh"

echo "================================================================"
echo "[SUCCESS] Pipeline completed successfully!"
echo "================================================================"
