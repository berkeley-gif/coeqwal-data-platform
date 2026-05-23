#!/usr/bin/env bash
# check_env.sh
#
# UNSUPPORTED: best-effort Linux smoke test for the local developer
# environment (API + local Postgres). Production ETL belongs on Cloud9,
# see etl/README.md. Not maintained for macOS or Windows.
#
# Prints PASS or FAIL for each prerequisite, with a one-line remediation
# hint per FAIL.
#
# Usage:
#   bash scripts/check_env.sh             # quick checks only
#   bash scripts/check_env.sh --container # also docker-build the batch container (slow)
#
# Exit code: 0 if every check passed, 1 otherwise.

set -uo pipefail

DO_CONTAINER=0
for arg in "$@"; do
  case "$arg" in
    --container) DO_CONTAINER=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DEFAULT_DATABASE_URL="postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario"
DATABASE_URL="${DATABASE_URL:-$DEFAULT_DATABASE_URL}"

FAIL_COUNT=0

pass() { printf '\033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '\033[31mFAIL\033[0m  %-50s -> %s\n' "$1" "$2"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
note() { printf '      %s\n' "$1"; }

check() {
  # check "label" "remediation" -- <command...>
  local label="$1"; shift
  local hint="$1"; shift
  local sep="$1"; shift
  if [ "$sep" != "--" ]; then echo "check() misuse" >&2; exit 2; fi
  if "$@" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label" "$hint"
  fi
}

# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------
if [ -n "${VIRTUAL_ENV:-}" ]; then
  note "Using venv: $VIRTUAL_ENV"
elif [ -f "$REPO_ROOT/.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.venv/bin/activate"
  note "Activated $REPO_ROOT/.venv"
fi

if command -v python >/dev/null 2>&1 && \
   python -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
  pass "python >= 3.10 ($(python --version 2>&1))"
else
  fail "python >= 3.10" "install Python 3.10+ and rerun scripts/setup_dev_env.sh"
fi

# ---------------------------------------------------------------------------
# Python packages
# ---------------------------------------------------------------------------
for pkg in boto3 pandas asyncpg psycopg2 fastapi uvicorn; do
  if python -c "import $pkg" >/dev/null 2>&1; then
    pass "import $pkg"
  else
    fail "import $pkg" "pip install -r requirements.txt"
  fi
done

# ---------------------------------------------------------------------------
# rclone
# ---------------------------------------------------------------------------
if command -v rclone >/dev/null 2>&1; then
  if rclone listremotes 2>/dev/null | grep -q '^gdrive:'; then
    pass "rclone gdrive: remote configured"
  else
    fail "rclone gdrive: remote" "run: rclone config (add a 'drive' remote named 'gdrive')"
  fi
else
  fail "rclone installed" "curl https://rclone.org/install.sh | sudo bash"
fi

# ---------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------
if command -v aws >/dev/null 2>&1; then
  if aws sts get-caller-identity >/dev/null 2>&1; then
    pass "aws sts get-caller-identity"
  else
    fail "aws credentials" "run: aws configure sso   (COEQWAL account, region us-west-2)"
  fi
else
  fail "aws CLI installed" "see https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
fi

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    pass "docker daemon running"
  else
    fail "docker daemon" "sudo systemctl start docker"
  fi
else
  fail "docker installed" "install Docker Engine (https://docs.docker.com/engine/install/)"
fi

# ---------------------------------------------------------------------------
# Local Postgres reachable
# ---------------------------------------------------------------------------
if command -v psql >/dev/null 2>&1; then
  if psql "$DATABASE_URL" -c "SELECT 1" >/dev/null 2>&1; then
    pass "psql connects to local DB"
    # Count tables. If there are zero, schema has not been loaded.
    TABLE_COUNT=$(psql "$DATABASE_URL" -tAc \
      "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'" 2>/dev/null \
      || echo "0")
    if [ "${TABLE_COUNT:-0}" -gt 0 ] 2>/dev/null; then
      pass "schema loaded ($TABLE_COUNT tables in public)"
      if psql "$DATABASE_URL" -tAc 'SELECT count(*) FROM scenario' >/dev/null 2>&1; then
        SCENARIO_COUNT=$(psql "$DATABASE_URL" -tAc 'SELECT count(*) FROM scenario' 2>/dev/null | tr -d '[:space:]')
        pass "scenario table queryable (${SCENARIO_COUNT} rows)"
      else
        fail "scenario table" "rerun bash scripts/load_local_seeds.sh (or check it for errors)"
      fi
    else
      fail "schema loaded" "bash scripts/load_local_seeds.sh"
    fi
  else
    fail "psql connects" "bring DB up: docker compose up -d postgres   (or set DATABASE_URL)"
  fi
else
  fail "psql installed" "sudo apt-get install -y postgresql-client"
fi

# ---------------------------------------------------------------------------
# API importability
# ---------------------------------------------------------------------------
if [ -f "$REPO_ROOT/api/coeqwal-api/main.py" ]; then
  if ( cd "$REPO_ROOT/api/coeqwal-api" && python -c 'import main' ) >/dev/null 2>&1; then
    pass "api main.py imports"
  else
    fail "api main.py imports" "cd api/coeqwal-api && python -c 'import main' to see the traceback"
  fi
else
  note "api/coeqwal-api/main.py not present; skipping API import check"
fi

# ---------------------------------------------------------------------------
# Optional: batch-container build (slow)
# ---------------------------------------------------------------------------
if [ "$DO_CONTAINER" -eq 1 ]; then
  if [ -f "$REPO_ROOT/etl/batch-container/Dockerfile" ]; then
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      note "Building etl/batch-container/ (this can take a few minutes)..."
      if docker build -q "$REPO_ROOT/etl/batch-container/" >/dev/null 2>&1; then
        pass "docker build etl/batch-container"
      else
        fail "docker build etl/batch-container" "run docker build etl/batch-container/ manually to see the error"
      fi
    else
      fail "docker build (container check)" "Docker not ready"
    fi
  else
    note "etl/batch-container/Dockerfile not present; skipping container build check"
  fi
else
  note "Skipping container build. Add --container to include it."
fi

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
echo ""
if [ "$FAIL_COUNT" -eq 0 ]; then
  printf '\033[32mAll checks passed.\033[0m\n'
  exit 0
else
  printf '\033[31m%d check(s) failed.\033[0m Run bash scripts/setup_dev_env.sh to remediate, then rerun this script.\n' "$FAIL_COUNT"
  exit 1
fi
