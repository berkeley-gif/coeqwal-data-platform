#!/usr/bin/env bash
# setup_etl_cloud9.sh
#
# One-shot Cloud9 preflight for the COEQWAL ETL. Confirms the environment
# is ready to run ingestion + statistics against the live S3 bucket and
# production RDS. Idempotent. Each step prints PASS / WARN / FAIL with a
# one-line remediation hint.
#
# Usage:
#   bash scripts/setup_etl_cloud9.sh             # full preflight + venv install
#   bash scripts/setup_etl_cloud9.sh --check     # read-only checks only, no install
#   bash scripts/setup_etl_cloud9.sh --no-venv   # checks + install but skip venv creation
#
# Exit code: 0 if every check passed, 1 otherwise.
#
# Cloud9 specifics this script assumes:
#   - AWS creds come from the EC2 IAM role (no aws configure needed)
#   - DATABASE_URL points at production RDS (set in your shell rc)
#   - rclone is the dependency most likely to be missing or unconfigured
#   - root EBS volume is the bottleneck during scenario downloads

set -uo pipefail

CHECK_ONLY=0
SKIP_VENV=0

for arg in "$@"; do
  case "$arg" in
    --check)   CHECK_ONLY=1 ;;
    --no-venv) SKIP_VENV=1 ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

FAIL_COUNT=0
WARN_COUNT=0

pass() { printf '\033[32mPASS\033[0m  %s\n' "$1"; }
warn() { printf '\033[33mWARN\033[0m  %-50s -> %s\n' "$1" "$2"; WARN_COUNT=$((WARN_COUNT + 1)); }
fail() { printf '\033[31mFAIL\033[0m  %-50s -> %s\n' "$1" "$2"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
note() { printf '      %s\n' "$1"; }
step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
# 1. AWS credentials (IAM role on Cloud9)
# ---------------------------------------------------------------------------
step "1/6 AWS credentials"

if ! command -v aws >/dev/null 2>&1; then
  fail "aws CLI installed" "this Cloud9 instance is missing the aws CLI. Reach out to whoever owns the AMI."
else
  if aws sts get-caller-identity >/dev/null 2>&1; then
    ARN=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null)
    pass "aws sts get-caller-identity ($ARN)"
  else
    fail "aws sts get-caller-identity" "the EC2 IAM role is not attached or is missing sts:GetCallerIdentity. Check the Cloud9 instance role in the AWS console."
  fi
fi

# ---------------------------------------------------------------------------
# 2. rclone + gdrive remote
# ---------------------------------------------------------------------------
step "2/6 rclone + gdrive"

if ! command -v rclone >/dev/null 2>&1; then
  fail "rclone installed" "run: curl https://rclone.org/install.sh | sudo bash"
else
  pass "$(rclone version | head -1)"
  if rclone listremotes 2>/dev/null | grep -q '^gdrive:'; then
    if rclone lsd gdrive: >/dev/null 2>&1; then
      pass "rclone lsd gdrive:"
    else
      fail "rclone lsd gdrive:" "remote is configured but the listing failed. Re-auth: rclone config reconnect gdrive:"
    fi
  else
    fail "rclone 'gdrive:' remote configured" "copy your local ~/.config/rclone/rclone.conf to this Cloud9 instance, or run: rclone config"
  fi
fi

# ---------------------------------------------------------------------------
# 3. Python venv + requirements
# ---------------------------------------------------------------------------
step "3/6 Python venv + requirements"

PYTHON_BIN=""
for candidate in python3.12 python3.11 python3.10 python3; do
  if command -v "$candidate" >/dev/null 2>&1; then
    if "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
      PYTHON_BIN="$candidate"
      break
    fi
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  fail "python >= 3.10" "install Python 3.10+ on Cloud9"
else
  pass "$($PYTHON_BIN --version) at $(command -v $PYTHON_BIN)"

  VENV_DIR="$REPO_ROOT/.venv"
  if [ "$SKIP_VENV" -eq 1 ]; then
    note "Skipping venv creation (--no-venv)"
  elif [ ! -d "$VENV_DIR" ]; then
    if [ "$CHECK_ONLY" -eq 1 ]; then
      fail ".venv exists" "rerun without --check to create it"
    else
      note "Creating $VENV_DIR ..."
      if "$PYTHON_BIN" -m venv "$VENV_DIR"; then
        pass "created .venv/"
      else
        fail "venv creation" "see traceback above"
      fi
    fi
  else
    pass ".venv/ already exists"
  fi

  if [ -d "$VENV_DIR" ]; then
    # shellcheck disable=SC1091
    . "$VENV_DIR/bin/activate"

    if [ "$CHECK_ONLY" -eq 1 ]; then
      note "Skipping pip install (--check)"
    else
      pip install --quiet --upgrade pip setuptools wheel >/dev/null 2>&1 && pass "pip upgraded ($(pip --version | awk '{print $2}'))" || warn "pip upgrade" "rerun manually to see the error"

      if [ -f "$REPO_ROOT/requirements.txt" ]; then
        note "Installing top-level requirements (this can take a minute)..."
        if pip install --quiet -r "$REPO_ROOT/requirements.txt"; then
          pass "installed requirements.txt"
        else
          fail "pip install -r requirements.txt" "rerun manually: source .venv/bin/activate && pip install -r requirements.txt"
        fi
      else
        fail "requirements.txt present" "expected at $REPO_ROOT/requirements.txt"
      fi
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 4. DATABASE_URL
# ---------------------------------------------------------------------------
step "4/6 DATABASE_URL"

if [ -z "${DATABASE_URL:-}" ]; then
  fail "DATABASE_URL set" "add 'export DATABASE_URL=postgresql://...' to ~/.bashrc and 'source ~/.bashrc'. Get the value from the team's password manager."
else
  # Redact the password before printing
  REDACTED=$(printf '%s' "$DATABASE_URL" | sed -E 's|(://[^:]+):[^@]+@|\1:***@|')
  pass "DATABASE_URL set ($REDACTED)"
  if command -v psql >/dev/null 2>&1; then
    if psql "$DATABASE_URL" -c 'SELECT 1' >/dev/null 2>&1; then
      pass "psql connects"
    else
      fail "psql connects" "DATABASE_URL is set but the connection failed. Check the RDS security group and your VPC routing."
    fi
  else
    warn "psql installed" "install the postgresql client on Cloud9"
  fi
fi

# ---------------------------------------------------------------------------
# 5. EBS root volume capacity
# ---------------------------------------------------------------------------
step "5/6 EBS root volume"

USED_PCT=$(df -P / | awk 'NR==2 {gsub("%",""); print $5}')
USED_PCT_NUM=${USED_PCT:-0}
AVAIL=$(df -h / | awk 'NR==2 {print $4}')

if [ "$USED_PCT_NUM" -ge 90 ] 2>/dev/null; then
  fail "root volume usage ${USED_PCT}% ($AVAIL free)" "scenario downloads need ~10-20 GB. Free space or grow the EBS volume."
elif [ "$USED_PCT_NUM" -ge 70 ] 2>/dev/null; then
  warn "root volume usage ${USED_PCT}% ($AVAIL free)" "still room for one or two scenarios. Consider cleanup before a big backfill."
else
  pass "root volume usage ${USED_PCT}% ($AVAIL free)"
fi

# ---------------------------------------------------------------------------
# 6. Smoke imports
# ---------------------------------------------------------------------------
step "6/6 ETL package import"

if [ -d "$REPO_ROOT/.venv" ] && [ -z "${VIRTUAL_ENV:-}" ]; then
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.venv/bin/activate"
fi

if python -c 'import sys; sys.path.insert(0, "."); from etl.common import S3_BUCKET' >/dev/null 2>&1; then
  pass "from etl.common import S3_BUCKET"
else
  fail "etl.common importable" "rerun without --check to install requirements, or run: pip install -r requirements.txt"
fi

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
echo ""
if [ "$FAIL_COUNT" -eq 0 ] && [ "$WARN_COUNT" -eq 0 ]; then
  printf '\033[32mAll checks passed.\033[0m\n'
  echo "Ready to run: python etl/run_full_pipeline.py"
  exit 0
elif [ "$FAIL_COUNT" -eq 0 ]; then
  printf '\033[33m%d warning(s).\033[0m Fix or ignore, then proceed.\n' "$WARN_COUNT"
  echo "Ready to run: python etl/run_full_pipeline.py"
  exit 0
else
  printf '\033[31m%d check(s) failed, %d warning(s).\033[0m Fix the failures above and rerun.\n' "$FAIL_COUNT" "$WARN_COUNT"
  exit 1
fi
