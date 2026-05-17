#!/usr/bin/env bash
# setup_dev_env.sh
#
# One-shot local setup for the COEQWAL backend, targeting macOS and Linux.
# Idempotent: rerun anytime. Each step prints what it did and why if it
# skipped. End state matches `docs/CLOUD9_INVENTORY.md` (the parity target),
# minus direct production-RDS access (Cloud9 keeps that).
#
# Usage:
#   bash scripts/setup_dev_env.sh                # full setup
#   bash scripts/setup_dev_env.sh --skip-postgres  # everything except local DB
#   bash scripts/setup_dev_env.sh --skip-seeds     # bring DB up, do not load seeds
#
# Steps:
#   1. Python 3.10+ check / advice
#   2. Project venv + pip install -r requirements.txt
#   3. rclone install + remote prompt
#   4. AWS CLI check + SSO advice
#   5. Docker check
#   6. docker compose up -d postgres
#   7. scripts/load_local_seeds.sh
#   8. scripts/check_env.sh

set -uo pipefail

SKIP_POSTGRES=0
SKIP_SEEDS=0

for arg in "$@"; do
  case "$arg" in
    --skip-postgres) SKIP_POSTGRES=1; SKIP_SEEDS=1 ;;
    --skip-seeds)    SKIP_SEEDS=1 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OS_KIND="unknown"
case "$(uname -s)" in
  Darwin) OS_KIND="mac" ;;
  Linux)  OS_KIND="linux" ;;
esac

LOCAL_DATABASE_URL="postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario"

ok()   { printf '\033[32m  OK\033[0m  %s\n' "$1"; }
warn() { printf '\033[33mWARN\033[0m  %s\n' "$1"; }
info() { printf '      %s\n' "$1"; }
step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
die()  { printf '\033[31m FAIL\033[0m %s\n' "$1" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Python
# ---------------------------------------------------------------------------
step "1/8 Python 3.10+"

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
  warn "No Python 3.10+ found."
  case "$OS_KIND" in
    mac)   info "Install with: brew install python@3.12   (or use pyenv: brew install pyenv && pyenv install 3.12)" ;;
    linux) info "Install with: sudo apt-get install -y python3.12 python3.12-venv   (Debian/Ubuntu) or distro equivalent" ;;
    *)     info "Install Python 3.10+ from https://www.python.org/" ;;
  esac
  die "Install Python 3.10+ and rerun."
fi
ok "$($PYTHON_BIN --version) at $(command -v $PYTHON_BIN)"

# ---------------------------------------------------------------------------
# 2. Virtual environment + requirements
# ---------------------------------------------------------------------------
step "2/8 Project venv at .venv/"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  "$PYTHON_BIN" -m venv .venv || die "venv creation failed"
  ok "Created .venv/"
else
  ok ".venv/ already exists"
fi

# shellcheck disable=SC1091
. "$REPO_ROOT/.venv/bin/activate"

pip install --quiet --upgrade pip setuptools wheel || die "pip upgrade failed"
ok "pip $(pip --version | awk '{print $2}')"

if [ -f "$REPO_ROOT/requirements.txt" ]; then
  info "Installing project requirements (this can take a minute)..."
  pip install --quiet -r "$REPO_ROOT/requirements.txt" || die "pip install -r requirements.txt failed"
  ok "Installed top-level requirements.txt"
else
  warn "No top-level requirements.txt found."
fi

# ---------------------------------------------------------------------------
# 3. rclone
# ---------------------------------------------------------------------------
step "3/8 rclone"

if ! command -v rclone >/dev/null 2>&1; then
  warn "rclone is not installed."
  case "$OS_KIND" in
    mac)
      if command -v brew >/dev/null 2>&1; then
        info "Installing via: brew install rclone"
        brew install rclone || warn "brew install rclone failed. Install manually."
      else
        info "Install Homebrew (https://brew.sh) and rerun, or run: curl https://rclone.org/install.sh | sudo bash"
      fi
      ;;
    *)
      info "Run: curl https://rclone.org/install.sh | sudo bash"
      ;;
  esac
fi

if command -v rclone >/dev/null 2>&1; then
  ok "$(rclone version | head -1)"
  if rclone listremotes 2>/dev/null | grep -q '^gdrive:'; then
    ok "rclone remote 'gdrive:' is configured"
  else
    warn "rclone has no 'gdrive:' remote yet."
    info "Run: rclone config   (choose 'n' for new, name it 'gdrive', type 'drive', follow OAuth prompts)"
    info "See etl/README.md for the COEQWAL Drive folder ID once authenticated."
  fi
fi

# ---------------------------------------------------------------------------
# 4. AWS CLI
# ---------------------------------------------------------------------------
step "4/8 AWS CLI"

if ! command -v aws >/dev/null 2>&1; then
  warn "aws CLI not installed."
  case "$OS_KIND" in
    mac)   info "Install with: brew install awscli" ;;
    linux) info "Install per https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" ;;
  esac
else
  ok "$(aws --version 2>&1)"
  if aws sts get-caller-identity >/dev/null 2>&1; then
    ok "AWS credentials valid: $(aws sts get-caller-identity --query 'Arn' --output text 2>/dev/null)"
  else
    warn "aws sts get-caller-identity failed. Configure SSO:"
    info "  aws configure sso"
    info "  Use SSO start URL for the COEQWAL account, region us-west-2."
  fi
fi

# ---------------------------------------------------------------------------
# 5. Docker
# ---------------------------------------------------------------------------
step "5/8 Docker"

if ! command -v docker >/dev/null 2>&1; then
  warn "docker not installed."
  case "$OS_KIND" in
    mac)   info "Install Docker Desktop from https://www.docker.com/products/docker-desktop/" ;;
    linux) info "Install Docker Engine per https://docs.docker.com/engine/install/" ;;
  esac
elif ! docker info >/dev/null 2>&1; then
  warn "docker is installed but the daemon is not running."
  info "Start Docker Desktop (mac) or 'sudo systemctl start docker' (linux), then rerun."
else
  ok "$(docker --version)"
fi

# ---------------------------------------------------------------------------
# 6. Local Postgres
# ---------------------------------------------------------------------------
step "6/8 Local Postgres (docker compose)"

if [ "$SKIP_POSTGRES" -eq 1 ]; then
  info "Skipped (--skip-postgres)"
elif ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  warn "Docker not ready. Skipping local Postgres bring-up. Fix Docker and rerun."
else
  if ! docker compose version >/dev/null 2>&1; then
    warn "'docker compose' v2 plugin not available. Install Docker Desktop or compose v2."
  else
    info "Starting service 'postgres'..."
    docker compose up -d postgres || warn "docker compose up failed"
    info "Waiting for Postgres healthcheck..."
    for i in $(seq 1 30); do
      if docker compose exec -T postgres pg_isready -U coeqwal -d coeqwal_scenario >/dev/null 2>&1; then
        ok "Postgres is ready on localhost:5432"
        break
      fi
      sleep 1
    done
    if ! docker compose exec -T postgres pg_isready -U coeqwal -d coeqwal_scenario >/dev/null 2>&1; then
      warn "Postgres did not become ready in 30 seconds. Inspect with: docker compose logs postgres"
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 7. Seeds
# ---------------------------------------------------------------------------
step "7/8 Schema + seeds"

if [ "$SKIP_SEEDS" -eq 1 ]; then
  info "Skipped (--skip-seeds)"
elif ! command -v psql >/dev/null 2>&1; then
  warn "psql not installed. The seed loader needs it."
  case "$OS_KIND" in
    mac)   info "Install with: brew install libpq && brew link --force libpq" ;;
    linux) info "Install with: sudo apt-get install -y postgresql-client" ;;
  esac
else
  export DATABASE_URL="$LOCAL_DATABASE_URL"
  info "Using DATABASE_URL=$LOCAL_DATABASE_URL"
  if bash "$REPO_ROOT/scripts/load_local_seeds.sh"; then
    ok "Schema and seeds applied"
  else
    warn "load_local_seeds.sh exited non-zero. Inspect output above and rerun."
  fi
fi

# ---------------------------------------------------------------------------
# 8. Smoke test
# ---------------------------------------------------------------------------
step "8/8 Smoke test"

if [ -x "$REPO_ROOT/scripts/check_env.sh" ] || [ -f "$REPO_ROOT/scripts/check_env.sh" ]; then
  bash "$REPO_ROOT/scripts/check_env.sh" || warn "Some checks failed. See output above for remediation hints."
else
  warn "scripts/check_env.sh not present. Skipping smoke test."
fi

# ---------------------------------------------------------------------------
# Final advice
# ---------------------------------------------------------------------------
step "Next steps"
cat <<EOF
Add this to your shell rc (~/.zshrc on mac, ~/.bashrc on linux) for future sessions:

  source $REPO_ROOT/.venv/bin/activate
  export DATABASE_URL="$LOCAL_DATABASE_URL"

Operator (Cloud9) tasks like ingestion against production S3 still belong on
Cloud9. See etl/README.md.

If anything above reported WARN or FAIL, fix and rerun:
  bash scripts/setup_dev_env.sh
EOF
