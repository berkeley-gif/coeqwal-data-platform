#!/usr/bin/env bash
# load_local_seeds.sh
#
# UNSUPPORTED: best-effort Linux bootstrapper for a local Postgres
# instance with the COEQWAL schema and seed data. Production RDS is
# bootstrapped through the Cloud9 developer workflow, see
# database/README.md. Not maintained for macOS or Windows.
#
# Walks `database/scripts/sql/` in a deterministic order and pipes each
# .sql file through `psql "$DATABASE_URL"`. Each file is its own
# transaction (--single-transaction with ON_ERROR_STOP=1), so a failure
# is loud and rolls back cleanly.
#
# Usage (from repo root, after `docker compose up -d postgres`):
#   export DATABASE_URL="postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario"
#   bash scripts/load_local_seeds.sh
#
# Flags:
#   --dry-run   Print the files that would be applied, but do not run them.
#   --skip-verify   Skip 09_verify_levelXX.sql checks at the end.

set -euo pipefail

DRY_RUN=0
RUN_VERIFY=1

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --skip-verify) RUN_VERIFY=0 ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set. Export it first, e.g." >&2
  echo "  export DATABASE_URL=\"postgresql://coeqwal:coeqwal@localhost:5432/coeqwal_scenario\"" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found on PATH. Install PostgreSQL client tools." >&2
  echo "  sudo apt-get install -y postgresql-client" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SQL_ROOT="$REPO_ROOT/database/scripts/sql"

# ---------------------------------------------------------------------------
# File discovery
#
# Files are applied in this order:
#   1. database/scripts/sql/00_create_helper_functions.sql
#   2. database/scripts/sql/00_versioning/   (skipping verify scripts)
#   3. database/scripts/sql/migrations/      (sorted by name; migrations are
#                                             incremental and must run in order)
#   4. Top-level numbered migrations not in migrations/ (47..55 today).
#   5. database/scripts/sql/11_reservoir_statistics/
#      database/scripts/sql/12_mi_statistics/
#      database/scripts/sql/13_ag_statistics/
#      database/scripts/sql/14_channel_entity/
#   6. Verify scripts (09_verify_levelXX.sql) at the very end, unless
#      --skip-verify is passed.
#
# Where a file has a `_local` / `_cloud9` / `_from_s3` variant, the loader
# prefers `_local`. The other variants reach for AWS-side state and would
# fail on a host with no AWS credentials.
# ---------------------------------------------------------------------------

is_verify_file() {
  case "$(basename "$1")" in
    09_verify_level*.sql) return 0 ;;
    *) return 1 ;;
  esac
}

is_inspect_file() {
  case "$(basename "$1")" in
    inspect_*.sql|inspect*.sql) return 0 ;;
    *) return 1 ;;
  esac
}

prefer_local_variant() {
  # Given a list of paths on stdin, drop any `_cloud9` or `_from_s3` variant
  # if a matching `_local` variant exists in the same directory.
  awk '
    {
      paths[NR] = $0;
      base = $0;
      sub(/.*\//, "", base);
      sub(/\.sql$/, "", base);
      dir = $0;
      sub(/\/[^\/]+$/, "", dir);
      # Strip _local / _cloud9 / _from_s3 suffix
      stem = base;
      sub(/_local$/, "", stem);
      sub(/_cloud9$/, "", stem);
      sub(/_from_s3$/, "", stem);
      key = dir "/" stem;
      if (base ~ /_local$/) has_local[key] = 1;
    }
    END {
      for (i = 1; i <= NR; i++) {
        p = paths[i];
        base = p; sub(/.*\//, "", base); sub(/\.sql$/, "", base);
        dir  = p; sub(/\/[^\/]+$/, "", dir);
        stem = base;
        sub(/_local$/,   "", stem);
        sub(/_cloud9$/,  "", stem);
        sub(/_from_s3$/, "", stem);
        key = dir "/" stem;
        if (has_local[key] && (base ~ /_cloud9$/ || base ~ /_from_s3$/)) continue;
        print p;
      }
    }
  '
}

collect_layer_files() {
  # Print non-verify .sql files in a layer dir, in sorted order, with local
  # variants preferred over cloud9 / from_s3 variants.
  local layer_dir="$1"
  if [ ! -d "$layer_dir" ]; then return 0; fi
  find "$layer_dir" -maxdepth 1 -type f -name '*.sql' \
    | sort \
    | while read -r f; do
        if is_verify_file "$f"; then continue; fi
        if is_inspect_file "$f"; then continue; fi
        printf '%s\n' "$f"
      done \
    | prefer_local_variant
}

collect_toplevel_numbered() {
  # Numbered SQL files at the top of database/scripts/sql/ that are NOT
  # already present in migrations/ by basename. Captures 47..55 today;
  # picks up future top-level migrations automatically.
  local toplevel_files
  toplevel_files=$(find "$SQL_ROOT" -maxdepth 1 -type f -name '[0-9]*_*.sql' | sort)
  local migration_names
  migration_names=$(find "$SQL_ROOT/migrations" -maxdepth 1 -type f -name '*.sql' -exec basename {} \; 2>/dev/null | sort -u)
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    local b
    b=$(basename "$f")
    # Skip 00_create_helper_functions.sql; it is applied first explicitly.
    [ "$b" = "00_create_helper_functions.sql" ] && continue
    # Skip if migrations/ already owns this basename.
    if printf '%s\n' "$migration_names" | grep -Fxq "$b"; then
      continue
    fi
    printf '%s\n' "$f"
  done <<< "$toplevel_files"
}

collect_verify_files() {
  find "$SQL_ROOT" -type f -name '09_verify_level*.sql' | sort
}

# ---------------------------------------------------------------------------
# Build the apply list
# ---------------------------------------------------------------------------
APPLY_LIST=()

if [ -f "$SQL_ROOT/00_create_helper_functions.sql" ]; then
  APPLY_LIST+=( "$SQL_ROOT/00_create_helper_functions.sql" )
fi

while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/00_versioning")
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/migrations")
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_toplevel_numbered)
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/11_reservoir_statistics")
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/12_mi_statistics")
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/13_ag_statistics")
while IFS= read -r f; do APPLY_LIST+=( "$f" ); done < <(collect_layer_files "$SQL_ROOT/14_channel_entity")

VERIFY_LIST=()
if [ "$RUN_VERIFY" -eq 1 ]; then
  while IFS= read -r f; do VERIFY_LIST+=( "$f" ); done < <(collect_verify_files)
fi

# ---------------------------------------------------------------------------
# Dry-run output
# ---------------------------------------------------------------------------
if [ "$DRY_RUN" -eq 1 ]; then
  echo "Would apply ${#APPLY_LIST[@]} schema/seed file(s) and ${#VERIFY_LIST[@]} verify file(s):"
  echo ""
  echo "--- schema + seeds ---"
  for f in "${APPLY_LIST[@]}"; do
    printf '  %s\n' "${f#$REPO_ROOT/}"
  done
  if [ "${#VERIFY_LIST[@]}" -gt 0 ]; then
    echo ""
    echo "--- verify ---"
    for f in "${VERIFY_LIST[@]}"; do
      printf '  %s\n' "${f#$REPO_ROOT/}"
    done
  fi
  echo ""
  echo "DATABASE_URL: $(printf '%s\n' "$DATABASE_URL" | sed 's#:[^:@/]*@#:***@#g')"
  exit 0
fi

# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------
echo "Applying ${#APPLY_LIST[@]} schema/seed file(s) to $(printf '%s\n' "$DATABASE_URL" | sed 's#:[^:@/]*@#:***@#g')"
echo ""

apply_one() {
  local f="$1"
  echo "==> $f"
  psql "$DATABASE_URL" \
    --single-transaction \
    --variable=ON_ERROR_STOP=1 \
    --no-psqlrc \
    -f "$f"
}

for f in "${APPLY_LIST[@]}"; do
  if ! apply_one "$f"; then
    echo "" >&2
    echo "FAILED at $f" >&2
    echo "Fix the file or rerun with --dry-run to inspect the plan, then re-run this script." >&2
    exit 1
  fi
done

if [ "${#VERIFY_LIST[@]}" -gt 0 ]; then
  echo ""
  echo "Running verification scripts ..."
  for f in "${VERIFY_LIST[@]}"; do
    echo "==> $f"
    psql "$DATABASE_URL" \
      --single-transaction \
      --variable=ON_ERROR_STOP=1 \
      --no-psqlrc \
      -f "$f" || true
  done
fi

echo ""
echo "Done. Local DB is bootstrapped."
echo "Sanity check:"
echo "  psql \"\$DATABASE_URL\" -c \"\\dt\""
