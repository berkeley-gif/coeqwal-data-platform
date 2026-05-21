#!/usr/bin/env bash
#
# Re-trigger DSS extraction for all scenarios by copying ZIPs back to ready/.
# The S3 event fires the Lambda, which submits the Batch job.
#
# Usage:
#   bash etl/ingestion/tools/retrigger_extraction.sh              # list what would be copied
#   bash etl/ingestion/tools/retrigger_extraction.sh --go         # actually do it
#   bash etl/ingestion/tools/retrigger_extraction.sh --go s0020   # single scenario

set -euo pipefail

BUCKET="coeqwal-model-run"
GO=false
FILTER=""

for arg in "$@"; do
  case "$arg" in
    --go) GO=true ;;
    s[0-9]*) FILTER="$arg" ;;
  esac
done

echo "Scanning s3://${BUCKET}/scenario/*/run/*.zip ..."
echo

ZIPS=$(aws s3 ls "s3://${BUCKET}/scenario/" --recursive \
  | grep '/run/.*\.zip$' \
  | awk '{print $NF}')

if [[ -z "$ZIPS" ]]; then
  echo "No ZIP files found."
  exit 0
fi

COUNT=0
JOBS=()

while IFS= read -r key; do
  filename=$(basename "$key")
  sid=$(echo "$filename" | grep -oE '^s[0-9]{4}')

  if [[ -n "$FILTER" && "$sid" != "$FILTER" ]]; then
    continue
  fi

  JOBS+=("$key")
  COUNT=$((COUNT + 1))
  echo "  ${sid}: ${key} -> ready/${filename}"
done <<< "$ZIPS"

echo
echo "Found ${COUNT} scenario ZIP(s)."

if [[ "$GO" != true ]]; then
  echo
  echo "Dry run. Add --go to submit."
  exit 0
fi

echo
read -rp "Copy ${COUNT} ZIP(s) to ready/ and trigger extraction? [y/N] " confirm
if [[ "$confirm" != "y" ]]; then
  echo "Aborted."
  exit 0
fi

echo
for key in "${JOBS[@]}"; do
  filename=$(basename "$key")
  echo "  Copying ${filename} to ready/ ..."
  aws s3 cp "s3://${BUCKET}/${key}" "s3://${BUCKET}/ready/${filename}"
  sleep 2
done

echo
echo "Done. ${COUNT} ZIP(s) copied to ready/. Lambda will trigger batch jobs."
echo "Monitor with:"
echo "  aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNABLE"
echo "  aws batch list-jobs --job-queue coeqwal-dss-queue --job-status RUNNING"
