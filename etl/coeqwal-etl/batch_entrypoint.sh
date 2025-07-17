#!/usr/bin/env bash
set -euo pipefail

echo "== COEQWAL scenario processing =="

: "${ZIP_BUCKET:?ZIP_BUCKET not set}"
: "${ZIP_KEY:?ZIP_KEY not set}"
: "${SCENARIO_ID:?SCENARIO_ID not set}"
: "${DDB_TABLE:=}"  # DynamoDB table for status updates

WORK=/tmp/work
mkdir -p "$WORK"
cd "$WORK"

echo "Downloading zip: s3://$ZIP_BUCKET/$ZIP_KEY"
aws s3 cp "s3://$ZIP_BUCKET/$ZIP_KEY" input.zip

echo "Unzipping..."
unzip -q input.zip -d extracted

echo "Locating DSS files..."
mapfile -t DSS_FILES < <(find extracted -type f -iname '*.dss' | sort)
if [ ${#DSS_FILES[@]} -eq 0 ]; then
  echo "ERROR: no DSS files found." >&2
  exit 1
fi

for f in "${DSS_FILES[@]}"; do
  lower=$(echo "$f" | tr '[:upper:]' '[:lower:]')
  role="calsim_output"
  [[ "$lower" == *sv* ]] && role="sv_input"
  outcsv="${SCENARIO_ID}__${role}.csv"

  echo "Processing DSS -> CSV: $f => $outcsv"
  python /app/python-code/dss_to_csv.py \
    --dss "$f" \
    --csv "$outcsv" \
    --type auto

  echo "Uploading artifacts to S3..."
  aws s3 cp "$f"      "s3://$ZIP_BUCKET/scenarios/${SCENARIO_ID}/dss/${SCENARIO_ID}__${role}.dss"
  aws s3 cp "$outcsv" "s3://$ZIP_BUCKET/scenarios/${SCENARIO_ID}/csv/${SCENARIO_ID}__${role}.csv"
done

# Optional DynamoDB status update (only if DDB_TABLE provided)
if [ -n "$DDB_TABLE" ]; then
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  aws dynamodb update-item \
    --table-name "$DDB_TABLE" \
    --key "{\"scenario_id\":{\"S\":\"$SCENARIO_ID\"}}" \
    --update-expression "SET #st = :s, last_update_ts = :t" \
    --expression-attribute-names '{"#st":"status"}' \
    --expression-attribute-values "{\":s\":{\"S\":\"csv_done\"},\":t\":{\"S\":\"'$ts'\"}}"
fi

echo "Done."