#!/usr/bin/env bash

# Batch entrypoint for COEQWAL ETL
# - Downloads ZIP from S3, unzips, classifies DSS files
# - Converts DSS -> CSV (SV + CalSim output)
# - (Optional) Validates against a reference CSV if provided in the S3 bucket
# - Uploads CSVs + manifest to S3

set -euo pipefail

# ----------------------------- Required env ------------------------------
: "${ZIP_BUCKET:?ZIP_BUCKET required}"
: "${ZIP_KEY:?ZIP_KEY required}"

# ----------------------------- Optional env ------------------------------
OUTPUT_PREFIX="${OUTPUT_PREFIX:-scenario/}"
JOB_ID="${AWS_BATCH_JOB_ID:-unknown}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCENARIO_ID_OVERRIDE="${SCENARIO_ID:-}"        # allow upstream override (optional)
VALIDATION_REF_CSV_KEY="${VALIDATION_REF_CSV_KEY:-}"  # e.g. scenario/s0020/verify/xxx.csv
ABS_TOL="${VALIDATION_ABS_TOL:-1e-06}"
REL_TOL="${VALIDATION_REL_TOL:-1e-06}"

WORKDIR=/tmp/work
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "[INFO] Batch job ${JOB_ID} starting."
echo "[INFO] Input: s3://${ZIP_BUCKET}/${ZIP_KEY}"

ZIP_BASENAME="$(basename "${ZIP_KEY}")"
ZIP_LOCAL="${WORKDIR}/input.zip"

# ----------------------------- Download & unzip --------------------------
aws s3 cp "s3://${ZIP_BUCKET}/${ZIP_KEY}" "${ZIP_LOCAL}"
unzip -q "${ZIP_LOCAL}" -d "${WORKDIR}/unzipped"

cd "${WORKDIR}/unzipped"
mapfile -t ALL_DSS < <(find . -type f -iname '*.dss' | sed 's|^\./||')
echo "[INFO] Found ${#ALL_DSS[@]} DSS file(s)."
printf '  - %s\n' "${ALL_DSS[@]}"

PATH_FILE="${WORKDIR}/dss_paths.txt"
printf '%s\n' "${ALL_DSS[@]}" > "${PATH_FILE}"

CLASSIFY_ENV="${WORKDIR}/classify.env"
python /app/python-code/classify_dss.py \
  --zip-base "${ZIP_BASENAME}" \
  --paths-file "${PATH_FILE}" \
  ${SCENARIO_ID_OVERRIDE:+--scenario-id "${SCENARIO_ID_OVERRIDE}"} \
  --out-env "${CLASSIFY_ENV}"

echo "[INFO] Classification:"
cat "${CLASSIFY_ENV}"

# shellcheck disable=SC1090
source "${CLASSIFY_ENV}"   # exports: SCENARIO_ID, SV_PATH, CALSIM_OUTPUT_PATH

# ----------------------------- Convert DSS -> CSV ------------------------
SV_CSV_LOCAL="${WORKDIR}/${SCENARIO_ID}_coeqwal_sv_input.csv"
CAL_CSV_LOCAL="${WORKDIR}/${SCENARIO_ID}_coeqwal_calsim_output.csv"
SV_BPARTS_FILE="${WORKDIR}/bparts_sv.txt"
CAL_BPARTS_FILE="${WORKDIR}/bparts_cal.txt"

sample_bparts_py () {
  local rel="$1" out_file="$2"
  python - <<'PY' "$rel" "$out_file"
import sys, os
from pydsstools.heclib.dss import HecDss
rel=sys.argv[1]; out=sys.argv[2]
if not rel:
    open(out,'w').close(); sys.exit(0)
path=os.path.join('.', rel)
if not os.path.isfile(path):
    open(out,'w').close(); sys.exit(0)
d=HecDss.Open(path)
try:
    pns=d.getPathnameList("/*/*/*/*/*/*/")
    seen=set(); outv=[]
    for pn in pns[:1000]:
        parts=pn.split('/')
        if len(parts)>=3:
            b=parts[2]
            if b not in seen:
                seen.add(b); outv.append(b)
    with open(out,'w') as f:
        f.write(",".join(outv[:10]))
finally:
    d.close()
PY
}

SV_UNIT_MISMATCHES=0
CAL_UNIT_MISMATCHES=0

# Helper: extract unit_mismatches from the METRICS JSON line in converter output
extract_unit_mismatches () {
  python -c "
import sys, json, re
for line in sys.stdin:
    m = re.search(r'METRICS\s+(\{.*\})', line)
    if m:
        print(json.loads(m.group(1)).get('unit_mismatches', 0))
        sys.exit(0)
print(0)
"
}

if [[ -n "${SV_PATH}" ]]; then
  echo "[INFO] Converting SV DSS: ${SV_PATH}"
  SV_CONVERT_LOG="${WORKDIR}/sv_convert.log"
  python /app/python-code/dss_to_csv.py \
    --dss "./${SV_PATH}" \
    --csv "${SV_CSV_LOCAL}" \
    --type sv_input --verify-units 2>&1 | tee "${SV_CONVERT_LOG}" \
    || echo "[WARN] SV convert error."
  SV_UNIT_MISMATCHES=$(extract_unit_mismatches < "${SV_CONVERT_LOG}")
  echo "[INFO] SV unit mismatches: ${SV_UNIT_MISMATCHES}"
  sample_bparts_py "${SV_PATH}" "${SV_BPARTS_FILE}"
fi

if [[ -n "${CALSIM_OUTPUT_PATH}" ]]; then
  echo "[INFO] Converting CalSim DSS: ${CALSIM_OUTPUT_PATH}"
  CAL_CONVERT_LOG="${WORKDIR}/cal_convert.log"
  python /app/python-code/dss_to_csv.py \
    --dss "./${CALSIM_OUTPUT_PATH}" \
    --csv "${CAL_CSV_LOCAL}" \
    --type calsim_output --verify-units 2>&1 | tee "${CAL_CONVERT_LOG}" \
    || echo "[WARN] CalSim convert error."
  CAL_UNIT_MISMATCHES=$(extract_unit_mismatches < "${CAL_CONVERT_LOG}")
  echo "[INFO] CalSim unit mismatches: ${CAL_UNIT_MISMATCHES}"
  sample_bparts_py "${CALSIM_OUTPUT_PATH}" "${CAL_BPARTS_FILE}"
fi

SV_B_SAMPLE="$(cat "${SV_BPARTS_FILE}" 2>/dev/null || echo "")"
CAL_B_SAMPLE="$(cat "${CAL_BPARTS_FILE}" 2>/dev/null || echo "")"

# ----------------------------- Optional validation -----------------------
VALIDATION_RESULT="skipped"
VALIDATION_TARGET="none"
VALIDATION_SUMMARY="No reference CSV supplied."

if [[ -n "${VALIDATION_REF_CSV_KEY}" ]]; then
  echo "[INFO] Validation CSV provided: s3://${ZIP_BUCKET}/${VALIDATION_REF_CSV_KEY}"
  REF_LOCAL="${WORKDIR}/reference.csv"
  if aws s3 cp "s3://${ZIP_BUCKET}/${VALIDATION_REF_CSV_KEY}" "${REF_LOCAL}"; then
    # Prefer CalSim output, then SV
    if [[ -f "${CAL_CSV_LOCAL}" ]]; then
      TARGET_LOCAL="${CAL_CSV_LOCAL}"
      VALIDATION_TARGET="calsim_output"
    elif [[ -f "${SV_CSV_LOCAL}" ]]; then
      TARGET_LOCAL="${SV_CSV_LOCAL}"
      VALIDATION_TARGET="sv_input"
    else
      TARGET_LOCAL=""
    fi

    if [[ -n "${TARGET_LOCAL:-}" ]]; then
      echo "[INFO] Validating reference CSV against ${VALIDATION_TARGET} CSV..."
      if [[ -f /app/python-code/validate_csvs.py ]]; then
        # Detailed validation reports
        VALIDATION_JSON_LOCAL="${WORKDIR}/validation_summary.json"
        VALIDATION_CSV_LOCAL="${WORKDIR}/validation_mismatches.csv"
        
        set +e
        VAL_OUT="$(
          python /app/python-code/validate_csvs.py \
            --ref "${REF_LOCAL}" \
            --file "${TARGET_LOCAL}" \
            --abs-tol "${ABS_TOL}" \
            --rel-tol "${REL_TOL}" \
            --out-json "${VALIDATION_JSON_LOCAL}" \
            --out-csv "${VALIDATION_CSV_LOCAL}" \
            --verbose \
            2>&1
        )"
        VAL_RC=$?
        set -e
        if [[ ${VAL_RC} -eq 0 ]]; then
          VALIDATION_RESULT="passed"
          VALIDATION_SUMMARY="Reference CSV matched (${VALIDATION_TARGET}). Detailed reports generated."
          echo "[INFO] Validation PASSED."
        else
          VALIDATION_RESULT="failed"
          VALIDATION_SUMMARY="${VAL_OUT}"
          echo "[WARN] Validation FAILED."
        fi
      else
        VALIDATION_RESULT="skipped_no_script"
        VALIDATION_SUMMARY="validate_csvs.py not present in container."
        echo "[INFO] Skipping validation: no validate_csvs.py"
      fi
    else
      VALIDATION_RESULT="skipped_no_targets"
      VALIDATION_SUMMARY="No produced CSVs to validate against."
      echo "[INFO] Skipping validation: no produced CSVs."
    fi
  else
    VALIDATION_RESULT="download_failed"
    VALIDATION_SUMMARY="Failed to download reference CSV."
  fi
fi

# --- right before you write the manifest: JSON-escape the summary text ---
VALIDATION_SUMMARY_JSON=$(python -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<< "$VALIDATION_SUMMARY")

# ----------------------------- Upload outputs ----------------------------
CSV_DIR="${OUTPUT_PREFIX}${SCENARIO_ID}/csv/"
VALIDATION_DIR="${OUTPUT_PREFIX}${SCENARIO_ID}/validation/"
SV_CSV_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_sv_input.csv"
CAL_CSV_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_calsim_output.csv"
MANIFEST_KEY="${OUTPUT_PREFIX}${SCENARIO_ID}/${SCENARIO_ID}_manifest.json"

# Upload main CSV outputs
[[ -f "${SV_CSV_LOCAL}"  ]] && aws s3 cp "${SV_CSV_LOCAL}"  "s3://${ZIP_BUCKET}/${SV_CSV_KEY}" || SV_CSV_KEY=""
[[ -f "${CAL_CSV_LOCAL}" ]] && aws s3 cp "${CAL_CSV_LOCAL}" "s3://${ZIP_BUCKET}/${CAL_CSV_KEY}" || CAL_CSV_KEY=""

# Upload validation reports
VALIDATION_JSON_KEY=""
VALIDATION_CSV_KEY=""
if [[ -f "${VALIDATION_JSON_LOCAL:-}" ]]; then
  VALIDATION_JSON_KEY="${VALIDATION_DIR}${SCENARIO_ID}_validation_summary.json"
  aws s3 cp "${VALIDATION_JSON_LOCAL}" "s3://${ZIP_BUCKET}/${VALIDATION_JSON_KEY}"
  echo "[INFO] Uploaded validation summary: s3://${ZIP_BUCKET}/${VALIDATION_JSON_KEY}"
fi
if [[ -f "${VALIDATION_CSV_LOCAL:-}" ]]; then
  VALIDATION_CSV_KEY="${VALIDATION_DIR}${SCENARIO_ID}_validation_mismatches.csv"
  aws s3 cp "${VALIDATION_CSV_LOCAL}" "s3://${ZIP_BUCKET}/${VALIDATION_CSV_KEY}"
  echo "[INFO] Uploaded validation mismatches: s3://${ZIP_BUCKET}/${VALIDATION_CSV_KEY}"
fi

# ----------------------------- Compute final status ----------------------
SV_DETECTED=$([[ -n "${SV_PATH}" ]] && echo true || echo false)
CAL_DETECTED=$([[ -n "${CALSIM_OUTPUT_PATH}" ]] && echo true || echo false)
SV_CSV_WRITTEN=$([[ -f "${SV_CSV_LOCAL}" ]] && echo true || echo false)
CAL_CSV_WRITTEN=$([[ -f "${CAL_CSV_LOCAL}" ]] && echo true || echo false)

if [[ -n "${SV_PATH}" && -n "${CALSIM_OUTPUT_PATH}" ]]; then
  FINAL_STATUS="SUCCEEDED"
elif [[ -n "${SV_PATH}" || -n "${CALSIM_OUTPUT_PATH}" ]]; then
  FINAL_STATUS="SUCCEEDED_PARTIAL"
else
  echo "[ERROR] No DSS candidates in expected folders; failing." >&2
  exit 1
fi

# ----------------------------- Manifest ----------------------------------
cat > "${WORKDIR}/manifest.json" <<MF
{
  "scenario_id": "${SCENARIO_ID}",
  "processed_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "job_id": "${JOB_ID}",
  "status": "${FINAL_STATUS}",
  "original_upload_key": "${ZIP_KEY}",
  "dss_files_detected": {
    "sv_input": "${SV_PATH}",
    "calsim_output": "${CALSIM_OUTPUT_PATH}"
  },
  "status_summary": {
    "sv_detected": ${SV_DETECTED},
    "calsim_detected": ${CAL_DETECTED},
    "sv_csv_written": ${SV_CSV_WRITTEN},
    "calsim_csv_written": ${CAL_CSV_WRITTEN}
  },
  "validation": {
    "reference_csv_key": "${VALIDATION_REF_CSV_KEY}",
    "target": "${VALIDATION_TARGET}",
    "result": "${VALIDATION_RESULT}",
    "summary": ${VALIDATION_SUMMARY_JSON},
    "detailed_reports": {
      "summary_json_key": "${VALIDATION_JSON_KEY}",
      "mismatches_csv_key": "${VALIDATION_CSV_KEY}"
    }
  },
  "unit_verification": {
    "sv_unit_mismatches": ${SV_UNIT_MISMATCHES},
    "calsim_unit_mismatches": ${CAL_UNIT_MISMATCHES}
  },
  "variable_sample_b_parts": {
    "sv_input": "${SV_B_SAMPLE}",
    "calsim_output": "${CAL_B_SAMPLE}"
  },
  "csv_outputs": {
    "sv_input_csv_key": "${SV_CSV_KEY}",
    "calsim_output_csv_key": "${CAL_CSV_KEY}"
  }
}
MF

aws s3 cp "${WORKDIR}/manifest.json" "s3://${ZIP_BUCKET}/${MANIFEST_KEY}"

echo "[INFO] Job ${JOB_ID} complete: ${FINAL_STATUS}"
exit 0