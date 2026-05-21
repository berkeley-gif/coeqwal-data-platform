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

# Which DSS sides to extract. Default is both; set to "sv" or "calsim" to
# extract only one. Used by `reextract_all_scenarios.py --sv-only` /
# `--dv-only` to skip the side the operator does not need.
EXTRACT_TARGETS="${EXTRACT_TARGETS:-sv,calsim}"

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

# Apply EXTRACT_TARGETS gating: blank out the path for any side the operator
# asked us to skip. The downstream `if [[ -n "${SV_PATH}" ]]` / `if [[ -n
# "${CALSIM_OUTPUT_PATH}" ]]` guards then skip that side cleanly. We also
# remember the original detection state so the manifest can distinguish
# "intentionally skipped" from "DSS not found in the ZIP".
SV_DETECTED_RAW=$([[ -n "${SV_PATH}" ]] && echo true || echo false)
CAL_DETECTED_RAW=$([[ -n "${CALSIM_OUTPUT_PATH}" ]] && echo true || echo false)
WANT_SV=true
WANT_CAL=true
if [[ ",${EXTRACT_TARGETS}," != *",sv,"* ]]; then
  WANT_SV=false
  SV_PATH=""
  echo "[INFO] EXTRACT_TARGETS=${EXTRACT_TARGETS}: skipping SV extraction."
fi
if [[ ",${EXTRACT_TARGETS}," != *",calsim,"* ]]; then
  WANT_CAL=false
  CALSIM_OUTPUT_PATH=""
  echo "[INFO] EXTRACT_TARGETS=${EXTRACT_TARGETS}: skipping CalSim extraction."
fi
if [[ "${WANT_SV}" != true && "${WANT_CAL}" != true ]]; then
  echo "[ERROR] EXTRACT_TARGETS=${EXTRACT_TARGETS}: nothing to do." >&2
  exit 1
fi

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

SV_CONVERT_RC=0
CAL_CONVERT_RC=0

if [[ -n "${SV_PATH}" ]]; then
  echo "[INFO] Converting SV DSS: ${SV_PATH}"
  SV_CONVERT_LOG="${WORKDIR}/sv_convert.log"
  set +e
  python /app/python-code/dss_to_csv.py \
    --dss "./${SV_PATH}" \
    --csv "${SV_CSV_LOCAL}" \
    --type sv_input --verify-units 2>&1 | tee "${SV_CONVERT_LOG}"
  SV_CONVERT_RC=${PIPESTATUS[0]}
  set -e
  if [[ ${SV_CONVERT_RC} -ne 0 ]]; then
    echo "[ERROR] SV conversion failed with exit code ${SV_CONVERT_RC}"
  fi
  SV_UNIT_MISMATCHES=$(extract_unit_mismatches < "${SV_CONVERT_LOG}")
  echo "[INFO] SV unit mismatches: ${SV_UNIT_MISMATCHES}"
  sample_bparts_py "${SV_PATH}" "${SV_BPARTS_FILE}"
fi

if [[ -n "${CALSIM_OUTPUT_PATH}" ]]; then
  echo "[INFO] Converting CalSim DSS: ${CALSIM_OUTPUT_PATH}"
  CAL_CONVERT_LOG="${WORKDIR}/cal_convert.log"
  set +e
  python /app/python-code/dss_to_csv.py \
    --dss "./${CALSIM_OUTPUT_PATH}" \
    --csv "${CAL_CSV_LOCAL}" \
    --type calsim_output --verify-units 2>&1 | tee "${CAL_CONVERT_LOG}"
  CAL_CONVERT_RC=${PIPESTATUS[0]}
  set -e
  if [[ ${CAL_CONVERT_RC} -ne 0 ]]; then
    echo "[ERROR] CalSim conversion failed with exit code ${CAL_CONVERT_RC}"
  fi
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

# Inline the two interesting counts from validate_csvs.py's local summary
# file so we can drop the separate <id>_validation_summary.json S3 object.
# Per-row debug data still goes out as <id>_validation_mismatches.csv below.
MISMATCH_COLUMNS=0
MISMATCH_CELLS=0
if [[ -f "${VALIDATION_JSON_LOCAL:-}" ]]; then
  read -r MISMATCH_COLUMNS MISMATCH_CELLS <<< "$(python -c '
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    print(int(d.get("mismatch_columns", 0)), int(d.get("mismatch_cells", 0)))
except Exception:
    print("0 0")
' "${VALIDATION_JSON_LOCAL}")"
fi

# ----------------------------- Upload outputs ----------------------------
CSV_DIR="${OUTPUT_PREFIX}${SCENARIO_ID}/csv/"
VALIDATION_DIR="${OUTPUT_PREFIX}${SCENARIO_ID}/validation/"
SV_CSV_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_sv_input.csv"
CAL_CSV_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_calsim_output.csv"
MANIFEST_KEY="${OUTPUT_PREFIX}${SCENARIO_ID}/${SCENARIO_ID}_manifest.json"

# Upload main CSV outputs and unit map sidecars
[[ -f "${SV_CSV_LOCAL}"  ]] && aws s3 cp "${SV_CSV_LOCAL}"  "s3://${ZIP_BUCKET}/${SV_CSV_KEY}" || SV_CSV_KEY=""
[[ -f "${CAL_CSV_LOCAL}" ]] && aws s3 cp "${CAL_CSV_LOCAL}" "s3://${ZIP_BUCKET}/${CAL_CSV_KEY}" || CAL_CSV_KEY=""

SV_UNITS_KEY=""
CAL_UNITS_KEY=""
if [[ -f "${SV_CSV_LOCAL}.units.json" ]]; then
  SV_UNITS_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_sv_input.csv.units.json"
  aws s3 cp "${SV_CSV_LOCAL}.units.json" "s3://${ZIP_BUCKET}/${SV_UNITS_KEY}"
  echo "[INFO] Uploaded SV unit map: s3://${ZIP_BUCKET}/${SV_UNITS_KEY}"
fi
if [[ -f "${CAL_CSV_LOCAL}.units.json" ]]; then
  CAL_UNITS_KEY="${CSV_DIR}${SCENARIO_ID}_coeqwal_calsim_output.csv.units.json"
  aws s3 cp "${CAL_CSV_LOCAL}.units.json" "s3://${ZIP_BUCKET}/${CAL_UNITS_KEY}"
  echo "[INFO] Uploaded CalSim unit map: s3://${ZIP_BUCKET}/${CAL_UNITS_KEY}"
fi

# Upload validation reports. The per-column summary (mismatch_columns,
# mismatch_cells) is inlined into the manifest above, so we no longer
# upload validation_summary.json as a separate S3 object. The per-row
# mismatches CSV is the only artifact rich enough to debug a failure and
# stays on its own key.
VALIDATION_CSV_KEY=""
if [[ -f "${VALIDATION_CSV_LOCAL:-}" ]]; then
  VALIDATION_CSV_KEY="${VALIDATION_DIR}${SCENARIO_ID}_validation_mismatches.csv"
  aws s3 cp "${VALIDATION_CSV_LOCAL}" "s3://${ZIP_BUCKET}/${VALIDATION_CSV_KEY}"
  echo "[INFO] Uploaded validation mismatches: s3://${ZIP_BUCKET}/${VALIDATION_CSV_KEY}"
fi

# ----------------------------- Compute final status ----------------------
# SV_DETECTED / CAL_DETECTED reflect what the ZIP actually contained, taken
# before EXTRACT_TARGETS gating blanked the paths. Use the *_RAW values so
# the manifest tells the truth about the upload, and a separate
# WANT_SV / WANT_CAL pair tells the truth about what we asked to extract.
SV_DETECTED="${SV_DETECTED_RAW}"
CAL_DETECTED="${CAL_DETECTED_RAW}"
SV_CSV_WRITTEN=$([[ -f "${SV_CSV_LOCAL}" ]] && echo true || echo false)
CAL_CSV_WRITTEN=$([[ -f "${CAL_CSV_LOCAL}" ]] && echo true || echo false)

# Refuse to run if the ZIP yielded no DSS candidates at all. EXTRACT_TARGETS
# is already enforced higher up, so reaching here with both raw flags false
# means the upload itself was empty.
if [[ "${SV_DETECTED_RAW}" != true && "${CAL_DETECTED_RAW}" != true ]]; then
  echo "[ERROR] No DSS candidates in expected folders; failing." >&2
  exit 1
fi

# A failure means: we asked to extract this side, the DSS was present, and
# the CSV did not get written. Intentional skips via EXTRACT_TARGETS are
# not failures.
FAILURES=0
if [[ "${WANT_SV}" == true && -n "${SV_PATH}" && ! -f "${SV_CSV_LOCAL}" ]]; then
  echo "[ERROR] SV DSS detected but CSV was not produced."
  FAILURES=$((FAILURES + 1))
fi
if [[ "${WANT_CAL}" == true && -n "${CALSIM_OUTPUT_PATH}" && ! -f "${CAL_CSV_LOCAL}" ]]; then
  echo "[ERROR] CalSim output DSS detected but CSV was not produced."
  FAILURES=$((FAILURES + 1))
fi

# SUCCEEDED only when every requested side produced its CSV. A partial
# extract by design (--sv-only / --dv-only) is therefore SUCCEEDED, not
# SUCCEEDED_PARTIAL. SUCCEEDED_PARTIAL is reserved for the case where both
# sides were requested but only one produced a CSV.
SV_OK=true
CAL_OK=true
[[ "${WANT_SV}" == true && ! -f "${SV_CSV_LOCAL}" ]] && SV_OK=false
[[ "${WANT_CAL}" == true && ! -f "${CAL_CSV_LOCAL}" ]] && CAL_OK=false

if [[ ${FAILURES} -gt 0 ]]; then
  FINAL_STATUS="FAILED"
elif [[ "${SV_OK}" == true && "${CAL_OK}" == true ]]; then
  FINAL_STATUS="SUCCEEDED"
else
  FINAL_STATUS="SUCCEEDED_PARTIAL"
fi

# ----------------------------- Manifest ----------------------------------
cat > "${WORKDIR}/manifest.json" <<MF
{
  "scenario_id": "${SCENARIO_ID}",
  "processed_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "job_id": "${JOB_ID}",
  "status": "${FINAL_STATUS}",
  "extract_targets": "${EXTRACT_TARGETS}",
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
    "mismatch_columns": ${MISMATCH_COLUMNS},
    "mismatch_cells": ${MISMATCH_CELLS},
    "mismatches_csv_key": "${VALIDATION_CSV_KEY}"
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

if [[ "${FINAL_STATUS}" == "FAILED" ]]; then
  echo "[ERROR] Exiting with error because one or more conversions failed."
  exit 1
fi
exit 0