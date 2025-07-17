#!/usr/bin/env bash
set -euo pipefail

# Required env (Batch container overrides from Lambda)
: "${ZIP_BUCKET:?ZIP_BUCKET required}"
: "${ZIP_KEY:?ZIP_KEY required}"

# Optional env / defaults
DDB_TABLE="${DDB_TABLE:-coeqwal_scenarios}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-scenario/}"
JOB_ID="${AWS_BATCH_JOB_ID:-unknown}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCENARIO_ID_OVERRIDE="${SCENARIO_ID:-}"  # allow upstream override (optional)

WORKDIR=/tmp/work
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "[INFO] Batch job ${JOB_ID} starting."
echo "[INFO] Input: s3://${ZIP_BUCKET}/${ZIP_KEY}"

ZIP_BASENAME="$(basename "${ZIP_KEY}")"
ZIP_LOCAL="${WORKDIR}/input.zip"

# ------------------------------------------------------------------
# Dynamo helper (best-effort; non-fatal)
# ------------------------------------------------------------------
ddb_update () {
  local status="$1"; shift
  local epoch="$(date +%s)"
  local ue="SET #s=:s, updated=:u"
  local names='{"#s":"status"}'
  local vals="{\":s\":{\"S\":\"${status}\"},\":u\":{\"N\":\"${epoch}\"}"
  while (( "$#" )); do
    local kv="$1"; shift
    local k="${kv%%=*}"; local v="${kv#*=}"
    ue="${ue}, ${k}=:${k}"
    vals="${vals},\":${k}\":{\"S\":\"${v}\"}"
  done
  vals="${vals}}"
  aws dynamodb update-item \
    --region "${AWS_REGION}" \
    --table-name "${DDB_TABLE}" \
    --key "{\"scenario_id\":{\"S\":\"${SCENARIO_ID:-unknown}\"}}" \
    --update-expression "${ue}" \
    --expression-attribute-names "${names}" \
    --expression-attribute-values "${vals}" \
    >/dev/null 2>&1 || echo "[WARN] DDB update failed (${status})."
}

# ------------------------------------------------------------------
# Download & unzip
# ------------------------------------------------------------------
aws s3 cp "s3://${ZIP_BUCKET}/${ZIP_KEY}" "${ZIP_LOCAL}"
unzip -q "${ZIP_LOCAL}" -d "${WORKDIR}/unzipped"

cd "${WORKDIR}/unzipped"
mapfile -t ALL_DSS < <(find . -type f -iname '*.dss' | sed 's|^\./||')
echo "[INFO] Found ${#ALL_DSS[@]} DSS file(s)."
printf '  - %s\n' "${ALL_DSS[@]}"

PATH_FILE="${WORKDIR}/dss_paths.txt"
printf '%s\n' "${ALL_DSS[@]}" >"${PATH_FILE}"

CLASSIFY_ENV="${WORKDIR}/classify.env"
python /app/python-code/classify_dss.py \
  --zip-base "${ZIP_BASENAME}" \
  --paths-file "${PATH_FILE}" \
  ${SCENARIO_ID_OVERRIDE:+--scenario-id "${SCENARIO_ID_OVERRIDE}"} \
  --out-env "${CLASSIFY_ENV}"

echo "[INFO] Classification:"
cat "${CLASSIFY_ENV}"

# shellcheck disable=SC1090
source "${CLASSIFY_ENV}"   # SCENARIO_ID, SV_PATH, CAL_PATH

# Mark RUNNING (Lambda wrote SUBMITTED earlier)
SCENARIO_ID="${SCENARIO_ID}" ddb_update "RUNNING" "job_id=${JOB_ID}" "zip_key=${ZIP_KEY}"

# ------------------------------------------------------------------
# Convert DSS -> CSV + sample B-parts
# ------------------------------------------------------------------
SV_CSV_LOCAL="${WORKDIR}/${SCENARIO_ID}_sv_input.csv"
CAL_CSV_LOCAL="${WORKDIR}/${SCENARIO_ID}_calsim_output.csv"
SV_BPARTS_FILE="${WORKDIR}/bparts_sv.txt"
CAL_BPARTS_FILE="${WORKDIR}/bparts_cal.txt"

sample_bparts_py () {
  local rel="$1" out_file="$2"
  python <<'PY' "$rel" "$out_file"
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

if [[ -n "${SV_PATH}" ]]; then
  echo "[INFO] Converting SV DSS: ${SV_PATH}"
  python /app/python-code/dss_to_csv.py \
    --dss "./${SV_PATH}" \
    --csv "${SV_CSV_LOCAL}" \
    --type sv_input || echo "[WARN] SV convert error."
  sample_bparts_py "${SV_PATH}" "${SV_BPARTS_FILE}"
fi

if [[ -n "${CAL_PATH}" ]]; then
  echo "[INFO] Converting CalSim DSS: ${CAL_PATH}"
  python /app/python-code/dss_to_csv.py \
    --dss "./${CAL_PATH}" \
    --csv "${CAL_CSV_LOCAL}" \
    --type calsim_output || echo "[WARN] CalSim convert error."
  sample_bparts_py "${CAL_PATH}" "${CAL_BPARTS_FILE}"
fi

SV_B_SAMPLE="$(cat "${SV_BPARTS_FILE}" 2>/dev/null || echo "")"
CAL_B_SAMPLE="$(cat "${CAL_BPARTS_FILE}" 2>/dev/null || echo "")"

# ------------------------------------------------------------------
# Upload outputs
# ------------------------------------------------------------------
SCEN_OUT_DIR="${OUTPUT_PREFIX}${SCENARIO_ID}/"
SV_CSV_KEY="${SCEN_OUT_DIR}${SCENARIO_ID}_sv_input.csv"
CAL_CSV_KEY="${SCEN_OUT_DIR}${SCENARIO_ID}_calsim_output.csv"
MANIFEST_KEY="${SCEN_OUT_DIR}${SCENARIO_ID}_manifest.json"
SRC_ZIP_KEY="${SCEN_OUT_DIR}source/${ZIP_BASENAME}"

[[ -f "${SV_CSV_LOCAL}"  ]] && aws s3 cp "${SV_CSV_LOCAL}"  "s3://${ZIP_BUCKET}/${SV_CSV_KEY}" || SV_CSV_KEY=""
[[ -f "${CAL_CSV_LOCAL}" ]] && aws s3 cp "${CAL_CSV_LOCAL}" "s3://${ZIP_BUCKET}/${CAL_CSV_KEY}" || CAL_CSV_KEY=""
aws s3 cp "s3://${ZIP_BUCKET}/${ZIP_KEY}" "s3://${ZIP_BUCKET}/${SRC_ZIP_KEY}"

# ------------------------------------------------------------------
# Manifest
# ------------------------------------------------------------------
cat >"${WORKDIR}/manifest.json" <<MF
{
  "scenario_id": "${SCENARIO_ID}",
  "processed_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "job_id": "${JOB_ID}",
  "original_upload_key": "${ZIP_KEY}",
  "source_zip_key": "${SRC_ZIP_KEY}",
  "dss_files_detected": {
    "sv_input": "${SV_PATH}",
    "calsim_output": "${CAL_PATH}"
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

# ------------------------------------------------------------------
# Final status (WARN partial)
# ------------------------------------------------------------------
if [[ -n "${SV_PATH}" && -n "${CAL_PATH}" ]]; then
  FINAL_STATUS="SUCCEEDED"
elif [[ -n "${SV_PATH}" || -n "${CAL_PATH}" ]]; then
  FINAL_STATUS="SUCCEEDED_PARTIAL"
else
  ddb_update "FAILED" "job_id=${JOB_ID}" "zip_key=${ZIP_KEY}"
  echo "[ERROR] No DSS candidates in expected folders; failing." >&2
  exit 1
fi

ddb_update "${FINAL_STATUS}" \
  "job_id=${JOB_ID}" \
  "zip_key=${ZIP_KEY}" \
  "sv_csv_key=${SV_CSV_KEY}" \
  "calsim_csv_key=${CAL_CSV_KEY}" \
  "manifest_key=${MANIFEST_KEY}"

echo "[INFO] Job ${JOB_ID} complete: ${FINAL_STATUS}"
exit 0