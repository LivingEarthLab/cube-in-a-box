#!/usr/bin/env bash
set -euo pipefail

# Mode handling:
# - prod: default
# - dev:  pass MODE=dev to make
MODE="${MODE:-prod}"
if [[ "${MODE}" != "dev" && "${MODE}" != "prod" ]]; then
  echo "ERROR: MODE must be 'dev' or 'prod' (got: ${MODE})" >&2
  exit 2
fi

products=(
  sentinel-2-l2a
  io-lulc-annual-v02
  nasadem
  ls45_c2l2_sp
  ls7_c2l2_sp
  ls89_c2l2_sp
  sentinel-1-rtc
)

max_jobs="${MAX_JOBS:-4}"

# Track background PIDs (Bash 3.2-compatible)
pids=()

start_job() {
  local product="$1"
  local target="index-${product}"

  echo "$(date) Start processing: ${target} (MODE=${MODE})"
  (
    if [[ "${MODE}" == "dev" ]]; then
      make "${target}" MODE=dev >/dev/null 2>&1
    else
      make "${target}" >/dev/null 2>&1
    fi

    echo "$(date) Successfully completed: ${target} (MODE=${MODE})"
  ) || echo "$(date) ERROR processing: ${target} (MODE=${MODE})" &

  pids+=("$!")
}

# Check running PIDs and remove finished ones from the list
check_jobs() {
  local new_pids=()
  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      new_pids+=("${pid}")
    fi
  done
  pids=("${new_pids[@]}")
}

# Wait until there is at least one free slot
wait_for_free_slot() {
  while true; do
    check_jobs
    if [[ "${#pids[@]}" -lt "${max_jobs}" ]]; then
      return
    fi
    sleep 1
  done
}

for product in "${products[@]}"; do
  wait_for_free_slot
  start_job "${product}"
done

# Wait for remaining jobs
for pid in "${pids[@]}"; do
  wait "${pid}" || true
done
