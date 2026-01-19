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

# Wait for the oldest PID in the queue, then shift the queue
wait_one() {
  local pid="${pids[0]}"
  # wait returns the exit code of that job
  wait "${pid}" || true
  pids=("${pids[@]:1}")  # Bash 3.2-compatible slice
}

for product in "${products[@]}"; do
  # If we're at max capacity, wait for one job to finish
  while [[ "${#pids[@]}" -ge "${max_jobs}" ]]; do
    wait_one
  done
  start_job "${product}"
done

# Wait for remaining jobs
while [[ "${#pids[@]}" -gt 0 ]]; do
  wait_one
done
