#!/usr/bin/env bash
set -euo pipefail

# Prefix applied to make targets:
# - prod: empty (default)
# - dev:  "dev-"
MAKE_TARGET_PREFIX="${MAKE_TARGET_PREFIX:-}"
MODE="${MODE:-}"
if [[ -n "${MODE}" && "${MODE}" == "dev" ]]; then
  MAKE_TARGET_PREFIX="dev-"
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

# Track background PIDs
pids=()

start_job() {
  local product="$1"
  local target="${MAKE_TARGET_PREFIX}index-${product}"

  echo "$(date) Start processing: ${target}"
  (
    if make "${target}" >/dev/null 2>&1; then
      echo "$(date) Successfully completed: ${target}"
    else
      echo "$(date) ERROR processing: ${target}"
    fi
  ) &
  pids+=("$!")
}

# Wait for the oldest PID in the queue, then shift the queue
wait_one() {
  local pid="${pids[0]}"
  # wait returns the exit code of that job
  wait "${pid}" || true
  # Remove first element (Bash 3.2-compatible)
  pids=("${pids[@]:1}")
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
