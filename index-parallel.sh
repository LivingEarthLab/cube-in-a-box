#!/usr/bin/env bash

products=(
  index-sentinel-2-l2a
  index-io-lulc-annual-v02
  index-nasadem
  index-ls45_c2l2_sp
  index-ls7_c2l2_sp
  index-ls89_c2l2_sp
  index-sentinel-1-rtc
)

max_jobs=4

start_job() {
  local product=$1
  echo "$(date) Start processing: $product"
   (make "$product" > /dev/null 2>&1 && \
    echo "$(date) Successfully completed: $product" || \
    echo "$(date) ERROR processing: $product") &
}

# Process all products with concurrency control
for product in "${products[@]}"; do
  # Wait for a job slot if we're at max capacity
  if [[ $(jobs -r | wc -l) -ge $max_jobs ]]; then
    wait -n  # Wait for next job to finish (Bash 4.3+)
  fi
  start_job "$product"
done

# Wait for any remaining jobs
wait
