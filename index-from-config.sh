#!/usr/bin/env bash
# Index ODC datasets from STAC using a YAML configuration file.
set -euo pipefail

INDEX_CONFIG="${INDEX_CONFIG:-config/indexation/default.yaml}"
BBOX="${BBOX:-25,20,35,30}"
DATETIME="${DATETIME:-2021-12-01/2021-12-31}"
MODE="${MODE:-prod}"
PROJECT="${PROJECT:-cube-in-a-box}"
PARALLELISM="${PARALLELISM:-}"
TIME_INDEX="${TIME_INDEX:-}"

timing_enabled=0
timing_dir=""
index_start_epoch=0
log_dir=""
log_lock=""

if [[ "${TIME_INDEX}" == "1" || "${TIME_INDEX}" == "true" ]]; then
  timing_enabled=1
  timing_dir="$(mktemp -d)"
fi

cleanup_temp_dirs() {
  if [[ -n "${timing_dir}" && -d "${timing_dir}" ]]; then
    rm -rf "${timing_dir}"
  fi
  if [[ -n "${log_dir}" && -d "${log_dir}" ]]; then
    rm -rf "${log_dir}"
  fi
  if [[ -n "${log_lock}" && -f "${log_lock}" ]]; then
    rm -f "${log_lock}"
  fi
}
trap cleanup_temp_dirs EXIT

log_line() {
  if [[ -n "${log_lock}" ]]; then
    {
      flock -x 200
      printf '%s\n' "$*"
    } 200>"${log_lock}"
  else
    printf '%s\n' "$*"
  fi
}

log_block() {
  if [[ -n "${log_lock}" ]]; then
    {
      flock -x 200
      cat
    } 200>"${log_lock}"
  else
    cat
  fi
}

init_parallel_logging() {
  if [[ "${parallelism}" != "1" ]]; then
    log_dir="$(mktemp -d)"
    log_lock="$(mktemp)"
  fi
}

format_duration() {
  local total=$1
  printf '%dm %02ds' $((total / 60)) $((total % 60))
}

record_job_timing() {
  local product_id="$1"
  local elapsed="$2"
  local status="$3"
  if (( timing_enabled )); then
    printf '%s|%s\n' "${elapsed}" "${status}" > "${timing_dir}/${product_id}"
  fi
}

print_timing_summary() {
  local wall_elapsed=$(( $(date +%s) - index_start_epoch ))
  local -a ids=()

  shopt -s nullglob
  for f in "${timing_dir}"/*; do
    ids+=("$(basename "${f}")")
  done
  shopt -u nullglob

  mapfile -t ids < <(printf '%s\n' "${ids[@]}" | sort)

  {
    printf '\n%s\n' "$(date) Indexation timing (config=${INDEX_CONFIG}, MODE=${MODE}, parallelism=${parallelism}):"
    local id elapsed status
    for id in "${ids[@]}"; do
      IFS='|' read -r elapsed status < "${timing_dir}/${id}"
      printf '  %-24s %s (%s)\n' "${id}:" "$(format_duration "${elapsed}")" "${status}"
    done
    printf '  %-24s %s\n' "Total wall time:" "$(format_duration "${wall_elapsed}")"
  } | log_block
}

finish_indexing() {
  local failed=$1

  if (( timing_enabled )); then
    print_timing_summary
  fi

  if (( failed )); then
    log_line "$(date) One or more indexing jobs failed (config=${INDEX_CONFIG}, MODE=${MODE})."
    exit 1
  fi

  log_line "$(date) All indexing jobs completed successfully (config=${INDEX_CONFIG}, MODE=${MODE})."
  exit 0
}

if [[ "${MODE}" != "dev" && "${MODE}" != "prod" ]]; then
  echo "ERROR: MODE must be 'dev' or 'prod' (got: ${MODE})" >&2
  exit 2
fi

if [[ ! -f "${INDEX_CONFIG}" ]]; then
  echo "ERROR: INDEX_CONFIG not found: ${INDEX_CONFIG}" >&2
  exit 1
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${repo_root}"

if [[ "${MODE}" == "dev" ]]; then
  DC=(env COMPOSE_PROJECT_NAME="${PROJECT}-dev" docker compose -p "${PROJECT}-dev" -f docker-compose.yml -f docker-compose.dev.yml)
else
  DC=(env COMPOSE_PROJECT_NAME="${PROJECT}" docker compose -p "${PROJECT}" -f docker-compose.yml)
fi

mapfile -t job_lines < <(PYTHONPATH="${repo_root}" python3 scripts/index_config.py jobs "${INDEX_CONFIG}")

if [[ "${#job_lines[@]}" -eq 0 ]]; then
  echo "ERROR: No products to index in ${INDEX_CONFIG}" >&2
  exit 1
fi

if [[ -z "${PARALLELISM}" ]]; then
  parallelism="$(PYTHONPATH="${repo_root}" python3 scripts/index_config.py parallelism "${INDEX_CONFIG}")"
else
  parallelism="${PARALLELISM}"
fi

init_parallel_logging

declare -a pids
pids=()

# Drop known STAC/index chatter; keep Added/error lines and anything unexpected.
filter_job_output() {
  # grep exits 1 when every line is filtered; treat that as success.
  grep -Ev 'API did not return the number of items|Indexing from STAC API' || [[ $? -eq 1 ]]
}

# Classify known STAC/catalog failures and print a short message instead of a traceback.
report_job_failure() {
  local product_id="$1"
  local log_file="$2"
  local reason="" hint=""

  if [[ ! -f "${log_file}" ]]; then
    log_line "ERROR: ${product_id}: indexing failed (no job log captured)"
    return
  fi

  if grep -qiE '504[[:space:]]+Gateway Time-?out' "${log_file}"; then
    reason="STAC catalog returned 504 Gateway Time-out"
    hint=1
  elif grep -qiE '502[[:space:]]+Bad Gateway' "${log_file}"; then
    reason="STAC catalog returned 502 Bad Gateway"
    hint=1
  elif grep -qiE '503[[:space:]]+Service Unavailable' "${log_file}"; then
    reason="STAC catalog returned 503 Service Unavailable"
    hint=1
  elif grep -qiE 'Read timed out|timed?[[:space:]]*out' "${log_file}"; then
    reason="STAC catalog request timed out"
    hint=1
  elif grep -qiE 'RemoteDisconnected|ConnectionResetError|Connection refused|ConnectionError|Connection aborted' "${log_file}"; then
    reason="STAC catalog connection failed"
    hint=1
  elif grep -qi 'APIError' "${log_file}"; then
    reason="STAC catalog APIError"
    hint=1
  fi

  if [[ -n "${reason}" ]]; then
    log_line "ERROR: ${product_id}: ${reason}"
    if [[ -n "${hint}" ]]; then
      log_line "Hint: transient catalog issue; retry with make index-serie or a one-product INDEX_CONFIG"
    fi
    return
  fi

  log_line "ERROR: ${product_id}: indexing failed"
  log_line "---- last log lines ----"
  tail -n 15 "${log_file}" | log_block || true
  log_line "---- end log ----"
}

run_job_body() {
  local product_id="$1"
  local optional="$2"
  local cmd="$3"
  shift 3
  local -a env_args=("$@")

  local job_start elapsed status=ok rc=0
  local job_log="" own_log=0 docker_rc=0
  job_start=$(date +%s)

  if [[ -n "${log_dir}" ]]; then
    job_log="${log_dir}/${product_id}.log"
  else
    job_log="$(mktemp)"
    own_log=1
  fi

  # Quiet compose lifecycle + start.sh banners + pystac UserWarnings
  env_args+=(-e "START_QUIET=1" -e "PYTHONWARNINGS=ignore")

  set +e
  "${DC[@]}" --progress quiet --profile init run --rm "${env_args[@]}" \
    jupyter bash -lc "${cmd}" >"${job_log}" 2>&1
  docker_rc=$?
  set -e

  if [[ "${optional}" == "True" ]]; then
    if [[ "${docker_rc}" -ne 0 ]]; then
      status=skipped
      report_job_failure "${product_id}" "${job_log}"
    else
      filter_job_output < "${job_log}" | log_block || true
    fi
  elif [[ "${docker_rc}" -ne 0 ]]; then
    status=failed
    rc=1
    report_job_failure "${product_id}" "${job_log}"
  else
    filter_job_output < "${job_log}" | log_block || true
  fi

  if (( own_log )); then
    rm -f "${job_log}"
  fi

  elapsed=$(($(date +%s) - job_start))
  record_job_timing "${product_id}" "${elapsed}" "${status}"
  return "${rc}"
}

run_job() {
  local job_json="$1"
  local product_id catalog_href auth collection optional rename_product use_datetime query

  # Parse all fields in one python call; emit NUL-delimited values to survive
  # spaces/newlines in query JSON.
  {
    IFS= read -r -d '' product_id
    IFS= read -r -d '' catalog_href
    IFS= read -r -d '' auth
    IFS= read -r -d '' collection
    IFS= read -r -d '' optional
    IFS= read -r -d '' rename_product
    IFS= read -r -d '' use_datetime
    IFS= read -r -d '' query
  } < <(python3 -c '
import json, sys
job = json.loads(sys.argv[1])
fields = ["id", "catalog_href", "auth", "collection", "optional",
          "rename_product", "datetime", "query"]
sys.stdout.write("\0".join(str(job.get(f, "")) for f in fields))
' "${job_json}")

  # AlphaEarth datasets are not served through a STAC API - dispatch to a
  # dedicated indexer, but keep it on the same execution path (container,
  # logging, timing, parallelism) as every other product.
  if [[ "${product_id}" == "aef_annual" ]]; then
    local -a aef_args=(
      python3 scripts/index_aef.py
      "--bbox=${BBOX}"
      "--datetime=${DATETIME}"
      "--product=aef_annual"
      "--docs-dir=dataset_docs/aef"
    )
    local cmd
    #cmd="$(printf '%q ' "${aef_args[@]}")"
    cmd="pip install --quiet aef-loader && $(printf '%q ' "${aef_args[@]}")"

    log_line "$(date) Start processing: ${product_id} (config=${INDEX_CONFIG}, MODE=${MODE})"
    if [[ "${parallelism}" == "1" ]]; then
      if run_job_body "${product_id}" "${optional}" "${cmd}" -e "AWS_NO_SIGN_REQUEST=true"; then
        log_line "$(date) Successfully completed: ${product_id} (MODE=${MODE})"
      else
        log_line "$(date) ERROR processing: ${product_id} (MODE=${MODE})"
        return 1
      fi
      return
    fi

    (
      if run_job_body "${product_id}" "${optional}" "${cmd}" -e "AWS_NO_SIGN_REQUEST=true"; then
        log_line "$(date) Successfully completed: ${product_id} (MODE=${MODE})"
      else
        log_line "$(date) ERROR processing: ${product_id} (MODE=${MODE})"
        exit 1
      fi
    ) &
    pids+=("$!")
    return
  fi
  
  local -a stac_args=(
    stac-to-dc
    "--bbox=${BBOX}"
    "--catalog-href=${catalog_href}"
    "--collections=${collection}"
  )

  if [[ "${use_datetime}" == "True" ]]; then
    stac_args+=("--datetime=${DATETIME}")
  fi
  if [[ -n "${rename_product}" ]]; then
    stac_args+=("--rename-product=${rename_product}")
  fi
  if [[ -n "${query}" ]]; then
    stac_args+=("--options=query=${query}")
  fi

  local -a env_args=()
  if [[ "${auth}" == "cdse_s3" ]]; then
    env_args+=(
      -e "AWS_S3_ENDPOINT=eodata.dataspace.copernicus.eu"
      -e "AWS_ACCESS_KEY_ID=${CDSE_S3_ACCESS_KEY:-}"
      -e "AWS_SECRET_ACCESS_KEY=${CDSE_S3_SECRET_KEY:-}"
      -e "AWS_HTTPS=YES"
      -e "AWS_VIRTUAL_HOSTING=FALSE"
      -e "GDAL_HTTP_UNSAFESSL=YES"
      -e "GDAL_HTTP_TCP_KEEPALIVE=YES"
      -e "AWS_NO_SIGN_REQUEST=NO"
    )
  else
    env_args+=(-e "AWS_NO_SIGN_REQUEST=true")
  fi

  local cmd
  cmd="$(printf '%q ' "${stac_args[@]}")"

  log_line "$(date) Start processing: ${product_id} (config=${INDEX_CONFIG}, MODE=${MODE})"
  if [[ "${parallelism}" == "1" ]]; then
    if run_job_body "${product_id}" "${optional}" "${cmd}" "${env_args[@]}"; then
      log_line "$(date) Successfully completed: ${product_id} (MODE=${MODE})"
    else
      log_line "$(date) ERROR processing: ${product_id} (MODE=${MODE})"
      return 1
    fi
    return
  fi

  (
    if run_job_body "${product_id}" "${optional}" "${cmd}" "${env_args[@]}"; then
      log_line "$(date) Successfully completed: ${product_id} (MODE=${MODE})"
    else
      log_line "$(date) ERROR processing: ${product_id} (MODE=${MODE})"
      exit 1
    fi
  ) &
  pids+=("$!")
}

check_jobs() {
  local -a new_pids=()
  local pid
  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      new_pids+=("${pid}")
    fi
  done
  pids=("${new_pids[@]}")
}

wait_for_free_slot() {
  while true; do
    check_jobs
    if [[ "${#pids[@]}" -lt "${parallelism}" ]]; then
      return
    fi
    sleep 1
  done
}

if (( timing_enabled )); then
  index_start_epoch=$(date +%s)
fi

any_failed=0
for job_json in "${job_lines[@]}"; do
  if [[ "${parallelism}" != "1" ]]; then
    wait_for_free_slot
  fi
  if ! run_job "${job_json}"; then
    any_failed=1
  fi
done

if [[ "${parallelism}" == "1" ]]; then
  finish_indexing "${any_failed}"
fi

failed=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    failed=1
  fi
done

finish_indexing "${failed}"
