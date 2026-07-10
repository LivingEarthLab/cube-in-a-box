#!/usr/bin/env bash
# Register ODC product definitions listed in INDEX_CONFIG.
set -euo pipefail

INDEX_CONFIG="${INDEX_CONFIG:-config/indexation/default.yaml}"
MODE="${MODE:-prod}"
PROJECT="${PROJECT:-cube-in-a-box}"

if [[ "${MODE}" != "dev" && "${MODE}" != "prod" ]]; then
  echo "ERROR: MODE must be 'dev' or 'prod' (got: ${MODE})" >&2
  exit 2
fi

if [[ ! -f "${INDEX_CONFIG}" ]]; then
  echo "ERROR: INDEX_CONFIG not found: ${INDEX_CONFIG}" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${repo_root}"

if [[ "${MODE}" == "dev" ]]; then
  DC=(env COMPOSE_PROJECT_NAME="${PROJECT}-dev" docker compose -p "${PROJECT}-dev" -f docker-compose.yml -f docker-compose.dev.yml)
else
  DC=(env COMPOSE_PROJECT_NAME="${PROJECT}" docker compose -p "${PROJECT}" -f docker-compose.yml)
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

PYTHONPATH="${repo_root}" python3 - "${INDEX_CONFIG}" "${tmp_dir}" <<'PY'
import sys
from pathlib import Path

from scripts.index_config import (
    config_root,
    load_config,
    resolve_product_paths,
    write_product_files,
)

config_path, out_dir = sys.argv[1], Path(sys.argv[2])
config = load_config(config_path)
root = config_root(config_path)
paths = resolve_product_paths(config, root)
write_product_files(paths, out_dir)
PY

"${DC[@]}" --profile init run --rm -v "${tmp_dir}:/conf-index:ro" jupyter bash -lc \
  "for f in /conf-index/*.odc-product.yaml; do \
     datacube product add \"\$f\" 2>/dev/null || datacube product update --allow-unsafe \"\$f\"; \
   done"
