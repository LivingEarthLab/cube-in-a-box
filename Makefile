## Local Open Data Cube (ODC) environment (Docker).
##
## Quick start (first time):
##   1) make setup
##   2) Open Jupyter in your browser: http://localhost/jupyter
##
## Day-to-day:
##   make up        Start the environment (runs in the background)
##   make status    Check what is running
##   make logs      View live logs (useful if something fails)
##   make down      Stop the environment (keeps your local data)
##
## Data / indexing:
##   make index     Load example data for the selected area/time (BBOX, DATETIME)
##
## Reset options (use with care):
##   make clean       Stop services and remove containers/volumes/images
##   make purge-data  Delete local data in ./data (irreversible; requires CONFIRM=1)
##
## Dev mode (local images):
##   Use "dev-" prefix for dev mode, or run: make dev-help
##   Example:
##     make up       # starts the normal (prebuilt) images
##     make dev-up   # starts the dev stack using locally built images

.PHONY: clean down help index index-parallel index-serie index-sentinel-2-l2a index-io-lulc-annual-v02 \
        index-nasadem index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp index-sentinel-1-rtc init logs \
        product pull purge-data shell setup status up update-explorer dev-build dev-build-nocache dev-clean \
        dev-down dev-help dev-index dev-index-parallel dev-index-serie dev-index-sentinel-2-l2a \
        dev-index-io-lulc-annual-v02 dev-index-nasadem dev-index-ls45_c2l2_sp dev-index-ls7_c2l2_sp \
        dev-index-ls89_c2l2_sp dev-index-sentinel-1-rtc dev-init dev-logs dev-product dev-pull dev-purge-data \
        dev-shell dev-setup dev-status dev-up dev-update-explorer
.DEFAULT_GOAL := help

# Local data location (bind-mounted into containers)
DATA_DIR ?= $(CURDIR)/data

# For destructive operations (purge-data)
CONFIRM ?= 0

# Defaults (override on the command line: make BBOX="..." DATETIME="...")
# BBOX=<left>,<bottom>,<right>,<top>
BBOX ?= 25,20,35,30
# DATETIME=<start_date>/<end_date> e.g. 2021-06-01/2021-07-01
DATETIME ?= 2021-12-01/2021-12-31

# Compose project name (prod). Dev uses "$(PROJECT)-dev".
PROJECT ?= cube-in-a-box

# Compose wrappers:
# - Prod mode uses ONLY docker-compose.yml (prebuilt images; no local builds).
# - Dev mode uses docker-compose.yml + docker-compose.dev.yml (local images).
DC_PROD := COMPOSE_PROJECT_NAME=$(PROJECT) docker compose -p $(PROJECT) -f docker-compose.yml
DC_DEV  := COMPOSE_PROJECT_NAME=$(PROJECT)-dev docker compose -p $(PROJECT)-dev -f docker-compose.yml -f docker-compose.dev.yml

# -------------------------
# Production / default mode
# -------------------------

clean: ## Stop everything and remove containers, volumes, and built images
	@$(DC_PROD) down --rmi all -v --remove-orphans

down: ## Stop the running services (keeps your data and images)
	@$(DC_PROD) down --remove-orphans

help: ## Show production commands
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
			{t=$$1; d=$$2} \
			(t ~ /^dev-/ && t != "dev-help") {next} \
			{printf "\033[36m%-18s\033[0m %s\n", t, d}'

index: index-parallel ## Index example data for the selected area/time (uses BBOX and DATETIME)
	@true

index-parallel: ## Index data using the automated script (recommended)
	@bash index-parallel.sh

index-serie: ## Index data step-by-step (older method; slower)
	@$(MAKE) index-sentinel-2-l2a index-io-lulc-annual-v02 index-nasadem \
	         index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp \
	         index-sentinel-1-rtc

index-sentinel-2-l2a: # Index Sentinel-2 L2A
	@$(DC_PROD) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-2-l2a' \
			--datetime='$(DATETIME)' \
			--rename-product='s2_l2a'"

index-io-lulc-annual-v02: # Index IO LULC Annual v02 (non-fatal if empty)
	@$(DC_PROD) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='io-lulc-annual-v02'" || true

index-nasadem: # Index NASADEM
	@$(DC_PROD) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='nasadem'"

index-ls45_c2l2_sp: # Index Landsat 4/5 Collection 2 L2
	@$(DC_PROD) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-4\",\"landsat-5\"]}}' \
            --rename-product='ls45_c2l2_sp'"

index-ls7_c2l2_sp: # Index Landsat 7 Collection 2 L2
	@$(DC_PROD) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-7\"]}}' \
            --rename-product='ls7_c2l2_sp'"

index-ls89_c2l2_sp: # Index Landsat 8/9 Collection 2 L2
	@$(DC_PROD) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-8\",\"landsat-9\"]}}' \
            --rename-product='ls89_c2l2_sp'"

index-sentinel-1-rtc: # Index Sentinel-1 RTC
	@$(DC_PROD) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-1-rtc' \
			--datetime='$(DATETIME)'"

init: ## Initialize the Open Data Cube database (run once after setup)
	@$(DC_PROD) exec -T jupyter datacube -v system init

logs: ## Show live logs from all services (useful for troubleshooting)
	@$(DC_PROD) logs --follow

product: ## Load product definitions into the database (describes available datasets)
	@$(DC_PROD) exec -T jupyter bash -lc "datacube product add /conf/*.odc-product.yaml"

pull: ## Download all service images (recommended before first run)
	@$(DC_PROD) pull

purge-data: down ## Delete local data in ./data (pg and local_data). Irreversible; requires CONFIRM=1
	@echo "This will delete:"
	@echo "  $(DATA_DIR)/pg/*"
	@echo "  $(DATA_DIR)/local_data/*"
	@echo "Re-run with: make purge-data CONFIRM=1"
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/pg/* /data/local_data/*" || true
	@docker image rm alpine:3.23.2 >/dev/null 2>&1 || true

shell: ## Open a terminal inside the Jupyter container (advanced)
	@$(DC_PROD) exec jupyter bash

setup: ## First-time setup using prebuilt images
	@$(MAKE) pull up init product index update-explorer

status: ## Show what is running (containers and their status)
	@$(DC_PROD) ps

up: ## Start the environment in the background (then open Jupyter in your browser)
	@$(DC_PROD) up -d --remove-orphans --wait --wait-timeout 120

update-explorer: ## Rebuild the Explorer index so datasets appear in the web UI
	@$(DC_PROD) exec -T explorer cubedash-gen --init --all

# --------
# Dev mode
# --------

dev-build: ## Build the dev images locally (jupyter + explorer)
	@$(DC_DEV) build --pull

dev-build-nocache: ## Build the dev images locally from scratch (ignores cache; slower but fixes “stuck” builds)
	@$(DC_DEV) build --pull --no-cache

dev-clean: ## Full reset of the dev environment (containers/volumes/images)
	@$(DC_DEV) down --rmi all -v --remove-orphans

dev-down: ## Stop the dev environment (keeps your local data and images)
	@$(DC_DEV) down --remove-orphans

dev-help: ## Show dev-mode commands (local builds, dev stack)
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
			{t=$$1; d=$$2} \
			(t !~ /^dev-/) {next} \
			(t == "dev-help") {next} \
			{printf "\033[36m%-18s\033[0m %s\n", t, d}'

dev-index: dev-index-parallel ## Index example data for the selected area/time (uses BBOX and DATETIME)
	@true

dev-index-parallel: ## Index data in dev using the automated script (recommended)
	@MODE=dev bash index-parallel.sh

dev-index-serie: ## Index dev data step-by-step (older method; slower)
	@$(MAKE) dev-index-sentinel-2-l2a dev-index-io-lulc-annual-v02 dev-index-nasadem \
	         dev-index-ls45_c2l2_sp dev-index-ls7_c2l2_sp dev-index-ls89_c2l2_sp \
	         dev-index-sentinel-1-rtc

dev-index-sentinel-2-l2a: # Index Sentinel-2 L2A
	@$(DC_DEV) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-2-l2a' \
			--datetime='$(DATETIME)' \
			--rename-product='s2_l2a'"

dev-index-io-lulc-annual-v02: # Index IO LULC Annual v02 (non-fatal if empty)
	@$(DC_DEV) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='io-lulc-annual-v02'" || true

dev-index-nasadem: # Index NASADEM
	@$(DC_DEV) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='nasadem'"

dev-index-ls45_c2l2_sp: # Index Landsat 4/5 Collection 2 L2
	@$(DC_DEV) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-4\",\"landsat-5\"]}}' \
            --rename-product='ls45_c2l2_sp'"

dev-index-ls7_c2l2_sp: # Index Landsat 7 Collection 2 L2
	@$(DC_DEV) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-7\"]}}' \
            --rename-product='ls7_c2l2_sp'"

dev-index-ls89_c2l2_sp: # Index Landsat 8/9 Collection 2 L2
	@$(DC_DEV) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-8\",\"landsat-9\"]}}' \
            --rename-product='ls89_c2l2_sp'"

dev-index-sentinel-1-rtc: # Index Sentinel-1 RTC
	@$(DC_DEV) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-1-rtc' \
			--datetime='$(DATETIME)'"

dev-init: ## Initialize the Open Data Cube database (run once after setup)
	@$(DC_DEV) exec -T jupyter datacube -v system init

dev-logs: ## Show live logs from all services (useful for troubleshooting)
	@$(DC_DEV) logs --follow

dev-product: ## Load product definitions into the database (describes available datasets)
	@$(DC_DEV) exec -T jupyter bash -lc "datacube product add /conf/*.odc-product.yaml"

dev-pull: ## Download all service images (recommended before first run)
	@$(DC_DEV) pull

dev-purge-data: dev-down ## Delete local data in ./data (pg and local_data). Irreversible; requires CONFIRM=1
	@echo "This will delete:"
	@echo "  $(DATA_DIR)/pg/*"
	@echo "  $(DATA_DIR)/local_data/*"
	@echo "Re-run with: make dev-purge-data CONFIRM=1"
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/pg/* /data/local_data/*" || true
	@docker image rm alpine:3.23.2 >/dev/null 2>&1 || true

dev-shell: ## Open a terminal inside the Jupyter container (advanced)
	@$(DC_DEV) exec jupyter bash

dev-setup: ## First-time setup with jupyter & explorer image build
	@$(MAKE) dev-build dev-up dev-init dev-product dev-index dev-update-explorer

dev-status: ## Show what is running (containers and their status)
	@$(DC_DEV) ps

dev-up: ## Start the environment in the background (then open Jupyter in your browser)
	@$(DC_DEV) up -d --remove-orphans --wait --wait-timeout 120

dev-update-explorer: ## Rebuild the Explorer index so datasets appear in the web UI
	@$(DC_DEV) exec -T explorer cubedash-gen --init --all