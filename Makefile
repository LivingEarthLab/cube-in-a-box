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
## Mode selection:
##   make up              # production mode (default, uses prebuilt images)
##   make up MODE=dev     # dev mode (uses locally built images)
##   export MODE=dev      # set dev mode for entire session
##
## Dev-only commands (require MODE=dev):
##   make build MODE=dev          Build images locally
##   make build-nocache MODE=dev  Build from scratch (no cache)

.PHONY: clean down help index index-parallel index-serie index-sentinel-2-l2a index-io-lulc-annual-v02 \
        index-nasadem index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp index-sentinel-1-rtc init logs \
        product pull purge-data shell setup status up update-explorer build build-nocache
.DEFAULT_GOAL := help

# Mode selection: prod (default) or dev
MODE ?= prod

# Local data location (bind-mounted into containers)
DATA_DIR ?= $(CURDIR)/data

# For destructive operations (purge-data)
CONFIRM ?= 0

# Defaults (override on the command line: make BBOX="..." DATETIME="...")
# BBOX=<left>,<bottom>,<right>,<top>
BBOX ?= 25,20,35,30
# DATETIME=<start_date>/<end_date> e.g. 2021-06-01/2021-07-01
DATETIME ?= 2021-12-01/2021-12-31

# Compose project name
PROJECT ?= cube-in-a-box

# Compose wrappers for prod and dev modes
DC_PROD := COMPOSE_PROJECT_NAME=$(PROJECT) docker compose -p $(PROJECT) -f docker-compose.yml
DC_DEV  := COMPOSE_PROJECT_NAME=$(PROJECT)-dev docker compose -p $(PROJECT)-dev -f docker-compose.yml -f docker-compose.dev.yml

# Select the appropriate docker compose command based on MODE
ifeq ($(MODE),dev)
    DC := $(DC_DEV)
else ifeq ($(MODE),prod)
    DC := $(DC_PROD)
else
    $(error Invalid MODE: $(MODE). Use MODE=prod or MODE=dev)
endif

# -----------------
# Dev-only commands
# -----------------

build: ## Build the images locally (dev mode only)
ifeq ($(MODE),dev)
	@$(DC) build --pull
else
	@echo "\033[31mError: 'build' is only available in dev mode\033[0m"
	@echo "Run with: make build MODE=dev"
	@exit 1
endif

build-nocache: ## Build the images locally from scratch (dev mode only, ignores cache)
ifeq ($(MODE),dev)
	@$(DC) build --pull --no-cache
else
	@echo "\033[31mError: 'build-nocache' is only available in dev mode\033[0m"
	@echo "Run with: make build-nocache MODE=dev"
	@exit 1
endif

# ------------------------------------
# Shared commands (work in both modes)
# ------------------------------------

clean: ## Stop everything and remove containers, volumes, and built images
	@$(DC) down --rmi all -v --remove-orphans

down: ## Stop the running services (keeps your data and images)
	@$(DC) down --remove-orphans

help: ## Show available commands
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c4-
	@echo
	@echo "Current mode: \033[36m$(MODE)\033[0m"
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'
ifeq ($(MODE),dev)
	@echo ""
	@echo "\033[90mTip: You are in dev mode. To switch to prod: unset MODE or use MODE=prod\033[0m"
else
	@echo ""
	@echo "\033[90mTip: For dev mode with local builds: export MODE=dev or use MODE=dev\033[0m"
endif

index: index-parallel ## Index example data for the selected area/time (uses BBOX and DATETIME)
	@true

index-parallel: ## Index data using the automated script (recommended)
ifeq ($(MODE),dev)
	@MODE=dev bash index-parallel.sh
else
	@bash index-parallel.sh
endif

index-serie: ## Index data step-by-step (older method; slower)
	@$(MAKE) index-sentinel-2-l2a index-io-lulc-annual-v02 index-nasadem \
	         index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp \
	         index-sentinel-1-rtc

index-sentinel-2-l2a: # Index Sentinel-2 L2A
	@$(DC) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-2-l2a' \
			--datetime='$(DATETIME)' \
			--rename-product='s2_l2a'"

index-io-lulc-annual-v02: # Index IO LULC Annual v02 (non-fatal if empty)
	@$(DC) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='io-lulc-annual-v02'" || true

index-nasadem: # Index NASADEM
	@$(DC) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='nasadem'"

index-ls45_c2l2_sp: # Index Landsat 4/5 Collection 2 L2
	@$(DC) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-4\",\"landsat-5\"]}}' \
            --rename-product='ls45_c2l2_sp'"

index-ls7_c2l2_sp: # Index Landsat 7 Collection 2 L2
	@$(DC) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-7\"]}}' \
            --rename-product='ls7_c2l2_sp'"

index-ls89_c2l2_sp: # Index Landsat 8/9 Collection 2 L2
	@$(DC) exec -T jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-8\",\"landsat-9\"]}}' \
            --rename-product='ls89_c2l2_sp'"

index-sentinel-1-rtc: # Index Sentinel-1 RTC
	@$(DC) exec -T jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-1-rtc' \
			--datetime='$(DATETIME)'"

init: ## Initialize the Open Data Cube database (run once after setup)
	@$(DC) exec -T jupyter datacube -v system init

logs: ## Show live logs from all services (useful for troubleshooting)
	@$(DC) logs --follow

product: ## Load product definitions into the database (describes available datasets)
	@$(DC) exec -T jupyter bash -lc "datacube product add /conf/*.odc-product.yaml"

pull: ## Download all service images (recommended before first run in prod mode)
	@$(DC) pull

purge-data: down ## Delete local data in ./data (pg and local_data). Irreversible; requires CONFIRM=1
	@echo "This will delete:"
	@echo "  $(DATA_DIR)/pg/*"
	@echo "  $(DATA_DIR)/local_data/*"
	@echo "Re-run with: make purge-data CONFIRM=1"
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/pg/* /data/local_data/*" || true
	@docker image rm alpine:3.23.2 >/dev/null 2>&1 || true

shell: ## Open a terminal inside the Jupyter container (advanced)
	@$(DC) exec jupyter bash

setup: ## First-time setup (mode-dependent: uses pull in prod, build in dev)
ifeq ($(MODE),dev)
	@echo "Setting up dev environment (building images locally)..."
	@$(MAKE) build up init product index update-explorer
else
	@echo "Setting up production environment (using prebuilt images)..."
	@$(MAKE) pull up init product index update-explorer
endif

status: ## Show what is running (containers and their status)
	@$(DC) ps

up: ## Start the environment in the background (then open Jupyter in your browser)
	@$(DC) up -d --remove-orphans --wait --wait-timeout 120

update-explorer: ## Rebuild the Explorer index so datasets appear in the web UI
	@$(DC) exec -T explorer cubedash-gen --init --all
