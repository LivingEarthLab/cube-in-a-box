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
##   export MODE=dev      # set dev mode for entire session, and then make *
##   unset MODE           # set dev mode to prod mode, and then make *

.PHONY: backup build build-nocache clean down help index index-esa-worldcover index-io-lulc-annual-v02 \
        index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp index-nasadem index-parallel \
        index-sentinel-1-rtc index-sentinel-2-l2a index-serie init logs product pull purge-data \
        purge-user purge-users release-push restore setup shell status up update-explorer wait-for-db \
        docs
.DEFAULT_GOAL := help

# Mode selection: prod (default) or dev
MODE ?= prod

# Local data location (bind-mounted into containers)
DATA_DIR ?= $(CURDIR)/data

# For destructive operations (purge-data, purge-user(s), restore)
CONFIRM ?= 0

# Defaults (override on the command line: make BBOX="..." DATETIME="...")
# BBOX=<left>,<bottom>,<right>,<top>
BBOX ?= 25,20,35,30
# DATETIME=<start_date>/<end_date> e.g. 2021-06-01/2021-07-01
DATETIME ?= 2021-12-01/2021-12-31

# Current date
DATE_YYYYMMDD := $(shell date +%Y%m%d)

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
# Commands
# -----------------

backup: ## Create a backup of the PostgreSQL database
	@mkdir -p ./backups
	@BACKUP_FILE="./backups/opendatacube_$$(date +%Y%m%d_%H%M%S).sql"; \
	echo "Creating backup: $$BACKUP_FILE"; \
	$(DC) exec -T postgres sh -lc \
		'PGPASSWORD="$$POSTGRES_PASS" pg_dump -h localhost -U "$$POSTGRES_USER" -d "$$POSTGRES_DBNAME"' \
		> "$$BACKUP_FILE"; \
	echo "Backup completed: $$BACKUP_FILE"

build: ## Build the images locally
	@docker buildx bake dev

build-nocache: ## Build the images locally from scratch
	@docker buildx bake --no-cache dev

clean: ## Stop everything and remove containers, volumes, and built images
	@# Remove spawned user containers to release the network
	@docker ps -aq --filter "name=^jupyter-" | xargs -r docker rm -f
	@$(DC) --profile init down --rmi all -v --remove-orphans

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

index-esa-worldcover: ## Index ESA WorldCover (non-fatal if empty)
	@$(DC) --profile init run --rm jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='esa-worldcover'" || true

index-io-lulc-annual-v02: # Index IO LULC Annual v02 (non-fatal if empty)
	@$(DC) --profile init run --rm jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='io-lulc-annual-v02'" || true

index-ls45_c2l2_sp: # Index Landsat 4/5 Collection 2 L2
	@$(DC) --profile init run --rm jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-4\",\"landsat-5\"]}}' \
            --rename-product='ls45_c2l2_sp'"

index-ls7_c2l2_sp: # Index Landsat 7 Collection 2 L2
	@$(DC) --profile init run --rm jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-7\"]}}' \
            --rename-product='ls7_c2l2_sp'"

index-ls89_c2l2_sp: # Index Landsat 8/9 Collection 2 L2
	@$(DC) --profile init run --rm jupyter bash -lc \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-8\",\"landsat-9\"]}}' \
            --rename-product='ls89_c2l2_sp'"

index-nasadem: # Index NASADEM
	@$(DC) --profile init run --rm jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='nasadem'"

index-parallel: ## Index data using the automated script (recommended)
ifeq ($(MODE),dev)
	@MODE=dev bash index-parallel.sh
else
	@bash index-parallel.sh
endif

index-sentinel-1-rtc: # Index Sentinel-1 RTC
	@$(DC) --profile init run --rm jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-1-rtc' \
			--datetime='$(DATETIME)'"

index-sentinel-2-l2a: # Index Sentinel-2 L2A
	@$(DC) --profile init run --rm jupyter bash -lc \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-2-l2a' \
			--datetime='$(DATETIME)' \
			--rename-product='s2_l2a'"

index-serie: ## Index data step-by-step (older method; slower)
	@$(MAKE) index-sentinel-2-l2a index-io-lulc-annual-v02 index-nasadem \
	         index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp \
	         index-sentinel-1-rtc index-esa-worldcover

init: wait-for-db ## Initialize the Open Data Cube database (run once after setup)
	@$(DC) --profile init run --rm jupyter datacube -v system init
	@echo "Setting up read-only user and permissions..."
	@$(DC) exec -T postgres sh -lc 'PGPASSWORD="$$POSTGRES_PASS" psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DBNAME" -f /docker-entrypoint-initdb.d/init-ro-user.sql'

logs: ## Show live logs from all services (useful for troubleshooting)
	@$(DC) logs --follow

docs: ## Render Quarto documentation (Admin and User guides)
	@echo "Rendering Admin Guide..."
	@quarto render quarto/admin
	@mkdir -p docs/admin
	@cp -r quarto/admin/_book/* docs/admin/
	@echo "Rendering User Guide..."
	@quarto render quarto/user
	@mkdir -p docs/user
	@cp -r quarto/user/_book/* docs/user/

product: ## Load product definitions into the database (describes available datasets)
	@$(DC) --profile init run --rm jupyter bash -lc "datacube product add /conf/*.odc-product.yaml"

pull: ## Download all service images (recommended before first run in prod mode)
	@$(DC) pull

purge-data: down ## Delete local data in ./data. Irreversible; requires CONFIRM=1
	@echo "This will delete:"
	@echo "  $(DATA_DIR)/jupyterhub_data/*"
	@echo "  $(DATA_DIR)/local_data/*"
	@echo "  $(DATA_DIR)/pg/*"
	@echo "  $(DATA_DIR)/shared/*"
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/jupyterhub_data/* /data/local_data/* /data/pg/* /data/shared/*" || true

purge-user: ## Remove a specific user container, volume and shared data (usage: make purge-user HUB_USER=username). Irreversible; requires CONFIRM=1
	@if [ -z "$(HUB_USER)" ]; then echo "Error: HUB_USER argument is required. Usage: make purge-user HUB_USER=<username>"; exit 1; fi
	@echo "Removing container, volume and shared data for user: $(HUB_USER)..."
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker rm -f jupyter-$(HUB_USER) 2>/dev/null || echo "Container jupyter-$(HUB_USER) not found or already removed."
	@docker volume rm jupyterhub-user-$(HUB_USER) 2>/dev/null || echo "Volume jupyterhub-user-$(HUB_USER) not found or already removed."
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/shared/$(HUB_USER)" || true

purge-users: ## Remove all spawned JupyterHub user containers, volumes and shared data. Irreversible; requires CONFIRM=1
	@echo "This will remove spawned user containers, volumes and shared data..."
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@docker ps -aq --filter "name=^jupyter-" | xargs -r docker rm -f
	@docker volume ls -q --filter "name=^jupyterhub-user-" | xargs -r docker volume rm
	@docker run --rm -v "$(DATA_DIR):/data" alpine:3.23.2 sh -c "rm -rf /data/shared/*" || true

release-push: ## Build and push multi-architecture production images to the configured container registry
	@echo "Ensuring QEMU emulation support..."
	@docker run --privileged --rm tonistiigi/binfmt --install all
	@echo "Tag: $(DATE_YYYYMMDD)"
	TAG=$(DATE_YYYYMMDD) docker buildx bake release --push

restore: ## Restore PostgreSQL database from a backup file (usage: make restore BACKUP_FILE=./backups/file.sql). Irreversible; requires CONFIRM=1
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "Error: BACKUP_FILE argument is required. Usage: make restore BACKUP_FILE=./backups/file.sql"; \
		exit 1; \
	fi
	@if [ ! -f "$(BACKUP_FILE)" ]; then \
		echo "Error: Backup file not found: $(BACKUP_FILE)"; \
		exit 1; \
	fi
	@echo "Restoring database from: $(BACKUP_FILE)"
	@echo "WARNING: This will overwrite the current database!"
	@if [ "$(CONFIRM)" != "1" ]; then echo "Refusing to run without CONFIRM=1"; exit 1; fi
	@$(DC) exec -T postgres sh -lc '\
		PGPASSWORD="$$POSTGRES_PASS" psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U "$$POSTGRES_USER" -d postgres \
		  -c "ALTER DATABASE \"$$POSTGRES_DBNAME\" WITH ALLOW_CONNECTIONS false;" \
		  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '\''$$POSTGRES_DBNAME'\'' AND pid <> pg_backend_pid();" \
		  -c "DROP DATABASE IF EXISTS \"$$POSTGRES_DBNAME\";" \
		  -c "CREATE DATABASE \"$$POSTGRES_DBNAME\" OWNER \"$$POSTGRES_USER\";" \
		  -c "ALTER DATABASE \"$$POSTGRES_DBNAME\" WITH ALLOW_CONNECTIONS true;"'
	@$(DC) exec -T postgres sh -lc \
		'PGPASSWORD="$$POSTGRES_PASS" psql -v ON_ERROR_STOP=1 -h localhost -U "$$POSTGRES_USER" -d "$$POSTGRES_DBNAME"' \
		< "$(BACKUP_FILE)"
	@echo "Restore completed successfully"

setup: ## First-time setup (mode-dependent: uses pull in prod, build in dev)
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found!"; \
		echo "Please create a .env file before running setup."; \
		echo "You can use .env.example as a template if available."; \
		exit 1; \
	fi
ifeq ($(MODE),dev)
	@echo ""
	@echo "╔═══════════════════════════════════════╗"
	@echo "║                                       ║"
	@echo "║         🛠️  DEV MODE SETUP 🛠️          ║"
	@echo "║                                       ║"
	@echo "║   Building images locally...          ║"
	@echo "║                                       ║"
	@echo "╚═══════════════════════════════════════╝"
	@echo ""
	@$(MAKE) build up init product index update-explorer
else
	@echo ""
	@echo "╔═══════════════════════════════════════╗"
	@echo "║                                       ║"
	@echo "║      🚀 PRODUCTION MODE SETUP 🚀      ║"
	@echo "║                                       ║"
	@echo "║   Pulling remote images...            ║"
	@echo "║                                       ║"
	@echo "╚═══════════════════════════════════════╝"
	@echo ""
	@$(MAKE) pull up init product index update-explorer
endif

shell: ## Open a terminal inside the Jupyter container (usage: make shell HUB_USER=<username>)
	@if [ -z "$(HUB_USER)" ]; then echo "Error: HUB_USER argument is required. Usage: make shell HUB_USER=<username>"; exit 1; fi
	@docker exec -it jupyter-$(HUB_USER) bash

status: ## Show what is running (containers and their status)
	@$(DC) ps

up: ## Start the environment in the background (then open Jupyterhub in your browser)
	@$(DC) up -d --remove-orphans --wait --wait-timeout 120

update-explorer: up ## Rebuild the Explorer index so datasets appear in the web UI
	@$(DC) exec -T explorer cubedash-gen --init --force-refresh --recreate-dataset-extents --all

wait-for-db: # Wait for PostgreSQL to be ready to accept connections
	@$(DC) exec postgres pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DBNAME}
