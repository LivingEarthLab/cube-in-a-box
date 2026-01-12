## You can follow the steps below in order to get yourself a local ODC.
## Start by running `setup` then you should have a system that is fully configured
##
## Once running, you can access a Jupyter environment at 'http://localhost'
.PHONY: help setup up down clean build init product index index-parallel index-serie \
        index-sentinel-2-l2a index-io-lulc-annual-v02 index-nasadem \
        index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp index-sentinel-1-rtc \
        update-explorer shell logs status purge-data build-nocache pull

# Defaults (override on the command line: make BBOX="..." DATETIME="...")
# BBOX=<left>,<bottom>,<right>,<top>
BBOX ?= 25,20,35,30
# DATETIME=<start_date>/<end_date> e.g. 2021-06-01/2021-07-01
DATETIME ?= 2021-12-01/2021-12-31

# Docker Compose wrapper (optionally override PROJECT: make PROJECT=myproj up)
PROJECT ?= cube-in-a-box
DC := docker compose -p $(PROJECT)

build: ## Rebuild the base image
	@$(DC) build --pull

build-nocache: ## Full rebuild without cache (slower)
	@$(DC) build --pull --no-cache

clean: ## Remove containers/images/volumes
	@$(DC) down --rmi all -v --remove-orphans

down: ## Bring down the system
	@$(DC) down --remove-orphans

index: index-parallel ## Index some data (override with BBOX='...' DATETIME='...')
	@true

index-parallel: ## Index data using index-parallel.sh
	@bash index-parallel.sh  # new products to be indexed to be added there as well

index-serie: ## Index data sequentially (legacy)
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

init: ## Prepare the database
	@$(DC) exec -T jupyter datacube -v system init

help: ## Print this help
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

logs: ## Show the logs from the stack
	@$(DC) logs --follow

product: ## Add a product definition for Sentinel-2
	@$(DC) exec -T jupyter bash -lc "datacube product add /conf/*.odc-product.yaml"

pull: ## Pull external service images (traefik/postgres, etc.)
	@$(DC) pull

purge-data: ## Delete local bind-mounted data (irreversible)
	@rm -rf ./data/pg/* || true
	@rm -rf ./data/local_data/* || true

shell: ## Start an interactive shell in the jupyter container
	@$(DC) exec jupyter bash

setup: ## Full setup using existing images
	@$(MAKE) up init product index update-explorer

setup-build: ## Full setup rebuilding images first (Jupyter & Explorer)
	@$(MAKE) build up init product index update-explorer

status: ## Show container status
	@$(DC) ps

up: ## Bring up your Docker environment
	@$(DC) up -d --remove-orphans --wait --wait-timeout 120

update-explorer: ## Update the Explorer DB
	@$(DC) exec -T explorer cubedash-gen --init --all