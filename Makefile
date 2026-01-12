## You can follow the steps below in order to get yourself a local ODC.
## Start by running `setup` then you should have a system that is fully configured
##
## Once running, you can access a Jupyter environment at 'http://localhost'
.PHONY: help setup up down clean

# BBOX=<left>,<bottom>,<right>,<top>
BBOX := 25,20,35,30
# DATETIME=<start_date>/<end_date> e.g. 2021-06-01/2021-07-01
DATETIME := 2021-12-01/2021-12-31

help: ## Print this help
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: build up init product index update-explorer

up: ## 1. Bring up your Docker environment
	docker compose up -d --remove-orphans

init: ## 2. Prepare the database
	docker compose exec -T jupyter datacube -v system init

product: ## 3. Add a product definition for Sentinel-2
	docker compose exec -T jupyter sh -c "datacube product add /conf/*.odc-product.yaml"
# 	docker compose exec -T jupyter datacube product add /conf/s1_rtc.odc-product.yaml
# 	docker compose exec -T jupyter datacube product add /conf/*.odc-product.yaml
# 	docker compose exec -T jupyter dc-sync-products /conf/products.csv
# 	docker compose exec -T jupyter datacube product add /conf/lsX_c2l2_sp.products.yaml
# 	docker compose exec -T jupyter datacube product add /conf/io_lulc_annual_v02.product.yaml

index: index-parallel  ## 4. Index some data (Change extents with BBOX='<left>,<bottom>,<right>,<top>')
index-parallel:
	bash index-parallel.sh  # new products to be indexed to be added there as well
index-serie: index-sentinel-2-l2a index-io-lulc-annual-v02 index-nasadem \
             index-ls45_c2l2_sp index-ls7_c2l2_sp index-ls89_c2l2_sp \
             index-sentinel-1-rtc
index-sentinel-2-l2a:
	docker compose exec -T jupyter bash -c \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-2-l2a' \
			--datetime='$(DATETIME)' \
			--rename-product='s2_l2a'"
index-io-lulc-annual-v02:
	docker compose exec -T jupyter bash -c \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href=https://planetarycomputer.microsoft.com/api/stac/v1/ \
			--collections='io-lulc-annual-v02'" || true
index-nasadem:
	docker compose exec -T jupyter bash -c \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='nasadem'"
index-ls45_c2l2_sp:
	docker compose exec -T jupyter bash -c \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-4\",\"landsat-5\"]}}' \
            --rename-product='ls45_c2l2_sp'"
index-ls7_c2l2_sp:
	docker compose exec -T jupyter bash -c \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-7\"]}}' \
            --rename-product='ls7_c2l2_sp'"
index-ls89_c2l2_sp:
	docker compose exec -T jupyter bash -c \
        "stac-to-dc \
            --bbox='$(BBOX)' \
            --catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
            --collections='landsat-c2-l2' \
            --datetime='$(DATETIME)' \
            --options='query={\"platform\":{\"in\":[\"landsat-8\",\"landsat-9\"]}}' \
            --rename-product='ls89_c2l2_sp'"
index-sentinel-1-rtc:
	docker compose exec -T jupyter bash -c \
		"stac-to-dc \
			--bbox='$(BBOX)' \
			--catalog-href='https://planetarycomputer.microsoft.com/api/stac/v1/' \
			--collections='sentinel-1-rtc' \
			--datetime='$(DATETIME)'"

update-explorer: # Update the Explorer DB
	docker compose exec -T explorer cubedash-gen --init --all

down: ## Bring down the system
	docker compose down

build: ## Rebuild the base image
	docker compose pull --ignore-pull-failures
	docker compose build

shell: ## Start an interactive shell
	docker compose exec jupyter bash

clean: ## Delete everything
	docker compose down --rmi all -v
	docker run --rm -v ./data/pg:/data/pg alpine sh -c "rm -rf /data/pg/*" || true
	docker rmi alpine || true

logs: ## Show the logs from the stack
	docker compose logs --follow

build-image:
	docker build --tag opendatacube/cube-in-a-box .

push-image:
	docker push opendatacube/cube-in-a-box
