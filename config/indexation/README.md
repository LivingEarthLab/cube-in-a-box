# STAC indexation configuration

YAML files in this directory drive `make product` and `make index` via `product-from-config.sh` and `index-from-config.sh`.

ODC product definitions live alongside under [`../products/`](../products/) (`pc/` and `cop/` folders).

## Schema

| Top-level field | Description |
|-----------------|-------------|
| `name` | Config identifier (informational) |
| `parallelism` | Max concurrent `stac-to-dc` jobs (default: 4); overridden by `PARALLELISM=1` for `make index-serie` |
| `catalogs` | Named STAC catalog endpoints |
| `products` | List of products to register and index |

### Catalog entry

| Field | Description |
|-------|-------------|
| `href` | STAC catalog root URL |
| `auth` | Optional: `cdse_s3` enables Copernicus Data Space S3 credentials |

### Product entry

| Field | Description |
|-------|-------------|
| `id` | Job identifier (used in logs) |
| `catalog` | Key into `catalogs` |
| `collection` | STAC collection ID |
| `product_file` | Path to ODC product YAML, relative to `config/` |
| `datetime` | If true, pass `DATETIME` from Make |
| `query` | JSON STAC query (`--options='query=...'`) |
| `optional` | If true, empty results do not fail the run |

The ODC product name used for indexing (`--rename-product`) is read from the `name:` field in `product_file`. Product YAMLs also record `metadata.product.source` and `metadata.product.stac_catalog`.

Only products listed in the active config are registered (`make product`) and indexed (`make index`).

## Layout

| Folder | `source` in YAML | Contents |
|--------|------------------|----------|
| `config/products/pc/` | `planetary-computer` | PC STAC products |
| `config/products/cop/` | `copernicus` | CDSE STAC products |

## Preset

- `default.yaml` — 11 products from Planetary Computer and Copernicus Data Space (including `s2_l2a_pc` and `s2_l2a_cdse` as separate Sentinel-2 L2A products)

## Usage

```bash
make setup BBOX=... DATETIME=...
make index-serie
make index TIME_INDEX=1 BBOX=... DATETIME=...
```

Pass `TIME_INDEX=1` to print per-product durations and total wall time at the end of indexing.

Set `CDSE_S3_ACCESS_KEY` and `CDSE_S3_SECRET_KEY` in `.env` when indexing CDSE products. CLCplus is Europe-only — use a European `BBOX` for meaningful results.

Switching `INDEX_CONFIG` on an existing database may replace a product definition with a different source; prefer a fresh setup when changing preset.
