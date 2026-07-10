"""Parse STAC indexation YAML configs for make product/index scripts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml


def config_root(index_config_path: str | Path) -> Path:
    """Return config/ directory containing indexation/ and products/."""
    path = Path(index_config_path).resolve()
    if path.parent.name == "indexation":
        return path.parent.parent
    return path.parent


def load_config(config_path: str | Path) -> dict:
    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def product_name_from_file(product_path: Path) -> str:
    with open(product_path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    name = doc.get("name") if isinstance(doc, dict) else None
    if not name:
        raise ValueError(f"No name field in product file: {product_path}")
    return name


def resolve_product_paths(config: dict, config_root_dir: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for product in config.get("products", []):
        rel = product.get("product_file")
        if not rel:
            raise ValueError(
                f"Product {product.get('id')!r} must define product_file"
            )
        path = (config_root_dir / rel).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Product file not found: {path}")
        if path not in seen:
            seen.add(path)
            paths.append(path)
    return paths


def odc_product_names(config: dict, config_root_dir: Path) -> list[str]:
    return [
        product_name_from_file(path)
        for path in resolve_product_paths(config, config_root_dir)
    ]


def indexing_jobs(config: dict, config_root_dir: Path) -> list[dict]:
    catalogs = config.get("catalogs", {})
    jobs: list[dict] = []
    for product in config.get("products", []):
        catalog_name = product["catalog"]
        catalog = catalogs[catalog_name]
        product_path = config_root_dir / product["product_file"]
        rename_product = product.get("rename_product") or product_name_from_file(
            product_path
        )
        jobs.append(
            {
                "id": product["id"],
                "catalog_href": catalog["href"],
                "auth": catalog.get("auth", ""),
                "collection": product["collection"],
                "optional": bool(product.get("optional", False)),
                "rename_product": rename_product,
                "datetime": bool(product.get("datetime", False)),
                "query": product.get("query", ""),
            }
        )
    return jobs


def write_product_files(product_paths: list[Path], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for src in product_paths:
        with open(src, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        name = doc.get("name", src.stem)
        dest = out_dir / f"{name}.odc-product.yaml"
        with open(dest, "w", encoding="utf-8") as fh:
            yaml.safe_dump(doc, fh, sort_keys=False)
        written.append(dest)
    return written


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(f"usage: {sys.argv[0]} <command> <config>")

    command = sys.argv[1]
    config_path = sys.argv[2]
    config = load_config(config_path)
    root = config_root(config_path)

    if command == "odc-products":
        for name in odc_product_names(config, root):
            print(name)
    elif command == "parallelism":
        print(config.get("parallelism", 4))
    elif command == "jobs":
        for job in indexing_jobs(config, root):
            print(json.dumps(job))
    elif command == "product-files":
        for path in resolve_product_paths(config, root):
            print(path)
    else:
        raise SystemExit(f"unknown command: {command}")


if __name__ == "__main__":
    main()
