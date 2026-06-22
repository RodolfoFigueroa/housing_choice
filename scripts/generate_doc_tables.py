from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import pandas as pd

from housing_choice.modeling.features import build_feature_catalog
from housing_choice.sector_clusters import SEMANTIC_SECTOR_CLUSTER_DEFINITIONS

if TYPE_CHECKING:
    from collections.abc import Sequence


SOURCE_CATALOG_COLUMNS = [
    "source_column",
    "model_column",
    "family",
    "transform",
    "scale_denominator",
    "reason",
]

SECTOR_CLUSTER_RAW_DESCRIPTION = "available raw sector-cluster exposure feature"

SECTOR_CLUSTER_FAMILY_BY_PREFIX = {
    output_prefix: f"{sector_name}_cluster"
    for sector_name, output_prefix, _scian_prefixes in (
        SEMANTIC_SECTOR_CLUSTER_DEFINITIONS
    )
}

FEATURE_DESCRIPTION_BY_REASON = {
    "identifier or geometry": "identifier or geometry",
    "binary access control": "binary access feature",
    "candidate job accessibility": "job accessibility feature",
    "zero variance": "zero variance in generated feature artifact",
    "service accessibility control": "service accessibility feature",
    "centrality control": "travel-time feature to city center",
    "raw crossing helper": (
        "raw travel-time feature used to derive nearest crossing time"
    ),
    "dynamic supply proxy": "built-area history feature",
    "selected interpretable cluster exposure": (
        "cluster exposure feature with prepared representation"
    ),
    "kept out to avoid over-specified cluster models": (SECTOR_CLUSTER_RAW_DESCRIPTION),
    "unclassified": "available raw feature not classified into a documented family",
    "nearest border crossing control": (
        "derived nearest border-crossing travel-time feature"
    ),
}

FEATURE_DERIVATION_BY_TRANSFORM = {
    "not a model covariate": "identifier or geometry",
    "not used in v1 model specs": "available raw feature; no prepared representation",
    "not classified for modelling": "available raw feature; no prepared representation",
}

SECTOR_THRESHOLD_COLUMN_NAMES = {
    "hotspot_candidate_cells": "hotspot_cells_before_cluster_threshold",
    "selected_clusters": "clusters_passing_threshold",
    "selected_cluster_jobs": "jobs_in_clusters_passing_threshold",
    "selected_cluster_businesses": "businesses_in_clusters_passing_threshold",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Markdown tables for the project documentation.",
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Project DATA_PATH. Defaults to the DATA_PATH environment variable.",
    )
    parser.add_argument(
        "--output",
        default="docs/generated/feature-catalog.md",
        help="Markdown output path.",
    )
    return parser.parse_args()


def resolve_data_path(cli_data_path: str | None) -> Path:
    if cli_data_path is not None:
        return Path(cli_data_path).expanduser().resolve()

    data_path = os.environ.get("DATA_PATH")
    if data_path is None:
        msg = "DATA_PATH is required. Set it or pass --data-path."
        raise RuntimeError(msg)
    return Path(data_path).expanduser().resolve()


def data_path_display(path: Path, data_path: Path) -> str:
    try:
        relative_path = path.relative_to(data_path)
    except ValueError:
        return str(path)
    return str(relative_path)


def portable_string(value: str, data_path: Path | None) -> str:
    if data_path is None:
        return value

    path = Path(value)
    if not path.is_absolute():
        return value

    return data_path_display(path, data_path)


def format_markdown_value(value: object, data_path: Path | None = None) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:g}"
    return (
        portable_string(str(value), data_path)
        .replace("\n", "<br>")
        .replace(
            "|",
            r"\|",
        )
    )


def markdown_table(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
    data_path: Path | None = None,
) -> str:
    if columns is not None:
        frame = frame.loc[:, list(columns)]
    if frame.empty:
        return "_No rows._\n"

    headers = list(frame.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend(
        "| "
        + " | ".join(format_markdown_value(value, data_path) for value in row)
        + " |"
        for row in frame.itertuples(index=False, name=None)
    )
    return "\n".join(lines) + "\n"


def read_optional_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def artifact_summary(data_path: Path) -> pd.DataFrame:
    generated_path = data_path / "generated"
    paths = {
        "neighborhood_features": generated_path / "col_final.gpkg",
        "transactions": generated_path / "transactions_final.parquet",
        "sector_cluster_config_summary": (
            generated_path / "sector_cluster_config_summary.parquet"
        ),
        "sector_cluster_threshold_audit": (
            generated_path / "sector_cluster_threshold_audit.parquet"
        ),
    }
    rows = []
    for artifact, path in paths.items():
        rows.append(
            {
                "artifact": artifact,
                "path": data_path_display(path, data_path),
                "exists": path.exists(),
            },
        )
    return pd.DataFrame(rows)


def feature_description(reason: object) -> str:
    return FEATURE_DESCRIPTION_BY_REASON.get(str(reason), str(reason))


def feature_derivation(transform: object) -> str:
    return FEATURE_DERIVATION_BY_TRANSFORM.get(str(transform), str(transform))


def sector_cluster_family_for_column(column: object) -> str | None:
    column_name = str(column)
    sector_families = sorted(
        SECTOR_CLUSTER_FAMILY_BY_PREFIX.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for prefix, family in sector_families:
        starts_with_sector_pattern = column_name.startswith(
            (
                f"{prefix}_",
                f"nearest_{prefix}_",
                f"distance_to_{prefix}_",
                f"intersects_{prefix}_",
                f"log_{prefix}_",
            ),
        )
        if starts_with_sector_pattern or f"_of_{prefix}_cluster" in column_name:
            return family
    return None


def build_documentation_catalog(feature_catalog: pd.DataFrame) -> pd.DataFrame:
    catalog = feature_catalog.assign(
        prepared_column=lambda df: df["model_column"],
        feature_derivation=lambda df: df["transform"].map(
            feature_derivation,
        ),
        feature_description=lambda df: df["reason"].map(feature_description),
    )
    sector_cluster_families = catalog["source_column"].map(
        sector_cluster_family_for_column,
    )
    sector_cluster_mask = sector_cluster_families.notna()
    catalog.loc[sector_cluster_mask, "family"] = sector_cluster_families.loc[
        sector_cluster_mask
    ]
    raw_sector_cluster_mask = sector_cluster_mask & catalog["prepared_column"].isna()
    catalog.loc[
        raw_sector_cluster_mask,
        "feature_description",
    ] = SECTOR_CLUSTER_RAW_DESCRIPTION
    return catalog.loc[
        :,
        [
            "source_column",
            "prepared_column",
            "family",
            "feature_derivation",
            "scale_denominator",
            "feature_description",
        ],
    ].sort_values(["family", "source_column"])


def build_documentation_summary(documentation_catalog: pd.DataFrame) -> pd.DataFrame:
    return (
        documentation_catalog.groupby(
            ["family", "feature_description"],
            dropna=False,
        )
        .size()
        .reset_index(name="columns")
        .sort_values(["family", "feature_description"])
    )


def neutralize_sector_thresholds(sector_thresholds: pd.DataFrame) -> pd.DataFrame:
    return sector_thresholds.rename(columns=SECTOR_THRESHOLD_COLUMN_NAMES)


def build_feature_catalog_document(data_path: Path) -> str:
    generated_path = data_path / "generated"
    neighborhood_features_path = generated_path / "col_final.gpkg"
    if not neighborhood_features_path.exists():
        msg = f"Missing neighborhood feature artifact: {neighborhood_features_path}"
        raise FileNotFoundError(msg)

    neighborhood_raw = gpd.read_file(neighborhood_features_path)
    feature_catalog = build_feature_catalog(neighborhood_raw)
    feature_catalog = feature_catalog.loc[:, SOURCE_CATALOG_COLUMNS]
    documentation_catalog = build_documentation_catalog(feature_catalog)
    feature_summary = build_documentation_summary(documentation_catalog)

    sector_thresholds = read_optional_parquet(
        generated_path / "sector_cluster_threshold_audit.parquet",
    )
    if sector_thresholds is not None:
        sector_thresholds = neutralize_sector_thresholds(sector_thresholds)

    sector_config = read_optional_parquet(
        generated_path / "sector_cluster_config_summary.parquet",
    )

    parts = [
        "# Generated Feature Catalog",
        "",
        "This file is generated by `scripts/generate_doc_tables.py` from the "
        "current `DATA_PATH/generated/col_final.gpkg` artifact and the "
        "project feature catalog helper.",
        "",
        "Paths in generated tables are relative to `DATA_PATH` unless otherwise noted.",
        "",
        "Families ending in `_cluster` are sector-cluster exposure features "
        "generated by `notebooks/10_cluster_statistics.py`; see "
        "`docs/neighborhood-feature-dictionary.md` for methodology.",
        "",
        "## Artifact Availability",
        "",
        markdown_table(artifact_summary(data_path), data_path=data_path),
        "## Feature Family Summary",
        "",
        markdown_table(feature_summary, data_path=data_path),
        "## Feature Catalog",
        "",
        markdown_table(documentation_catalog, data_path=data_path),
    ]

    if sector_config is not None:
        parts.extend(
            [
                "## Sector Cluster Configuration",
                "",
                markdown_table(sector_config, data_path=data_path),
            ],
        )

    if sector_thresholds is not None:
        parts.extend(
            [
                "## Sector Cluster Threshold Audit",
                "",
                markdown_table(sector_thresholds, data_path=data_path),
            ],
        )

    return "\n".join(parts).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    data_path = resolve_data_path(args.data_path)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_feature_catalog_document(data_path),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
