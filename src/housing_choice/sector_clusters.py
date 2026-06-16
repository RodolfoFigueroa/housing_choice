from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from esda.getisord import G_Local
from esda.moran import Moran, Moran_Local
from libpysal.weights import W

if TYPE_CHECKING:
    from pyproj import CRS
    from sqlalchemy.engine import Engine


PER_OCU_TO_NUM_WORKERS_MAP = {
    "0 a 5 personas": 3,
    "6 a 10 personas": 8,
    "11 a 30 personas": 20,
    "31 a 50 personas": 40,
    "51 a 100 personas": 75,
    "101 a 250 personas": 175,
    "251 y m?s personas": 500,
    "251 y mas personas": 500,
    "251 y más personas": 500,
}


@dataclass(frozen=True)
class SectorClusterConfig:
    sector_name: str
    output_prefix: str
    scian_prefixes: tuple[str, ...]
    diagnostics_path: Path
    denue_table: str = "denue_2025_05"
    crs: str = "EPSG:6372"
    buffer_m: float = 10_000
    grid_size_m: int = 250
    hotspot_min_jobs: float = 500
    hotspot_min_businesses: int = 2
    spatial_permutations: int = 199
    spatial_random_seed: int = 42
    distance_bands_km: tuple[float, ...] = (0.5, 1.0, 2.0, 5.0)


@dataclass(frozen=True)
class SectorClusterResult:
    config: SectorClusterConfig
    points: gpd.GeoDataFrame
    point_summary: pd.DataFrame
    grid: gpd.GeoDataFrame
    grid_summary: pd.DataFrame
    hotspot_grid: gpd.GeoDataFrame
    spatial_stats_summary: pd.DataFrame
    hotspot_cells: gpd.GeoDataFrame
    clusters: gpd.GeoDataFrame
    cluster_summary: pd.DataFrame
    neighborhood_features: gpd.GeoDataFrame
    neighborhood_feature_frame: pd.DataFrame
    cluster_feature_cols: list[str]
    neighborhood_feature_summary: pd.DataFrame


MANUFACTURING_CLUSTER_CONFIG = SectorClusterConfig(
    sector_name="manufacturing",
    output_prefix="mfg",
    scian_prefixes=("31", "32", "33"),
    diagnostics_path=Path("./data/processed/mfg_spatial_diagnostics.gpkg"),
)

LOGISTICS_CLUSTER_CONFIG = SectorClusterConfig(
    sector_name="logistics",
    output_prefix="logistics",
    scian_prefixes=("48", "49"),
    diagnostics_path=Path("./data/processed/logistics_spatial_diagnostics.gpkg"),
)


def build_sector_cluster_analysis(
    df_col: gpd.GeoDataFrame,
    engine: Engine,
    config: SectorClusterConfig,
) -> SectorClusterResult:
    points, point_summary, bounds = load_sector_points(df_col, engine, config)
    grid, grid_summary, grid_n_cols, grid_n_rows = aggregate_jobs_to_grid(
        points,
        bounds,
        config,
    )
    weight_neighbors, weights, weights_binary = build_spatial_weights(
        grid,
        grid_n_cols,
        grid_n_rows,
    )
    hotspot_grid, spatial_stats_summary = detect_hotspots(
        grid,
        weights,
        weights_binary,
        config,
    )
    clusters, hotspot_cells, cluster_summary = build_hotspot_clusters(
        grid,
        hotspot_grid,
        weight_neighbors,
        config,
    )
    (
        neighborhood_features,
        neighborhood_feature_frame,
        cluster_feature_cols,
        neighborhood_feature_summary,
    ) = compute_neighborhood_cluster_features(df_col, clusters, config)
    return SectorClusterResult(
        config=config,
        points=points,
        point_summary=point_summary,
        grid=grid,
        grid_summary=grid_summary,
        hotspot_grid=hotspot_grid,
        spatial_stats_summary=spatial_stats_summary,
        hotspot_cells=hotspot_cells,
        clusters=clusters,
        cluster_summary=cluster_summary,
        neighborhood_features=neighborhood_features,
        neighborhood_feature_frame=neighborhood_feature_frame,
        cluster_feature_cols=cluster_feature_cols,
        neighborhood_feature_summary=neighborhood_feature_summary,
    )


def load_sector_points(
    df_col: gpd.GeoDataFrame,
    engine: Engine,
    config: SectorClusterConfig,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, tuple[float, float, float, float]]:
    xmin, ymin, xmax, ymax = df_col.buffer(config.buffer_m).total_bounds
    table_name = _sql_identifier(config.denue_table)
    prefix_filters = " OR ".join(
        f"codigo_act LIKE %(prefix_{idx})s"
        for idx, _prefix in enumerate(config.scian_prefixes)
    )
    params = {
        "xmin": int(xmin),
        "ymin": int(ymin),
        "xmax": int(xmax),
        "ymax": int(ymax),
        **{
            f"prefix_{idx}": f"{prefix}%"
            for idx, prefix in enumerate(config.scian_prefixes)
        },
    }

    with engine.connect() as conn:
        points = gpd.read_postgis(
            f"""
            SELECT codigo_act, per_ocu, geometry
            FROM {table_name}
            WHERE
                ({prefix_filters})
                AND (
                    geometry
                    && ST_MakeEnvelope(
                        %(xmin)s, %(ymin)s, %(xmax)s, %(ymax)s, 6372
                    )
                )
            """,  # noqa: S608 - table name is validated by _sql_identifier.
            conn,
            geom_col="geometry",
            params=params,
        )

    points = points.assign(
        num_jobs=lambda frame: frame["per_ocu"].map(PER_OCU_TO_NUM_WORKERS_MAP),
    ).drop(columns=["per_ocu"])
    point_summary = pd.DataFrame(
        [
            {
                "sector": config.sector_name,
                "businesses": len(points),
                "jobs": float(points["num_jobs"].sum()),
                "min_x": xmin,
                "min_y": ymin,
                "max_x": xmax,
                "max_y": ymax,
            },
        ],
    )
    return points, point_summary, (xmin, ymin, xmax, ymax)


def aggregate_jobs_to_grid(
    points: gpd.GeoDataFrame,
    bounds: tuple[float, float, float, float],
    config: SectorClusterConfig,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, int, int]:
    xmin, ymin, xmax, ymax = bounds
    n_cols = int((xmax - xmin) / config.grid_size_m) + 1
    n_rows = int((ymax - ymin) / config.grid_size_m) + 1
    boxes = [
        shapely.box(
            xmin + col * config.grid_size_m,
            ymin + row * config.grid_size_m,
            xmin + (col + 1) * config.grid_size_m,
            ymin + (row + 1) * config.grid_size_m,
        )
        for col in range(n_cols)
        for row in range(n_rows)
    ]
    grid_cells = gpd.GeoDataFrame(geometry=boxes, crs=config.crs).reset_index(
        names="grid_idx",
    )
    grid_cells["grid_col"] = grid_cells["grid_idx"] // n_rows
    grid_cells["grid_row"] = grid_cells["grid_idx"] % n_rows

    joined_points = grid_cells.sjoin(
        points[["num_jobs", "geometry"]],
        how="left",
        predicate="contains",
    )
    cell_stats = joined_points.groupby("grid_idx", as_index=False).agg(
        num_jobs=("num_jobs", "sum"),
        num_businesses=("num_jobs", "count"),
    )
    grid = grid_cells.merge(cell_stats, on="grid_idx", how="left")
    grid["num_jobs"] = grid["num_jobs"].fillna(0.0)
    grid["num_businesses"] = grid["num_businesses"].fillna(0).astype(int)
    grid["cell_area_km2"] = grid.geometry.area / 1_000_000
    grid["jobs_per_km2"] = grid["num_jobs"] / grid["cell_area_km2"]
    grid["log_jobs"] = np.log1p(grid["num_jobs"])

    grid_summary = pd.DataFrame(
        [
            {
                "sector": config.sector_name,
                "grid_cells": len(grid),
                "cells_with_businesses": int(grid["num_businesses"].gt(0).sum()),
                "businesses": int(grid["num_businesses"].sum()),
                "jobs": float(grid["num_jobs"].sum()),
            },
        ],
    )
    return grid, grid_summary, n_cols, n_rows


def build_spatial_weights(
    grid: gpd.GeoDataFrame,
    n_cols: int,
    n_rows: int,
) -> tuple[dict[int, list[int]], W, W]:
    neighbor_offsets = [
        (delta_col, delta_row)
        for delta_col in (-1, 0, 1)
        for delta_row in (-1, 0, 1)
        if not (delta_col == 0 and delta_row == 0)
    ]
    weight_neighbors: dict[int, list[int]] = {}
    for idx, col, row in grid[["grid_idx", "grid_col", "grid_row"]].itertuples(
        index=False,
        name=None,
    ):
        neighbor_ids = []
        for delta_col, delta_row in neighbor_offsets:
            neighbor_col = int(col) + delta_col
            neighbor_row = int(row) + delta_row
            if 0 <= neighbor_col < n_cols and 0 <= neighbor_row < n_rows:
                neighbor_ids.append(int(neighbor_col * n_rows + neighbor_row))
        weight_neighbors[int(idx)] = neighbor_ids

    weights = W(weight_neighbors, silence_warnings=True)
    weights.transform = "R"
    weights_binary = W(weight_neighbors, silence_warnings=True)
    weights_binary.transform = "B"
    return weight_neighbors, weights, weights_binary


def detect_hotspots(
    grid: gpd.GeoDataFrame,
    weights: W,
    weights_binary: W,
    config: SectorClusterConfig,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    values = grid["num_jobs"].to_numpy(dtype=float)
    moran_jobs = Moran(values, weights, permutations=config.spatial_permutations)
    local_moran_jobs = Moran_Local(
        values,
        weights,
        permutations=config.spatial_permutations,
        seed=config.spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )
    getis_ord_jobs = G_Local(
        values,
        weights_binary,
        transform="B",
        permutations=config.spatial_permutations,
        star=True,
        seed=config.spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )

    hotspot_grid = grid.copy()
    hotspot_grid["local_moran_i"] = local_moran_jobs.Is
    hotspot_grid["local_moran_q"] = local_moran_jobs.q
    hotspot_grid["local_moran_p"] = local_moran_jobs.p_sim
    hotspot_grid["getis_ord_g"] = getis_ord_jobs.Gs
    hotspot_grid["getis_ord_z"] = getis_ord_jobs.Zs
    hotspot_grid["getis_ord_p"] = getis_ord_jobs.p_sim
    hotspot_grid["is_local_high_high"] = hotspot_grid["local_moran_q"].eq(1) & (
        hotspot_grid["local_moran_p"].lt(0.05)
    )
    hotspot_grid["is_gi_hotspot"] = hotspot_grid["getis_ord_z"].gt(1.96) & (
        hotspot_grid["getis_ord_p"].lt(0.05)
    )
    hotspot_grid["is_hotspot_candidate"] = hotspot_grid["is_gi_hotspot"] & (
        hotspot_grid["num_jobs"].gt(0) | hotspot_grid["is_local_high_high"]
    )

    spatial_stats_summary = pd.DataFrame(
        [
            {
                "sector": config.sector_name,
                "statistic": "Global Moran's I",
                "value": moran_jobs.I,
                "expected_value": moran_jobs.EI,
                "p_sim": moran_jobs.p_sim,
                "permutations": config.spatial_permutations,
            },
            {
                "sector": config.sector_name,
                "statistic": "Local Moran high-high cells",
                "value": int(hotspot_grid["is_local_high_high"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": config.spatial_permutations,
            },
            {
                "sector": config.sector_name,
                "statistic": "Getis-Ord Gi* hotspot cells",
                "value": int(hotspot_grid["is_gi_hotspot"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": config.spatial_permutations,
            },
            {
                "sector": config.sector_name,
                "statistic": "Selected hotspot candidate cells",
                "value": int(hotspot_grid["is_hotspot_candidate"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": config.spatial_permutations,
            },
        ],
    )
    return hotspot_grid, spatial_stats_summary


def build_hotspot_clusters(
    grid: gpd.GeoDataFrame,
    hotspot_grid: gpd.GeoDataFrame,
    weight_neighbors: dict[int, list[int]],
    config: SectorClusterConfig,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    selected_hotspot_ids = set(
        hotspot_grid.loc[hotspot_grid["is_hotspot_candidate"], "grid_idx"].astype(int),
    )
    hotspot_cells = hotspot_grid.loc[hotspot_grid["is_hotspot_candidate"]].copy()
    if hotspot_cells.empty:
        hotspot_cells["cluster_id"] = pd.Series(dtype="Int64")
        clusters_all = _empty_cluster_frame(grid.crs)
    else:
        component_by_cell = assign_connected_components(
            selected_hotspot_ids,
            weight_neighbors,
        )
        hotspot_cells["cluster_id"] = (
            hotspot_cells["grid_idx"].astype(int).map(component_by_cell)
        )
        clusters_all = (
            hotspot_cells.dissolve(
                by="cluster_id",
                aggfunc={
                    "grid_idx": "count",
                    "num_jobs": "sum",
                    "num_businesses": "sum",
                    "getis_ord_z": "max",
                    "getis_ord_p": "min",
                    "local_moran_i": "max",
                },
            )
            .rename(
                columns={
                    "grid_idx": "num_cells",
                    "getis_ord_z": "max_getis_ord_z",
                    "getis_ord_p": "min_getis_ord_p",
                    "local_moran_i": "max_local_moran_i",
                },
            )
            .reset_index()
        )
        clusters_all["area_km2"] = clusters_all.geometry.area / 1_000_000
        clusters_all["jobs_per_km2"] = (
            clusters_all["num_jobs"] / clusters_all["area_km2"]
        )
        clusters_all["centroid_x"] = clusters_all.geometry.centroid.x
        clusters_all["centroid_y"] = clusters_all.geometry.centroid.y
        clusters_all["passes_cluster_threshold"] = clusters_all["num_jobs"].ge(
            config.hotspot_min_jobs,
        ) & clusters_all["num_businesses"].ge(config.hotspot_min_businesses)

    clusters_all = clusters_all.sort_values(
        ["num_jobs", "area_km2"],
        ascending=[False, False],
    ).reset_index(drop=True)
    clusters_all["cluster_rank_all"] = np.arange(1, len(clusters_all) + 1)
    clusters = clusters_all.loc[clusters_all["passes_cluster_threshold"]].copy()
    clusters = clusters.sort_values(
        ["num_jobs", "area_km2"],
        ascending=[False, False],
    ).reset_index(drop=True)
    clusters["cluster_rank"] = np.arange(1, len(clusters) + 1)
    cluster_summary = clusters[
        [
            "cluster_rank",
            "cluster_id",
            "num_cells",
            "num_jobs",
            "num_businesses",
            "area_km2",
            "jobs_per_km2",
            "max_getis_ord_z",
            "min_getis_ord_p",
            "centroid_x",
            "centroid_y",
        ]
    ].copy()
    cluster_summary.insert(0, "sector", config.sector_name)
    return clusters, hotspot_cells, cluster_summary


def assign_connected_components(
    selected_ids: set[int],
    neighbors: dict[int, list[int]],
) -> dict[int, int]:
    component_by_cell = {}
    component_id = 0
    for start in sorted(selected_ids):
        if start in component_by_cell:
            continue
        component_id += 1
        stack = [start]
        component_by_cell[start] = component_id
        while stack:
            cell = stack.pop()
            for neighbor in neighbors[cell]:
                if neighbor in selected_ids and neighbor not in component_by_cell:
                    component_by_cell[neighbor] = component_id
                    stack.append(neighbor)
    return component_by_cell


def compute_neighborhood_cluster_features(
    df_col: gpd.GeoDataFrame,
    clusters: gpd.GeoDataFrame,
    config: SectorClusterConfig,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, list[str], pd.DataFrame]:
    neighborhood_base = (
        df_col[["name", "name_detail", "geometry"]]
        .copy()
        .reset_index(names="neighborhood_idx")
    )
    exposure_records = _cluster_exposure_records(
        neighborhood_base,
        clusters,
        config,
    )
    exposure_frame = pd.DataFrame(exposure_records)
    neighborhood_features = neighborhood_base.merge(
        exposure_frame,
        on="neighborhood_idx",
        how="left",
    )
    cluster_feature_cols = [
        col
        for col in neighborhood_features.columns
        if col not in {"neighborhood_idx", "name", "name_detail", "geometry"}
    ]
    neighborhood_feature_frame = neighborhood_features.set_index("name_detail")[
        cluster_feature_cols
    ]
    neighborhood_feature_summary = (
        neighborhood_features[
            [
                "name_detail",
                f"nearest_{config.output_prefix}_cluster_rank",
                f"nearest_{config.output_prefix}_cluster_jobs",
                f"{config.output_prefix}_distance_nearest_cluster_km",
                f"{config.output_prefix}_jobs_within_2km",
                f"log_{config.output_prefix}_jobs_within_2km",
                f"{config.output_prefix}_cluster_gravity_inv_sq",
                f"log_{config.output_prefix}_cluster_gravity_inv_sq",
                f"intersects_{config.output_prefix}_cluster",
                f"within_1km_of_{config.output_prefix}_cluster",
            ]
        ]
        .rename(
            columns={
                f"nearest_{config.output_prefix}_cluster_rank": "nearest_cluster_rank",
                f"nearest_{config.output_prefix}_cluster_jobs": "nearest_cluster_jobs",
                (
                    f"{config.output_prefix}_distance_nearest_cluster_km"
                ): "distance_nearest_cluster_km",
                f"{config.output_prefix}_jobs_within_2km": "jobs_within_2km",
                f"log_{config.output_prefix}_jobs_within_2km": "log_jobs_within_2km",
                (
                    f"{config.output_prefix}_cluster_gravity_inv_sq"
                ): "cluster_gravity_inv_sq",
                (
                    f"log_{config.output_prefix}_cluster_gravity_inv_sq"
                ): "log_cluster_gravity_inv_sq",
                f"intersects_{config.output_prefix}_cluster": "intersects_cluster",
                (
                    f"within_1km_of_{config.output_prefix}_cluster"
                ): "within_1km_of_cluster",
            },
        )
        .assign(sector=config.sector_name)
        .loc[
            :,
            [
                "sector",
                "name_detail",
                "nearest_cluster_rank",
                "nearest_cluster_jobs",
                "distance_nearest_cluster_km",
                "jobs_within_2km",
                "log_jobs_within_2km",
                "cluster_gravity_inv_sq",
                "log_cluster_gravity_inv_sq",
                "intersects_cluster",
                "within_1km_of_cluster",
            ],
        ]
        .sort_values(["sector", "distance_nearest_cluster_km"])
    )
    return (
        neighborhood_features,
        neighborhood_feature_frame,
        cluster_feature_cols,
        neighborhood_feature_summary,
    )


def export_sector_cluster_diagnostics(result: SectorClusterResult) -> Path:
    output_path = result.config.diagnostics_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    result.hotspot_grid.to_file(
        output_path,
        layer=f"{result.config.output_prefix}_hotspot_grid",
        driver="GPKG",
    )
    result.hotspot_cells.to_file(
        output_path,
        layer=f"{result.config.output_prefix}_hotspot_cells",
        driver="GPKG",
    )
    result.clusters.to_file(
        output_path,
        layer=f"{result.config.output_prefix}_clusters",
        driver="GPKG",
    )
    return output_path


def _cluster_exposure_records(
    neighborhoods: gpd.GeoDataFrame,
    clusters: gpd.GeoDataFrame,
    config: SectorClusterConfig,
) -> list[dict[str, object]]:
    neighborhood_indices = neighborhoods["neighborhood_idx"].astype(int).tolist()
    geometries = cast("list[shapely.Geometry]", neighborhoods.geometry.tolist())
    if clusters.empty:
        return [
            empty_cluster_exposure_record(
                neighborhood_idx,
                config.output_prefix,
                config.distance_bands_km,
            )
            for neighborhood_idx in neighborhood_indices
        ]

    cluster_table = clusters[
        [
            "cluster_id",
            "cluster_rank",
            "num_jobs",
            "num_businesses",
            "jobs_per_km2",
            "area_km2",
            "geometry",
        ]
    ].copy()
    cluster_centroids = cluster_table.geometry.centroid
    cluster_jobs = cluster_table["num_jobs"].to_numpy(dtype=float)
    records = []
    for neighborhood_idx, geometry in zip(
        neighborhood_indices,
        geometries,
        strict=True,
    ):
        boundary_distances_m = cluster_table.geometry.distance(geometry).to_numpy(
            dtype=float,
        )
        point = shapely.point_on_surface(geometry)
        centroid_distances_m = cluster_centroids.distance(point).to_numpy(dtype=float)
        nearest_position = int(boundary_distances_m.argmin())
        nearest_cluster = cluster_table.iloc[nearest_position]
        distance_km = boundary_distances_m / 1000
        record = _nearest_cluster_record(
            neighborhood_idx,
            nearest_cluster,
            (boundary_distances_m, centroid_distances_m, nearest_position),
            config.output_prefix,
        )
        gravity_inv_sq = float((cluster_jobs / np.power(distance_km + 0.25, 2)).sum())
        gravity_exp_2km = float((cluster_jobs * np.exp(-distance_km / 2)).sum())
        record[f"{config.output_prefix}_cluster_gravity_inv_sq"] = gravity_inv_sq
        record[f"{config.output_prefix}_cluster_gravity_exp_2km"] = gravity_exp_2km
        record[f"log_{config.output_prefix}_cluster_gravity_inv_sq"] = float(
            np.log1p(gravity_inv_sq),
        )
        record[f"log_{config.output_prefix}_cluster_gravity_exp_2km"] = float(
            np.log1p(gravity_exp_2km),
        )
        for band_km in config.distance_bands_km:
            suffix = band_suffix(band_km)
            in_band = distance_km <= band_km
            band_jobs = cluster_jobs[in_band]
            record[f"{config.output_prefix}_clusters_within_{suffix}"] = int(
                in_band.sum(),
            )
            record[f"{config.output_prefix}_jobs_within_{suffix}"] = float(
                band_jobs.sum(),
            )
            record[f"log_{config.output_prefix}_jobs_within_{suffix}"] = float(
                np.log1p(band_jobs.sum()),
            )
            record[f"{config.output_prefix}_largest_cluster_jobs_within_{suffix}"] = (
                float(band_jobs.max()) if len(band_jobs) else 0.0
            )
        records.append(record)
    return records


def empty_cluster_exposure_record(
    neighborhood_idx: int,
    output_prefix: str,
    bands_km: tuple[float, ...],
) -> dict[str, object]:
    record: dict[str, object] = {
        "neighborhood_idx": neighborhood_idx,
        f"nearest_{output_prefix}_cluster_id": np.nan,
        f"nearest_{output_prefix}_cluster_rank": np.nan,
        f"nearest_{output_prefix}_cluster_jobs": np.nan,
        f"nearest_{output_prefix}_cluster_businesses": np.nan,
        f"nearest_{output_prefix}_cluster_jobs_per_km2": np.nan,
        f"nearest_{output_prefix}_cluster_area_km2": np.nan,
        f"distance_to_{output_prefix}_cluster_boundary_m": np.nan,
        f"distance_to_{output_prefix}_cluster_centroid_m": np.nan,
        f"{output_prefix}_distance_nearest_cluster_km": np.nan,
        f"{output_prefix}_distance_nearest_cluster_centroid_km": np.nan,
        f"intersects_{output_prefix}_cluster": False,
        f"within_500m_of_{output_prefix}_cluster": False,
        f"within_1km_of_{output_prefix}_cluster": False,
        f"within_2km_of_{output_prefix}_cluster": False,
        f"{output_prefix}_cluster_gravity_inv_sq": 0.0,
        f"{output_prefix}_cluster_gravity_exp_2km": 0.0,
        f"log_{output_prefix}_cluster_gravity_inv_sq": 0.0,
        f"log_{output_prefix}_cluster_gravity_exp_2km": 0.0,
    }
    for band_km in bands_km:
        suffix = band_suffix(band_km)
        record[f"{output_prefix}_clusters_within_{suffix}"] = 0
        record[f"{output_prefix}_jobs_within_{suffix}"] = 0.0
        record[f"log_{output_prefix}_jobs_within_{suffix}"] = 0.0
        record[f"{output_prefix}_largest_cluster_jobs_within_{suffix}"] = 0.0
    return record


def band_suffix(distance_km: float) -> str:
    if float(distance_km).is_integer():
        return f"{int(distance_km)}km"
    return f"{int(distance_km * 1000)}m"


def _nearest_cluster_record(
    neighborhood_idx: int,
    nearest_cluster: pd.Series,
    distance_context: tuple[np.ndarray, np.ndarray, int],
    output_prefix: str,
) -> dict[str, object]:
    boundary_distances_m, centroid_distances_m, nearest_position = distance_context
    return {
        "neighborhood_idx": neighborhood_idx,
        f"nearest_{output_prefix}_cluster_id": nearest_cluster["cluster_id"],
        f"nearest_{output_prefix}_cluster_rank": nearest_cluster["cluster_rank"],
        f"nearest_{output_prefix}_cluster_jobs": float(nearest_cluster["num_jobs"]),
        f"nearest_{output_prefix}_cluster_businesses": int(
            nearest_cluster["num_businesses"],
        ),
        f"nearest_{output_prefix}_cluster_jobs_per_km2": float(
            nearest_cluster["jobs_per_km2"],
        ),
        f"nearest_{output_prefix}_cluster_area_km2": float(
            nearest_cluster["area_km2"],
        ),
        f"distance_to_{output_prefix}_cluster_boundary_m": float(
            boundary_distances_m[nearest_position],
        ),
        f"distance_to_{output_prefix}_cluster_centroid_m": float(
            centroid_distances_m[nearest_position],
        ),
        f"{output_prefix}_distance_nearest_cluster_km": float(
            boundary_distances_m[nearest_position] / 1000,
        ),
        f"{output_prefix}_distance_nearest_cluster_centroid_km": float(
            centroid_distances_m[nearest_position] / 1000,
        ),
        f"intersects_{output_prefix}_cluster": bool(
            boundary_distances_m[nearest_position] <= 0,
        ),
        f"within_500m_of_{output_prefix}_cluster": bool(
            boundary_distances_m[nearest_position] <= 500,
        ),
        f"within_1km_of_{output_prefix}_cluster": bool(
            boundary_distances_m[nearest_position] <= 1_000,
        ),
        f"within_2km_of_{output_prefix}_cluster": bool(
            boundary_distances_m[nearest_position] <= 2_000,
        ),
    }


def _empty_cluster_frame(crs: CRS | str | int | None) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        columns=[
            "cluster_id",
            "num_cells",
            "num_jobs",
            "num_businesses",
            "max_getis_ord_z",
            "min_getis_ord_p",
            "max_local_moran_i",
            "area_km2",
            "jobs_per_km2",
            "centroid_x",
            "centroid_y",
            "passes_cluster_threshold",
            "geometry",
        ],
        geometry="geometry",
        crs=crs,
    )


def _sql_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        msg = f"Invalid SQL identifier: {identifier!r}"
        raise ValueError(msg)
    return identifier
