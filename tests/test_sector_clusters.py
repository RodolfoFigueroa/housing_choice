from __future__ import annotations

from pathlib import Path
from unittest import TestCase

import geopandas as gpd
import pandas as pd
import shapely

from housing_choice.sector_clusters import (
    LOGISTICS_CLUSTER_CONFIG,
    SectorClusterConfig,
    assign_connected_components,
    build_hotspot_clusters,
    compute_neighborhood_cluster_features,
    empty_cluster_exposure_record,
)


class SectorClusterTest(TestCase):
    def test_logistics_column_names_match_prefix_contract(self) -> None:
        record = empty_cluster_exposure_record(
            neighborhood_idx=7,
            output_prefix=LOGISTICS_CLUSTER_CONFIG.output_prefix,
            bands_km=LOGISTICS_CLUSTER_CONFIG.distance_bands_km,
        )

        assert "nearest_logistics_cluster_jobs" in record
        assert "logistics_distance_nearest_cluster_km" in record
        assert "log_logistics_jobs_within_2km" in record
        assert "log_logistics_cluster_gravity_inv_sq" in record
        assert record["logistics_jobs_within_500m"] == 0.0

    def test_empty_clusters_return_stable_defaults(self) -> None:
        neighborhoods = gpd.GeoDataFrame(
            {
                "name": ["a"],
                "name_detail": ["a"],
                "geometry": [shapely.box(0, 0, 100, 100)],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )
        clusters = gpd.GeoDataFrame(
            columns=["cluster_id", "cluster_rank", "geometry"],
            geometry="geometry",
            crs="EPSG:6372",
        )

        features, feature_frame, feature_cols, _summary = (
            compute_neighborhood_cluster_features(
                neighborhoods,
                clusters,
                LOGISTICS_CLUSTER_CONFIG,
            )
        )

        assert len(features) == 1
        assert "logistics_jobs_within_2km" in feature_cols
        assert feature_frame.loc["a", "logistics_jobs_within_2km"] == 0.0
        assert not feature_frame.loc["a", "within_1km_of_logistics_cluster"]
        assert pd.isna(
            feature_frame.loc["a", "logistics_distance_nearest_cluster_km"],
        )

    def test_distance_band_jobs_are_monotone(self) -> None:
        neighborhoods = gpd.GeoDataFrame(
            {
                "name": ["a"],
                "name_detail": ["a"],
                "geometry": [shapely.box(0, 0, 100, 100)],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )
        clusters = gpd.GeoDataFrame(
            {
                "cluster_id": [1, 2],
                "cluster_rank": [1, 2],
                "num_jobs": [100.0, 400.0],
                "num_businesses": [2, 3],
                "jobs_per_km2": [100.0, 400.0],
                "area_km2": [1.0, 1.0],
                "geometry": [
                    shapely.box(200, 0, 300, 100),
                    shapely.box(1500, 0, 1600, 100),
                ],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )

        _features, feature_frame, _feature_cols, _summary = (
            compute_neighborhood_cluster_features(
                neighborhoods,
                clusters,
                LOGISTICS_CLUSTER_CONFIG,
            )
        )
        row = feature_frame.loc["a"]

        assert row["logistics_jobs_within_500m"] == 100.0
        assert row["logistics_jobs_within_1km"] == 100.0
        assert row["logistics_jobs_within_2km"] == 500.0
        assert row["logistics_jobs_within_2km"] >= row["logistics_jobs_within_1km"]

    def test_neighborhood_summary_uses_sector_tidy_columns(self) -> None:
        neighborhoods = gpd.GeoDataFrame(
            {
                "name": ["a"],
                "name_detail": ["a"],
                "geometry": [shapely.box(0, 0, 100, 100)],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )
        clusters = gpd.GeoDataFrame(
            {
                "cluster_id": [1],
                "cluster_rank": [1],
                "num_jobs": [100.0],
                "num_businesses": [2],
                "jobs_per_km2": [100.0],
                "area_km2": [1.0],
                "geometry": [shapely.box(200, 0, 300, 100)],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )

        _features, _feature_frame, _feature_cols, summary = (
            compute_neighborhood_cluster_features(
                neighborhoods,
                clusters,
                LOGISTICS_CLUSTER_CONFIG,
            )
        )

        assert "sector" in summary.columns
        assert "nearest_cluster_jobs" in summary.columns
        assert "nearest_logistics_cluster_jobs" not in summary.columns
        assert summary.loc[0, "sector"] == "logistics"
        assert summary.loc[0, "nearest_cluster_jobs"] == 100.0

    def test_connected_components_split_disconnected_hotspots(self) -> None:
        components = assign_connected_components(
            selected_ids={0, 1, 4},
            neighbors={0: [1], 1: [0], 4: []},
        )

        assert components[0] == components[1]
        assert components[0] != components[4]

    def test_hotspot_clusters_apply_thresholds(self) -> None:
        config = SectorClusterConfig(
            sector_name="test",
            output_prefix="test",
            scian_prefixes=("99",),
            diagnostics_path=Path("test.gpkg"),
            hotspot_min_jobs=500,
            hotspot_min_businesses=2,
        )
        grid = gpd.GeoDataFrame(
            {
                "grid_idx": [0, 1],
                "grid_col": [0, 1],
                "grid_row": [0, 0],
                "num_jobs": [300.0, 300.0],
                "num_businesses": [1, 1],
                "geometry": [
                    shapely.box(0, 0, 250, 250),
                    shapely.box(250, 0, 500, 250),
                ],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )
        hotspot_grid = grid.assign(
            is_hotspot_candidate=[True, True],
            getis_ord_z=[2.0, 2.1],
            getis_ord_p=[0.01, 0.02],
            local_moran_i=[1.0, 1.1],
        )

        clusters, hotspot_cells, cluster_summary = build_hotspot_clusters(
            grid,
            hotspot_grid,
            {0: [1], 1: [0]},
            config,
        )

        assert len(hotspot_cells) == 2
        assert len(clusters) == 1
        assert clusters.iloc[0]["num_jobs"] == 600.0
        assert clusters.iloc[0]["cluster_rank"] == 1
        pd.testing.assert_frame_equal(
            cluster_summary[["sector", "cluster_rank", "num_jobs"]],
            pd.DataFrame(
                {"sector": ["test"], "cluster_rank": [1], "num_jobs": [600.0]},
            ),
            check_dtype=False,
        )
