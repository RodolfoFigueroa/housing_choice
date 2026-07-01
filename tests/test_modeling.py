from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

if TYPE_CHECKING:
    from collections.abc import Callable

from housing_choice.modeling import (
    add_centroid_grid_features,
    add_centroid_quadratic_features,
    add_centroid_spatial_controls,
    add_job_group_features,
    align_choice_data,
    build_active_choice_set,
    build_availability_choice_dataframe,
    build_choice_dataframe,
    build_combination_model_specs,
    build_feature_catalog,
    build_feature_diagnostics_frame,
    build_job_group_specs,
    build_single_candidate_model_specs,
    compute_feature_diagnostics,
    compute_scale_audit,
    fit_fast_availability_mnl_screen,
    fit_fast_mnl_screen,
    prepare_baseline_transactions,
    prepare_neighborhood_features,
    prepare_transactions,
    validate_availability_choice_dataframe,
    validate_choice_dataframe,
)


def sample_neighborhoods() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "name": ["a", "b"],
            "name_detail": ["A", "B"],
            "access": ["LIBRE", "RESTRINGIDO"],
            "jobs_logistics_20_2025": [100.0, 300.0],
            "jobs_mfg_20_2025": [50.0, 150.0],
            "accessibility_services": [10.0, 20.0],
            "travel_time_city_center": [600.0, 1200.0],
            "travel_time_crossing_west": [1000.0, 800.0],
            "travel_time_crossing_east": [900.0, 1000.0],
            "built_area_2020": [0.0, 10_000.0],
            "built_area_2021": [10_000.0, 30_000.0],
            "mfg_distance_nearest_cluster_km": [1.0, 2.0],
            "log_mfg_jobs_within_2km": [0.5, 1.5],
            "log_mfg_cluster_gravity_inv_sq": [0.25, 0.75],
            "logistics_distance_nearest_cluster_km": [2.0, 4.0],
            "log_logistics_jobs_within_2km": [0.3, 0.9],
            "log_logistics_cluster_gravity_inv_sq": [0.2, 0.8],
            "nearest_logistics_cluster_jobs": [100.0, 200.0],
        },
        geometry=[shapely.Point(0, 0), shapely.Point(1, 1)],
        crs="EPSG:6372",
    )


def value_error_message(callback: Callable[[], object]) -> str:
    try:
        callback()
    except ValueError as exc:
        return str(exc)
    return ""


class ModelingTest(TestCase):
    def test_feature_catalog_classifies_and_prepares_model_columns(self) -> None:
        neighborhoods = sample_neighborhoods()

        catalog = build_feature_catalog(neighborhoods)
        prepared = prepare_neighborhood_features(neighborhoods, catalog)

        roles_by_model_column = catalog.set_index("model_column")["role"].to_dict()
        assert roles_by_model_column["access_is_restricted"] == "control"
        assert roles_by_model_column["jobs_logistics_20_2025_scaled"] == "job_screen"
        assert roles_by_model_column["log_mfg_jobs_within_2km"] == "mfg_screen"
        assert (
            roles_by_model_column["logistics_distance_nearest_cluster_km_scaled"]
            == "logistics_screen"
        )
        assert "travel_time_nearest_crossing_scaled" in prepared.columns
        assert prepared.loc[1, "access_is_restricted"] == 1
        assert "nearest_logistics_cluster_jobs" not in prepared.columns

    def test_choice_data_keeps_dynamic_built_area_by_purchase_year(self) -> None:
        neighborhoods = sample_neighborhoods()
        catalog = build_feature_catalog(neighborhoods)
        prepared = prepare_neighborhood_features(neighborhoods, catalog)
        transactions_raw = pd.DataFrame(
            {
                "address": ["A", "A", "B", "C"],
                "purchase_date": [
                    "2020-01-01",
                    "2021-01-01",
                    "2021-06-01",
                    "2019-01-01",
                ],
            },
        )

        filtered, counts, wanted_names = prepare_transactions(
            transactions_raw,
            neighborhoods["name_detail"],
            2020,
            2021,
            1,
        )
        choice_features, choice_transactions, name_to_idx = align_choice_data(
            prepared,
            filtered,
            wanted_names,
        )
        choice_frame, model_feature_cols = build_choice_dataframe(
            choice_features,
            choice_transactions,
            ["access_is_restricted"],
            ["built_area_2020", "built_area_2021"],
        )

        assert counts["transactions"].sum() == 3
        assert name_to_idx == {"A": 0, "B": 1}
        assert model_feature_cols == ["access_is_restricted", "log_built_area_ha"]
        assert choice_frame.loc[0, "log_built_area_ha_0"] == 0.0
        assert choice_frame.loc[1, "log_built_area_ha_0"] == np.log1p(1.0)

    def test_single_candidate_specs_use_list_based_metadata(self) -> None:
        model_specs, summary = build_single_candidate_model_specs(
            ["control_a", "control_b"],
            ["job_a"],
            ["mfg_a"],
            ["logistics_a"],
        )

        assert list(model_specs) == [
            "baseline_no_jobs",
            "job__job_a",
            "mfg__mfg_a",
            "logistics__logistics_a",
        ]
        assert model_specs["baseline_no_jobs"]["static_cols"] == [
            "control_a",
            "control_b",
        ]
        assert model_specs["baseline_no_jobs"]["spec_kind"] == "baseline"
        assert model_specs["baseline_no_jobs"]["candidate_features"] == []
        assert model_specs["job__job_a"]["static_cols"] == [
            "job_a",
            "control_a",
            "control_b",
        ]
        assert model_specs["job__job_a"]["spec_kind"] == "single_candidate"
        assert model_specs["job__job_a"]["candidate_features"] == ["job_a"]
        assert model_specs["job__job_a"]["candidate_families"] == [
            "job_accessibility",
        ]
        job_summary = summary.loc[summary["spec_id"].eq("job__job_a")].iloc[0]
        assert job_summary["candidate_count"] == 1
        assert job_summary["candidate_features"] == ["job_a"]
        assert job_summary["all_features"].endswith("log_built_area_ha")

    def test_combination_specs_generate_two_and_three_way_candidates(self) -> None:
        model_specs, summary = build_combination_model_specs(
            ["control_a", "control_b"],
            {
                "job_accessibility": ["job_a", "job_b"],
                "manufacturing_cluster": ["mfg_a"],
                "logistics_cluster": ["logistics_a"],
            },
        )

        assert list(model_specs) == [
            "combo__job_a__mfg_a",
            "combo__job_b__mfg_a",
            "combo__job_a__logistics_a",
            "combo__job_b__logistics_a",
            "combo__mfg_a__logistics_a",
            "combo__job_a__mfg_a__logistics_a",
            "combo__job_b__mfg_a__logistics_a",
        ]
        assert "baseline_no_jobs" not in model_specs
        assert model_specs["combo__job_a__mfg_a"]["static_cols"] == [
            "job_a",
            "mfg_a",
            "control_a",
            "control_b",
        ]
        assert model_specs["combo__job_a__mfg_a"]["candidate_families"] == [
            "job_accessibility",
            "manufacturing_cluster",
        ]
        combo_summary = summary.loc[
            summary["spec_id"].eq("combo__job_a__mfg_a__logistics_a")
        ].iloc[0]
        assert combo_summary["spec_kind"] == "combination"
        assert combo_summary["candidate_count"] == 3
        assert combo_summary["candidate_features"] == [
            "job_a",
            "mfg_a",
            "logistics_a",
        ]
        assert combo_summary["all_features"].endswith("log_built_area_ha")

    def test_combination_specs_reject_ambiguous_inputs(self) -> None:
        message = value_error_message(
            lambda: build_combination_model_specs(
                ["control_a"],
                {"job_accessibility": ["shared"], "logistics_cluster": ["shared"]},
            ),
        )
        assert "candidate features must be unique" in message

        message = value_error_message(
            lambda: build_combination_model_specs(
                ["control_a"],
                {"job_accessibility": ["control_a"], "logistics_cluster": ["log_a"]},
            ),
        )
        assert "overlap with base controls" in message

        message = value_error_message(
            lambda: build_combination_model_specs(
                ["control_a"],
                {"job_accessibility": ["job_a"], "logistics_cluster": ["log_a"]},
                min_candidates=1,
            ),
        )
        assert "min_candidates must be at least 2" in message

        message = value_error_message(
            lambda: build_combination_model_specs(
                ["control_a"],
                {"job_accessibility": ["job_a"], "logistics_cluster": ["log_a"]},
                min_candidates=3,
                max_candidates=2,
            ),
        )
        assert "greater than or equal" in message

    def test_diagnostics_validation_and_fast_screen_are_finite(self) -> None:
        neighborhood_features = pd.DataFrame(
            {
                "name_detail": ["A", "B", "C"],
                "x": [0.0, 1.0, 2.0],
                "built_area_2020": [10_000.0, 20_000.0, 30_000.0],
                "built_area_2021": [20_000.0, 30_000.0, 40_000.0],
            },
        )
        transactions = pd.DataFrame(
            {
                "neighborhood_idx": [0, 1, 2, 1, 2, 0],
                "purchase_year": [2020, 2020, 2020, 2021, 2021, 2021],
            },
        )

        choice_frame, model_feature_cols = build_choice_dataframe(
            neighborhood_features,
            transactions,
            ["x"],
            ["built_area_2020", "built_area_2021"],
        )
        validation = validate_choice_dataframe(choice_frame, model_feature_cols, 3)
        diagnostic_frame = build_feature_diagnostics_frame(
            neighborhood_features,
            transactions,
            ["x"],
            ["built_area_2020", "built_area_2021"],
        )
        _, correlation, vif, max_corr = compute_feature_diagnostics(diagnostic_frame)
        screen_row, coefficients = fit_fast_mnl_screen(
            "demo",
            ["x"],
            neighborhood_features,
            transactions,
            ["built_area_2020", "built_area_2021"],
        )

        assert validation["passed"].all()
        assert correlation.shape == (2, 2)
        assert len(vif) == 2
        assert max_corr >= 0
        aic = screen_row["aic"]
        assert isinstance(aic, float)
        assert np.isfinite(aic)
        assert screen_row["null_log_likelihood"] == -6 * np.log(3)
        mcfadden_r_squared = screen_row["mcfadden_r_squared"]
        assert isinstance(mcfadden_r_squared, float)
        assert np.isfinite(mcfadden_r_squared)
        assert coefficients["feature"].tolist() == ["x", "log_built_area_ha"]

    def test_scale_audit_flags_binary_and_unscaled_columns(self) -> None:
        audit = compute_scale_audit(
            pd.DataFrame(
                {
                    "binary": [0, 1, 0],
                    "large": [100.0, 200.0, 300.0],
                    "ok": [1.0, 5.0, 10.0],
                },
            ),
            ["binary", "large", "ok"],
        )

        warnings = audit.set_index("feature")["scale_warning"].to_dict()
        assert warnings["binary"] == "binary"
        assert warnings["large"] == "too large"
        assert warnings["ok"] == "ok"

    def test_prepare_baseline_transactions_keeps_all_matched_neighborhoods(
        self,
    ) -> None:
        transactions = pd.DataFrame(
            {
                "address": ["A", "B", "C", "A"],
                "purchase_date": [
                    "2020-01-01",
                    "2021-01-01",
                    "2022-01-01",
                    "2019-01-01",
                ],
            },
        )

        prepared = prepare_baseline_transactions(transactions, ["A", "B"], 2020, 2021)

        assert prepared["neighborhood"].tolist() == ["A", "B"]
        assert prepared["purchase_year"].tolist() == [2020, 2021]
        assert prepared["transaction_id"].tolist() == [0, 1]

    def test_spatial_controls_are_centered_and_scaled_in_km(self) -> None:
        neighborhoods = gpd.GeoDataFrame(
            {
                "name_detail": ["A", "B", "C"],
                "geometry": [
                    shapely.Point(0, 0),
                    shapely.Point(1000, 2000),
                    shapely.Point(3000, 4000),
                ],
            },
            geometry="geometry",
            crs="EPSG:6372",
        )

        with_spatial = add_centroid_spatial_controls(neighborhoods)

        assert with_spatial["centroid_east_km"].tolist() == [-1.0, 0.0, 2.0]
        assert with_spatial["centroid_north_km"].tolist() == [-2.0, 0.0, 2.0]

    def test_centroid_quadratic_features_are_deterministic(self) -> None:
        neighborhoods = pd.DataFrame(
            {
                "centroid_east_km": [1.0, -2.0],
                "centroid_north_km": [3.0, 4.0],
            },
        )

        with_quadratics = add_centroid_quadratic_features(neighborhoods)

        assert with_quadratics["centroid_east_km_sq"].tolist() == [1.0, 4.0]
        assert with_quadratics["centroid_north_km_sq"].tolist() == [9.0, 16.0]
        assert with_quadratics["centroid_east_x_north_km2"].tolist() == [3.0, -8.0]

    def test_centroid_grid_features_create_eight_non_reference_dummies(
        self,
    ) -> None:
        neighborhoods = pd.DataFrame(
            {
                "centroid_east_km": [0.0, 0.1, 0.2, 1.0, 1.1, 1.2, 2.0, 2.1, 2.2],
                "centroid_north_km": [0.0, 1.0, 2.0, 0.1, 1.1, 2.1, 0.2, 1.2, 2.2],
            },
        )

        with_grid, catalog = add_centroid_grid_features(neighborhoods)

        dummy_columns = catalog.loc[
            ~catalog["is_reference"],
            "model_column",
        ].tolist()
        assert len(dummy_columns) == 8
        assert catalog.loc[
            catalog["is_reference"],
            "zone_id",
        ].tolist() == ["central_central"]
        assert "spatial_grid_3x3_central_central" not in with_grid.columns
        assert set(with_grid["spatial_grid_3x3_zone"]) == set(catalog["zone_id"])
        assert with_grid.loc[:, dummy_columns].sum(axis=1).tolist() == [
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
        ]

    def test_spatial_feature_helpers_reject_missing_centroids(self) -> None:
        message = value_error_message(
            lambda: add_centroid_quadratic_features(pd.DataFrame({"x": [1.0]})),
        )
        assert "missing spatial columns" in message

        message = value_error_message(
            lambda: add_centroid_grid_features(pd.DataFrame({"x": [1.0]})),
        )
        assert "missing spatial columns" in message

    def test_active_choice_set_excludes_focal_sale_and_forces_chosen(
        self,
    ) -> None:
        transactions = pd.DataFrame(
            {
                "transaction_id": [0, 1, 2, 3],
                "neighborhood_idx": [0, 0, 1, 2],
                "purchase_date": pd.to_datetime(
                    ["2020-01-01", "2020-01-05", "2020-01-10", "2022-01-01"],
                ),
                "purchase_year": [2020, 2020, 2020, 2022],
            },
        )

        active_choice = build_active_choice_set(
            transactions,
            3,
            window_days=10,
        )

        assert len(active_choice.transactions) == 3
        assert len(active_choice.dropped_transactions) == 1
        assert active_choice.active_sales[0].tolist() == [1.0, 1.0, 0.0]
        assert active_choice.active_sales[2].tolist() == [2.0, 0.0, 0.0]
        assert active_choice.availability[2].tolist() == [True, True, False]
        assert active_choice.summary.loc[0, "min_available_alternatives"] == 2

    def test_availability_choice_dataframe_validates_dynamic_supply(
        self,
    ) -> None:
        neighborhood_features = pd.DataFrame(
            {
                "name_detail": ["A", "B", "C"],
                "control": [0.0, 1.0, 2.0],
                "built_area_2020": [10_000.0, 20_000.0, 30_000.0],
            },
        )
        transactions = pd.DataFrame(
            {
                "transaction_id": [0, 1, 2],
                "neighborhood_idx": [0, 0, 1],
                "purchase_date": pd.to_datetime(
                    ["2020-01-01", "2020-01-05", "2020-01-10"],
                ),
                "purchase_year": [2020, 2020, 2020],
            },
        )
        active_choice = build_active_choice_set(
            transactions,
            3,
            window_days=10,
        )
        log_active_sales = np.log1p(active_choice.active_sales)

        choice_frame, model_feature_cols = build_availability_choice_dataframe(
            neighborhood_features,
            active_choice.transactions,
            ["control"],
            ["built_area_2020"],
            active_choice.availability,
            {"log_active_sales_12m": log_active_sales},
        )
        validation = validate_availability_choice_dataframe(
            choice_frame,
            model_feature_cols,
            3,
        )

        assert model_feature_cols == [
            "control",
            "log_active_sales_12m",
            "log_built_area_ha",
        ]
        assert choice_frame.loc[0, "log_active_sales_12m_0"] == np.log1p(1.0)
        assert choice_frame.loc[2, "log_active_sales_12m_1"] == 0.0
        assert choice_frame.loc[2, "available_1"] == 1
        assert validation["passed"].all()

    def test_job_group_features_average_interpretable_groups(self) -> None:
        neighborhood_features = pd.DataFrame(
            {
                "jobs_all_20_2025_scaled": [1.0, 2.0],
                "jobs_manufacture_20_2025_scaled": [2.0, 4.0],
                "jobs_logistics_20_2025_scaled": [4.0, 8.0],
                "jobs_construction_20_2025_scaled": [6.0, 12.0],
                "jobs_business_services_20_2025_scaled": [10.0, 20.0],
                "jobs_care_education_health_20_2025_scaled": [20.0, 40.0],
                "jobs_local_services_20_2025_scaled": [30.0, 60.0],
                "jobs_commerce_20_2025_scaled": [3.0, 6.0],
            },
        )
        specs = build_job_group_specs((20,))

        with_groups, catalog = add_job_group_features(neighborhood_features, specs)

        assert catalog["model_column"].tolist() == [
            "jobs_group_all_20_2025_scaled",
            "jobs_group_industrial_20_2025_scaled",
            "jobs_group_services_20_2025_scaled",
            "jobs_group_commerce_20_2025_scaled",
        ]
        assert with_groups["jobs_group_all_20_2025_scaled"].tolist() == [1.0, 2.0]
        assert with_groups["jobs_group_industrial_20_2025_scaled"].tolist() == [
            4.0,
            8.0,
        ]
        assert with_groups["jobs_group_services_20_2025_scaled"].tolist() == [
            20.0,
            40.0,
        ]
        assert with_groups["jobs_group_commerce_20_2025_scaled"].tolist() == [
            3.0,
            6.0,
        ]

    def test_job_group_features_reject_missing_inputs_and_duplicate_outputs(
        self,
    ) -> None:
        specs = build_job_group_specs((20,))
        missing_message = value_error_message(
            lambda: add_job_group_features(
                pd.DataFrame({"jobs_all_20_2025_scaled": [1.0]}),
                specs,
            ),
        )
        assert "missing job group inputs" in missing_message

        duplicate_frame = pd.DataFrame(
            {
                "jobs_all_20_2025_scaled": [1.0],
                "jobs_manufacture_20_2025_scaled": [1.0],
                "jobs_logistics_20_2025_scaled": [1.0],
                "jobs_construction_20_2025_scaled": [1.0],
                "jobs_business_services_20_2025_scaled": [1.0],
                "jobs_care_education_health_20_2025_scaled": [1.0],
                "jobs_local_services_20_2025_scaled": [1.0],
                "jobs_commerce_20_2025_scaled": [1.0],
                "jobs_group_all_20_2025_scaled": [1.0],
            },
        )
        duplicate_message = value_error_message(
            lambda: add_job_group_features(duplicate_frame, specs),
        )
        assert "job group outputs already exist" in duplicate_message

    def test_availability_fast_screen_is_finite_with_dynamic_supply(self) -> None:
        neighborhood_features = pd.DataFrame(
            {
                "name_detail": ["A", "B", "C"],
                "x": [0.0, 1.0, 2.0],
                "built_area_2020": [10_000.0, 20_000.0, 30_000.0],
                "built_area_2021": [20_000.0, 30_000.0, 40_000.0],
            },
        )
        transactions = pd.DataFrame(
            {
                "neighborhood_idx": [0, 1, 2, 1, 2, 0],
                "purchase_year": [2020, 2020, 2020, 2021, 2021, 2021],
            },
        )
        availability = np.array(
            [
                [True, True, False],
                [True, True, False],
                [False, True, True],
                [True, True, True],
                [False, True, True],
                [True, False, True],
            ],
        )
        log_active_sales = availability.astype(float)

        screen_row, coefficients = fit_fast_availability_mnl_screen(
            "availability_demo",
            ["x"],
            neighborhood_features,
            transactions,
            ["built_area_2020", "built_area_2021"],
            availability,
            dynamic_alt_features={"log_active_sales_12m": log_active_sales},
        )

        aic = screen_row["aic"]
        assert isinstance(aic, float)
        assert np.isfinite(aic)
        assert screen_row["null_log_likelihood"] == -(
            2 * np.log(2) + 2 * np.log(2) + 2 * np.log(3)
        )
        mcfadden_r_squared = screen_row["mcfadden_r_squared"]
        assert isinstance(mcfadden_r_squared, float)
        assert np.isfinite(mcfadden_r_squared)
        assert coefficients["feature"].tolist() == [
            "x",
            "log_active_sales_12m",
            "log_built_area_ha",
        ]
