from __future__ import annotations

from unittest import TestCase

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from housing_choice.modeling import (
    align_choice_data,
    build_choice_dataframe,
    build_feature_catalog,
    build_feature_diagnostics_frame,
    build_single_candidate_model_specs,
    compute_feature_diagnostics,
    compute_scale_audit,
    fit_fast_mnl_screen,
    prepare_neighborhood_features,
    prepare_transactions,
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

    def test_single_candidate_specs_match_current_notebook_contract(self) -> None:
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
        assert model_specs["job__job_a"]["static_cols"] == [
            "job_a",
            "control_a",
            "control_b",
        ]
        assert (
            summary.loc[
                summary["spec_id"].eq("job__job_a"),
                "all_features",
            ]
            .iloc[0]
            .endswith("log_built_area_ha")
        )

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
