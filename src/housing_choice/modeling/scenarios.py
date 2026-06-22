from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import numpy as np
import pandas as pd

from housing_choice.modeling.availability import (
    ActiveChoiceSet,
    add_centroid_spatial_controls,
    build_active_choice_set,
    prepare_baseline_transactions,
)
from housing_choice.modeling.choice_data import align_choice_data
from housing_choice.modeling.features import (
    build_feature_catalog,
    prepare_neighborhood_features,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


DEFAULT_BASELINE_STATIC_COLS = (
    "accessibility_services_scaled",
    "travel_time_city_center_scaled",
    "travel_time_nearest_crossing_scaled",
    "access_is_restricted",
    "centroid_east_km",
    "centroid_north_km",
)
DEFAULT_SUPPLY_FEATURE = "log_active_sales_12m"


@dataclass(frozen=True)
class StructuralBaselineInputs:
    data_path: Path
    neighborhood_features_path: Path
    transactions_path: Path
    df_neighborhood_raw: gpd.GeoDataFrame
    df_transactions_raw: pd.DataFrame
    feature_catalog: pd.DataFrame
    prepared_neighborhood_features: pd.DataFrame
    built_area_cols: list[str]
    df_transactions_baseline: pd.DataFrame
    choice_neighborhood_features: pd.DataFrame
    df_transactions_aligned: pd.DataFrame
    name_to_idx_map: dict[str, int]
    active_choice_set: ActiveChoiceSet
    dynamic_alt_features: Mapping[str, np.ndarray]
    choice_set_summary: pd.DataFrame
    baseline_static_cols: tuple[str, ...]
    supply_feature: str


def build_structural_baseline_inputs(  # noqa: PLR0913
    data_path: str | Path,
    *,
    min_year: int = 2020,
    max_year: int = 2025,
    window_days: int = 365,
    min_available_alternatives: int = 2,
    baseline_static_cols: Sequence[str] = DEFAULT_BASELINE_STATIC_COLS,
    supply_feature: str = DEFAULT_SUPPLY_FEATURE,
) -> StructuralBaselineInputs:
    data_path = Path(data_path)
    generated_path = data_path / "generated"
    neighborhood_features_path = generated_path / "col_final.gpkg"
    transactions_path = generated_path / "transactions_final.parquet"

    df_neighborhood_raw = gpd.read_file(neighborhood_features_path)
    df_transactions_raw = pd.read_parquet(transactions_path)
    feature_catalog = build_feature_catalog(df_neighborhood_raw)
    prepared_neighborhood_features = prepare_neighborhood_features(
        df_neighborhood_raw,
        feature_catalog,
    ).pipe(add_centroid_spatial_controls)
    built_area_cols = sorted(
        column
        for column in prepared_neighborhood_features.columns
        if column.startswith("built_area_")
    )

    df_transactions_baseline = prepare_baseline_transactions(
        df_transactions_raw,
        prepared_neighborhood_features["name_detail"].tolist(),
        min_year,
        max_year,
    )
    (
        choice_neighborhood_features,
        df_transactions_aligned,
        name_to_idx_map,
    ) = align_choice_data(
        prepared_neighborhood_features,
        df_transactions_baseline,
        prepared_neighborhood_features["name_detail"].tolist(),
    )
    active_choice_set = build_active_choice_set(
        df_transactions_aligned,
        len(choice_neighborhood_features),
        window_days=window_days,
        min_available_alternatives=min_available_alternatives,
    )
    dynamic_alt_features = {
        supply_feature: np.log1p(active_choice_set.active_sales),
    }
    choice_set_summary = active_choice_set.summary.assign(
        candidate_neighborhoods=len(choice_neighborhood_features),
        matched_transactions=len(df_transactions_baseline),
    )[
        [
            "candidate_neighborhoods",
            "matched_transactions",
            "kept_transactions",
            "dropped_transactions",
            "drop_share",
            "min_available_alternatives",
            "median_available_alternatives",
            "max_available_alternatives",
        ]
    ]

    return StructuralBaselineInputs(
        data_path=data_path,
        neighborhood_features_path=neighborhood_features_path,
        transactions_path=transactions_path,
        df_neighborhood_raw=df_neighborhood_raw,
        df_transactions_raw=df_transactions_raw,
        feature_catalog=feature_catalog,
        prepared_neighborhood_features=prepared_neighborhood_features,
        built_area_cols=built_area_cols,
        df_transactions_baseline=df_transactions_baseline,
        choice_neighborhood_features=choice_neighborhood_features,
        df_transactions_aligned=df_transactions_aligned,
        name_to_idx_map=name_to_idx_map,
        active_choice_set=active_choice_set,
        dynamic_alt_features=dynamic_alt_features,
        choice_set_summary=choice_set_summary,
        baseline_static_cols=tuple(baseline_static_cols),
        supply_feature=supply_feature,
    )
