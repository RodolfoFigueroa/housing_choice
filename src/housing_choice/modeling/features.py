from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_TARGET_SCALE_LOWER = 1.0
DEFAULT_TARGET_SCALE_UPPER = 10.0


def nice_scale_denominator(
    values: pd.Series | Sequence[object] | np.ndarray,
    target_p75: float = 5.0,
) -> float:
    finite = (
        pd.to_numeric(pd.Series(values), errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    finite = finite.loc[finite.ne(0)].abs()
    if finite.empty:
        return 1.0

    raw_denominator = float(finite.quantile(0.75) / target_p75)
    if raw_denominator <= 0 or not np.isfinite(raw_denominator):
        return 1.0

    exponent = math.floor(math.log10(raw_denominator))
    candidates = [
        multiplier * (10**exp)
        for exp in range(exponent - 2, exponent + 3)
        for multiplier in (1, 2, 5, 10)
    ]
    return float(
        min(
            candidates,
            key=lambda candidate: abs(math.log(candidate / raw_denominator)),
        ),
    )


def _cluster_family_for_column(column: str) -> str | None:
    cluster_column_rules = (
        ("mfg", "manufacturing_cluster"),
        ("logistics", "logistics_cluster"),
    )
    for prefix, family in cluster_column_rules:
        if (
            column.startswith((f"{prefix}_", f"nearest_{prefix}_"))
            or f"_{prefix}_cluster" in column
        ):
            return family
    return None


def _selected_cluster_features() -> dict[str, dict[str, str]]:
    return {
        "mfg_distance_nearest_cluster_km": {
            "family": "manufacturing_cluster",
            "role": "mfg_screen",
            "transform_kind": "scaled_distance",
        },
        "log_mfg_jobs_within_2km": {
            "family": "manufacturing_cluster",
            "role": "mfg_screen",
            "transform_kind": "already_log_scaled",
        },
        "log_mfg_cluster_gravity_inv_sq": {
            "family": "manufacturing_cluster",
            "role": "mfg_screen",
            "transform_kind": "already_log_scaled",
        },
        "logistics_distance_nearest_cluster_km": {
            "family": "logistics_cluster",
            "role": "logistics_screen",
            "transform_kind": "scaled_distance",
        },
        "log_logistics_jobs_within_2km": {
            "family": "logistics_cluster",
            "role": "logistics_screen",
            "transform_kind": "already_log_scaled",
        },
        "log_logistics_cluster_gravity_inv_sq": {
            "family": "logistics_cluster",
            "role": "logistics_screen",
            "transform_kind": "already_log_scaled",
        },
    }


def build_feature_catalog(neighborhood_raw: pd.DataFrame) -> pd.DataFrame:  # noqa: C901, PLR0912
    rows: list[dict[str, object]] = []
    selected_cluster_features = _selected_cluster_features()

    for column in neighborhood_raw.columns:
        if column in {"name", "name_detail", "geometry"}:
            rows.append(
                {
                    "source_column": column,
                    "model_column": None,
                    "family": "identifier",
                    "role": "exclude",
                    "transform": "not a model covariate",
                    "scale_denominator": np.nan,
                    "eligible": False,
                    "reason": "identifier or geometry",
                },
            )
        elif column == "access":
            rows.append(
                {
                    "source_column": column,
                    "model_column": "access_is_restricted",
                    "family": "access",
                    "role": "control",
                    "transform": "LIBRE=0, RESTRINGIDO=1",
                    "scale_denominator": 1.0,
                    "eligible": True,
                    "reason": "binary access control",
                },
            )
        elif column.startswith("jobs_") and column.endswith("_2025"):
            source = pd.to_numeric(neighborhood_raw[column], errors="coerce")
            eligible = len(source.dropna().unique()) > 1
            denominator = nice_scale_denominator(source) if eligible else np.nan
            rows.append(
                {
                    "source_column": column,
                    "model_column": f"{column}_scaled",
                    "family": "job_accessibility",
                    "role": "job_screen" if eligible else "exclude",
                    "transform": (
                        f"divide by {denominator:g} jobs"
                        if eligible
                        else "zero variance"
                    ),
                    "scale_denominator": denominator,
                    "eligible": bool(eligible),
                    "reason": (
                        "candidate job accessibility" if eligible else "zero variance"
                    ),
                },
            )
        elif column == "accessibility_services":
            denominator = nice_scale_denominator(neighborhood_raw[column])
            rows.append(
                {
                    "source_column": column,
                    "model_column": "accessibility_services_scaled",
                    "family": "service_accessibility",
                    "role": "control",
                    "transform": f"divide by {denominator:g}",
                    "scale_denominator": denominator,
                    "eligible": True,
                    "reason": "service accessibility control",
                },
            )
        elif column == "travel_time_city_center":
            denominator = nice_scale_denominator(neighborhood_raw[column])
            rows.append(
                {
                    "source_column": column,
                    "model_column": "travel_time_city_center_scaled",
                    "family": "travel_time",
                    "role": "control",
                    "transform": f"divide by {denominator:g} seconds",
                    "scale_denominator": denominator,
                    "eligible": True,
                    "reason": "centrality control",
                },
            )
        elif column in {"travel_time_crossing_west", "travel_time_crossing_east"}:
            rows.append(
                {
                    "source_column": column,
                    "model_column": None,
                    "family": "travel_time",
                    "role": "helper",
                    "transform": "combined into nearest crossing time",
                    "scale_denominator": np.nan,
                    "eligible": False,
                    "reason": "raw crossing helper",
                },
            )
        elif column.startswith("built_area_"):
            rows.append(
                {
                    "source_column": column,
                    "model_column": "log_built_area_ha",
                    "family": "built_area_history",
                    "role": "transaction_varying",
                    "transform": "log1p(area_m2 / 10000) by purchase year",
                    "scale_denominator": np.nan,
                    "eligible": True,
                    "reason": "dynamic supply proxy",
                },
            )
        elif column in selected_cluster_features:
            cluster_feature = selected_cluster_features[column]
            if cluster_feature["transform_kind"] == "scaled_distance":
                denominator = nice_scale_denominator(neighborhood_raw[column])
                model_column = f"{column}_scaled"
                transform = f"divide by {denominator:g} km"
            else:
                denominator = 1.0
                model_column = column
                transform = "already log scaled"
            rows.append(
                {
                    "source_column": column,
                    "model_column": model_column,
                    "family": cluster_feature["family"],
                    "role": cluster_feature["role"],
                    "transform": transform,
                    "scale_denominator": denominator,
                    "eligible": True,
                    "reason": "selected interpretable cluster exposure",
                },
            )
        elif (cluster_family := _cluster_family_for_column(column)) is not None:
            rows.append(
                {
                    "source_column": column,
                    "model_column": None,
                    "family": cluster_family,
                    "role": "available_not_screened",
                    "transform": "not used in v1 model specs",
                    "scale_denominator": np.nan,
                    "eligible": False,
                    "reason": "kept out to avoid over-specified cluster models",
                },
            )
        else:
            rows.append(
                {
                    "source_column": column,
                    "model_column": None,
                    "family": "other",
                    "role": "exclude",
                    "transform": "not classified for modelling",
                    "scale_denominator": np.nan,
                    "eligible": False,
                    "reason": "unclassified",
                },
            )

    nearest_crossing = neighborhood_raw[
        ["travel_time_crossing_west", "travel_time_crossing_east"]
    ].min(axis=1)
    nearest_denominator = nice_scale_denominator(nearest_crossing)
    rows.append(
        {
            "source_column": (
                "min(travel_time_crossing_west, travel_time_crossing_east)"
            ),
            "model_column": "travel_time_nearest_crossing_scaled",
            "family": "travel_time",
            "role": "control",
            "transform": f"divide by {nearest_denominator:g} seconds",
            "scale_denominator": nearest_denominator,
            "eligible": True,
            "reason": "nearest border crossing control",
        },
    )

    return pd.DataFrame(rows)


def prepare_neighborhood_features(
    neighborhood_raw: pd.DataFrame,
    feature_catalog: pd.DataFrame,
) -> pd.DataFrame:
    prepared = pd.DataFrame(index=neighborhood_raw.index)
    prepared["name_detail"] = neighborhood_raw["name_detail"]
    prepared["name"] = neighborhood_raw["name"]
    prepared["geometry"] = neighborhood_raw["geometry"]

    eligible_features = feature_catalog.loc[
        lambda df: df["eligible"] & df["model_column"].notna()
    ]
    for _, spec in eligible_features.iterrows():
        model_column = spec["model_column"]
        source_column = spec["source_column"]
        if model_column in prepared.columns or spec["role"] == "transaction_varying":
            continue
        if model_column == "access_is_restricted":
            prepared[model_column] = neighborhood_raw["access"].map(
                {"LIBRE": 0, "RESTRINGIDO": 1},
            )
            continue
        if source_column == "min(travel_time_crossing_west, travel_time_crossing_east)":
            values = neighborhood_raw[
                ["travel_time_crossing_west", "travel_time_crossing_east"]
            ].min(axis=1)
        else:
            values = pd.to_numeric(neighborhood_raw[source_column], errors="coerce")
        denominator = float(spec["scale_denominator"])
        prepared[model_column] = values.astype(float) / denominator

    for column in neighborhood_raw.columns:
        if column.startswith("built_area_"):
            prepared[column] = pd.to_numeric(neighborhood_raw[column], errors="coerce")

    return prepared


def compute_scale_audit(
    feature_frame: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    target_scale_lower: float = DEFAULT_TARGET_SCALE_LOWER,
    target_scale_upper: float = DEFAULT_TARGET_SCALE_UPPER,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for column in feature_columns:
        values = pd.to_numeric(feature_frame[column], errors="coerce")
        finite = values.replace([np.inf, -np.inf], np.nan).dropna()
        is_binary = (
            bool((finite.eq(0) | finite.eq(1)).all()) if not finite.empty else False
        )
        abs_finite = finite.abs()
        nonzero_abs = abs_finite.loc[abs_finite.gt(0)]
        if finite.empty:
            warning = "all missing"
        elif is_binary:
            warning = "binary"
        elif abs_finite.quantile(0.75) > target_scale_upper:
            warning = "too large"
        elif nonzero_abs.empty or nonzero_abs.median() < target_scale_lower:
            warning = "too small"
        else:
            warning = "ok"
        rows.append(
            {
                "feature": column,
                "missing": int(values.isna().sum()),
                "n_unique": int(values.nunique(dropna=True)),
                "min": finite.min() if not finite.empty else np.nan,
                "p25": finite.quantile(0.25) if not finite.empty else np.nan,
                "median": finite.median() if not finite.empty else np.nan,
                "p75": finite.quantile(0.75) if not finite.empty else np.nan,
                "max": finite.max() if not finite.empty else np.nan,
                "scale_warning": warning,
            },
        )
    return pd.DataFrame(rows).round(4)
