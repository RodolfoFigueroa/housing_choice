from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Sequence


CENTROID_EAST_COLUMN = "centroid_east_km"
CENTROID_NORTH_COLUMN = "centroid_north_km"
CENTROID_QUADRATIC_COLUMNS = (
    "centroid_east_km_sq",
    "centroid_north_km_sq",
    "centroid_east_x_north_km2",
)
GRID_ZONE_COLUMN = "spatial_grid_3x3_zone"
GRID_PREFIX = "spatial_grid_3x3"
GRID_EAST_LABELS = ("west", "central", "east")
GRID_NORTH_LABELS = ("south", "central", "north")
GRID_REFERENCE_ZONE = "central_central"


def add_centroid_quadratic_features(
    neighborhood_features: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        neighborhood_features,
        (CENTROID_EAST_COLUMN, CENTROID_NORTH_COLUMN),
    )
    _reject_existing_columns(neighborhood_features, CENTROID_QUADRATIC_COLUMNS)

    with_features = neighborhood_features.copy()
    east = pd.to_numeric(with_features[CENTROID_EAST_COLUMN], errors="coerce")
    north = pd.to_numeric(with_features[CENTROID_NORTH_COLUMN], errors="coerce")
    _require_finite(CENTROID_EAST_COLUMN, east)
    _require_finite(CENTROID_NORTH_COLUMN, north)

    with_features["centroid_east_km_sq"] = east**2
    with_features["centroid_north_km_sq"] = north**2
    with_features["centroid_east_x_north_km2"] = east * north
    return with_features


def add_centroid_grid_features(
    neighborhood_features: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _require_columns(
        neighborhood_features,
        (CENTROID_EAST_COLUMN, CENTROID_NORTH_COLUMN),
    )
    if len(neighborhood_features) < 9:
        msg = "centroid grid features require at least 9 neighborhoods"
        raise ValueError(msg)

    zone_ids = [
        f"{east_label}_{north_label}"
        for east_label in GRID_EAST_LABELS
        for north_label in GRID_NORTH_LABELS
    ]
    dummy_columns = [
        _zone_dummy_column(zone_id)
        for zone_id in zone_ids
        if zone_id != GRID_REFERENCE_ZONE
    ]
    _reject_existing_columns(
        neighborhood_features,
        (GRID_ZONE_COLUMN, *dummy_columns),
    )

    with_features = neighborhood_features.copy()
    east = pd.to_numeric(with_features[CENTROID_EAST_COLUMN], errors="coerce")
    north = pd.to_numeric(with_features[CENTROID_NORTH_COLUMN], errors="coerce")
    _require_finite(CENTROID_EAST_COLUMN, east)
    _require_finite(CENTROID_NORTH_COLUMN, north)

    east_bin = _ranked_tercile_labels(east, GRID_EAST_LABELS)
    north_bin = _ranked_tercile_labels(north, GRID_NORTH_LABELS)
    zone = east_bin.astype(str).str.cat(north_bin.astype(str), sep="_")
    with_features[GRID_ZONE_COLUMN] = zone

    rows: list[dict[str, object]] = []
    zone_counts = zone.value_counts().to_dict()
    for zone_id in zone_ids:
        is_reference = zone_id == GRID_REFERENCE_ZONE
        model_column = None if is_reference else _zone_dummy_column(zone_id)
        if model_column is not None:
            with_features[model_column] = zone.eq(zone_id).astype(int)
        rows.append(
            {
                "zone_id": zone_id,
                "model_column": model_column,
                "is_reference": is_reference,
                "zone_count": int(zone_counts.get(zone_id, 0)),
                "description": _zone_description(zone_id),
            },
        )

    return with_features, pd.DataFrame(rows)


def _ranked_tercile_labels(
    values: pd.Series,
    labels: Sequence[str],
) -> pd.Series:
    ranked = values.rank(method="first")
    return pd.qcut(ranked, q=len(labels), labels=labels).astype(str)


def _zone_dummy_column(zone_id: str) -> str:
    return f"{GRID_PREFIX}_{zone_id}"


def _zone_description(zone_id: str) -> str:
    east_label, north_label = zone_id.split("_", maxsplit=1)
    return f"{east_label} / {north_label}"


def _require_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
) -> None:
    missing_columns = sorted(set(columns).difference(frame.columns))
    if missing_columns:
        msg = f"neighborhood_features missing spatial columns: {missing_columns}"
        raise ValueError(msg)


def _reject_existing_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
) -> None:
    existing_columns = sorted(set(columns).intersection(frame.columns))
    if existing_columns:
        msg = f"spatial feature outputs already exist: {existing_columns}"
        raise ValueError(msg)


def _require_finite(
    column: str,
    values: pd.Series,
) -> None:
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        msg = f"{column} must contain only finite numeric values"
        raise ValueError(msg)
