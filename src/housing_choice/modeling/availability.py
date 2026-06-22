from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class ActiveChoiceSet:
    transactions: pd.DataFrame
    availability: npt.NDArray[np.bool_]
    active_sales: npt.NDArray[np.float64]
    dropped_transactions: pd.DataFrame
    summary: pd.DataFrame


def prepare_baseline_transactions(
    transactions_raw: pd.DataFrame,
    neighborhood_names: Sequence[str],
    min_year: int,
    max_year: int,
) -> pd.DataFrame:
    neighborhood_set = set(neighborhood_names)
    transactions = (
        transactions_raw.loc[:, ["address", "purchase_date"]]
        .rename(columns={"address": "neighborhood"})
        .assign(purchase_date=lambda df: pd.to_datetime(df["purchase_date"]))
        .assign(purchase_year=lambda df: df["purchase_date"].dt.year)
        .loc[lambda df: df["purchase_year"].between(min_year, max_year)]
        .loc[lambda df: df["neighborhood"].isin(neighborhood_set)]
        .reset_index(drop=True)
        .assign(transaction_id=lambda df: np.arange(len(df), dtype=int))
    )
    return transactions.loc[
        :,
        ["transaction_id", "neighborhood", "purchase_date", "purchase_year"],
    ].copy()


def add_centroid_spatial_controls(
    neighborhood_features: pd.DataFrame,
) -> pd.DataFrame:
    if "geometry" not in neighborhood_features.columns:
        msg = "neighborhood_features must include a geometry column"
        raise ValueError(msg)

    prepared = neighborhood_features.copy()
    centroids = gpd.GeoSeries(prepared["geometry"], crs=getattr(prepared, "crs", None))
    centroids = centroids.centroid
    median_x = float(centroids.x.median())
    median_y = float(centroids.y.median())
    prepared["centroid_east_km"] = (centroids.x.to_numpy() - median_x) / 1000
    prepared["centroid_north_km"] = (centroids.y.to_numpy() - median_y) / 1000
    return prepared


def build_active_choice_set(
    transactions: pd.DataFrame,
    n_alternatives: int,
    *,
    window_days: int = 365,
    min_available_alternatives: int = 2,
) -> ActiveChoiceSet:
    if window_days < 0:
        msg = "window_days must be non-negative"
        raise ValueError(msg)
    if min_available_alternatives < 2:
        msg = "min_available_alternatives must be at least 2"
        raise ValueError(msg)

    required_columns = {"neighborhood_idx", "purchase_date"}
    missing_columns = required_columns.difference(transactions.columns)
    if missing_columns:
        msg = f"transactions missing required columns: {sorted(missing_columns)}"
        raise ValueError(msg)

    choice_ids = transactions["neighborhood_idx"].astype(int).to_numpy()
    if ((choice_ids < 0) | (choice_ids >= n_alternatives)).any():
        msg = "transactions include neighborhood_idx values outside alternatives"
        raise ValueError(msg)

    purchase_days = (
        pd.to_datetime(transactions["purchase_date"])
        .to_numpy(dtype="datetime64[D]")
        .astype("int64")
    )
    n_transactions = len(transactions)
    active_sales = np.zeros((n_transactions, n_alternatives), dtype=float)

    for alternative_idx in range(n_alternatives):
        alternative_days = np.sort(purchase_days[choice_ids == alternative_idx])
        lower = np.searchsorted(
            alternative_days,
            purchase_days - window_days,
            side="left",
        )
        upper = np.searchsorted(
            alternative_days,
            purchase_days + window_days,
            side="right",
        )
        active_sales[:, alternative_idx] = upper - lower

    active_sales[np.arange(n_transactions), choice_ids] -= 1
    active_sales = np.clip(active_sales, 0, None)

    availability = active_sales > 0
    availability[np.arange(n_transactions), choice_ids] = True
    available_alternatives = availability.sum(axis=1)
    keep_mask = available_alternatives >= min_available_alternatives

    annotated_transactions = transactions.copy().assign(
        available_alternatives=available_alternatives,
    )
    kept_transactions = annotated_transactions.loc[keep_mask].reset_index(drop=True)
    dropped_transactions = annotated_transactions.loc[~keep_mask].reset_index(drop=True)
    kept_availability = availability[keep_mask]
    kept_active_sales = active_sales[keep_mask]
    kept_counts = available_alternatives[keep_mask]

    summary = pd.DataFrame(
        [
            {
                "window_days": window_days,
                "input_transactions": n_transactions,
                "kept_transactions": len(kept_transactions),
                "dropped_transactions": len(dropped_transactions),
                "drop_share": (
                    len(dropped_transactions) / n_transactions
                    if n_transactions
                    else np.nan
                ),
                "min_available_alternatives": (
                    int(kept_counts.min()) if len(kept_counts) else 0
                ),
                "median_available_alternatives": (
                    float(np.median(kept_counts)) if len(kept_counts) else np.nan
                ),
                "max_available_alternatives": (
                    int(kept_counts.max()) if len(kept_counts) else 0
                ),
            },
        ],
    )
    return ActiveChoiceSet(
        transactions=kept_transactions,
        availability=kept_availability,
        active_sales=kept_active_sales,
        dropped_transactions=dropped_transactions,
        summary=summary,
    )


def build_availability_choice_dataframe(  # noqa: PLR0913
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    static_cols: Sequence[str],
    built_area_cols: Sequence[str],
    availability: npt.NDArray[np.bool_],
    dynamic_alt_features: Mapping[str, npt.NDArray[np.float64]] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    n_observations = len(transactions)
    n_alternatives = len(neighborhood_features)
    dynamic_alt_features = dynamic_alt_features or {}
    _validate_alt_matrix("availability", availability, n_observations, n_alternatives)
    for name, matrix in dynamic_alt_features.items():
        _validate_alt_matrix(name, matrix, n_observations, n_alternatives)

    duplicated_features = set(static_cols).intersection(dynamic_alt_features)
    if duplicated_features:
        msg = f"static and dynamic features overlap: {sorted(duplicated_features)}"
        raise ValueError(msg)
    if "log_built_area_ha" in {*static_cols, *dynamic_alt_features}:
        msg = "log_built_area_ha is reserved for transaction-year built area"
        raise ValueError(msg)

    model_feature_cols = [
        *static_cols,
        *dynamic_alt_features.keys(),
        "log_built_area_ha",
    ]
    feature_data: dict[str, npt.NDArray[np.float64]] = {}

    for alternative_idx, (_, row) in enumerate(neighborhood_features.iterrows()):
        for feature in static_cols:
            value = float(row[feature])
            feature_data[f"{feature}_{alternative_idx}"] = np.full(
                n_observations,
                value,
                dtype=float,
            )

        for feature, matrix in dynamic_alt_features.items():
            feature_data[f"{feature}_{alternative_idx}"] = matrix[
                :,
                alternative_idx,
            ].astype(float)

        area_by_year = {
            int(column.rsplit("_", maxsplit=1)[1]): float(row[column])
            for column in built_area_cols
        }
        mapped_area = transactions["purchase_year"].map(area_by_year).astype(float)
        feature_data[f"log_built_area_ha_{alternative_idx}"] = np.log1p(
            mapped_area.to_numpy() / 10_000,
        )

    availability_data = {
        f"available_{alternative_idx}": availability[:, alternative_idx].astype(int)
        for alternative_idx in range(n_alternatives)
    }
    choice_frame = pd.concat(
        [
            transactions.loc[:, ["neighborhood_idx", "purchase_year"]].reset_index(
                drop=True,
            ),
            pd.DataFrame(feature_data),
            pd.DataFrame(availability_data),
        ],
        axis=1,
    )
    ordered_columns = [
        "neighborhood_idx",
        "purchase_year",
        *[
            f"{feature}_{alternative_idx}"
            for alternative_idx in range(n_alternatives)
            for feature in model_feature_cols
        ],
        *[f"available_{alternative_idx}" for alternative_idx in range(n_alternatives)],
    ]
    return choice_frame.loc[:, ordered_columns].copy(), model_feature_cols


def validate_availability_choice_dataframe(
    choice_frame: pd.DataFrame,
    model_feature_cols: Sequence[str],
    n_alternatives: int,
    *,
    missing_value_sentinel: int = 99999,
) -> pd.DataFrame:
    numeric = choice_frame.drop(columns=["purchase_year"])
    availability_cols = [f"available_{idx}" for idx in range(n_alternatives)]
    missing_availability_cols = [
        column for column in availability_cols if column not in choice_frame.columns
    ]
    availability_values = (
        choice_frame.loc[:, availability_cols]
        if not missing_availability_cols
        else pd.DataFrame(index=choice_frame.index)
    )
    availability_counts = availability_values.sum(axis=1)
    choice_ids = choice_frame["neighborhood_idx"].astype(int)
    chosen_available = (
        bool(
            availability_values.to_numpy()[
                np.arange(len(choice_frame)),
                choice_ids.to_numpy(),
            ].all(),
        )
        if not missing_availability_cols and len(choice_frame)
        else False
    )
    checks = [
        {
            "check": "no_missing_values",
            "passed": not numeric.isna().any().any(),
            "value": int(numeric.isna().sum().sum()),
        },
        {
            "check": "finite_values",
            "passed": bool(np.isfinite(numeric.to_numpy(dtype=float)).all()),
            "value": "all finite",
        },
        {
            "check": "no_missing_value_sentinel",
            "passed": not numeric.eq(missing_value_sentinel).any().any(),
            "value": missing_value_sentinel,
        },
        {
            "check": "choice_ids_valid",
            "passed": choice_ids.between(0, n_alternatives - 1).all(),
            "value": f"0 to {n_alternatives - 1}",
        },
        {
            "check": "has_free_betas",
            "passed": len(model_feature_cols) > 0,
            "value": len(model_feature_cols),
        },
        {
            "check": "availability_columns_present",
            "passed": not missing_availability_cols,
            "value": ", ".join(missing_availability_cols),
        },
        {
            "check": "availability_is_binary",
            "passed": bool(
                availability_values.isin([0, 1]).all().all()
                if not missing_availability_cols
                else False,
            ),
            "value": "0/1",
        },
        {
            "check": "chosen_alternative_available",
            "passed": chosen_available,
            "value": "chosen available",
        },
        {
            "check": "at_least_two_available_alternatives",
            "passed": bool(availability_counts.ge(2).all())
            if not missing_availability_cols
            else False,
            "value": int(availability_counts.min())
            if not missing_availability_cols and len(availability_counts)
            else 0,
        },
    ]
    return pd.DataFrame(checks)


def summarize_availability_by_transaction(
    transactions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        transactions["available_alternatives"]
        .describe(percentiles=[0.25, 0.5, 0.75, 0.9])
        .rename("available_alternatives")
        .reset_index()
        .rename(columns={"index": "statistic"})
    )


def _validate_alt_matrix(
    label: str,
    values: npt.ArrayLike,
    n_observations: int,
    n_alternatives: int,
) -> None:
    matrix = np.asarray(values)
    expected_shape = (n_observations, n_alternatives)
    if matrix.shape != expected_shape:
        msg = f"{label} must have shape {expected_shape}, got {matrix.shape}"
        raise ValueError(msg)
