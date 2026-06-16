from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

DEFAULT_MISSING_VALUE_SENTINEL = 99999


def prepare_transactions(
    transactions_raw: pd.DataFrame,
    neighborhood_names: Iterable[str],
    min_year: int,
    max_year: int,
    threshold: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    neighborhood_set = set(neighborhood_names)
    transactions = (
        transactions_raw.loc[:, ["address", "purchase_date"]]
        .rename(columns={"address": "neighborhood"})
        .assign(purchase_year=lambda df: pd.to_datetime(df["purchase_date"]).dt.year)
        .loc[lambda df: df["purchase_year"].between(min_year, max_year)]
        .loc[lambda df: df["neighborhood"].isin(neighborhood_set)]
        .reset_index(drop=True)
    )
    counts = (
        transactions["neighborhood"]
        .value_counts()
        .rename_axis("neighborhood")
        .reset_index(name="transactions")
    )
    wanted_names = counts.loc[
        counts["transactions"] >= threshold,
        "neighborhood",
    ].tolist()
    filtered = transactions.loc[
        transactions["neighborhood"].isin(wanted_names)
    ].reset_index(drop=True)
    return filtered, counts, wanted_names


def align_choice_data(
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    wanted_names: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    choice_features = (
        neighborhood_features.loc[lambda df: df["name_detail"].isin(wanted_names)]
        .sort_values("name_detail")
        .reset_index(drop=True)
        .assign(neighborhood_idx=lambda df: np.arange(len(df)))
        .set_index("neighborhood_idx")
    )
    name_to_idx = {
        str(name): int(idx)
        for idx, name in choice_features["name_detail"].to_dict().items()
    }
    choice_transactions = transactions.assign(
        neighborhood_idx=lambda df: df["neighborhood"].map(name_to_idx).astype(int),
    )
    return choice_features, choice_transactions, name_to_idx


def build_choice_dataframe(
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    static_cols: Sequence[str],
    built_area_cols: Sequence[str],
) -> tuple[pd.DataFrame, list[str]]:
    model_feature_cols = [*static_cols, "log_built_area_ha"]
    static_series = (
        neighborhood_features.loc[:, static_cols]
        .reset_index(names="index")
        .melt(id_vars="index")
        .assign(index=lambda df: df["index"].astype(str))
        .assign(variable=lambda df: df["variable"] + "_" + df["index"])
        .drop(columns=["index"])
        .set_index("variable")["value"]
    )

    dynamic_features: dict[str, pd.Series] = {}
    for idx, row in neighborhood_features.loc[:, built_area_cols].iterrows():
        area_by_year = {
            int(col.rsplit("_", maxsplit=1)[1]): float(row[col])
            for col in built_area_cols
        }
        mapped_area = transactions["purchase_year"].map(area_by_year).astype(float)
        dynamic_features[f"log_built_area_ha_{idx}"] = pd.Series(
            np.log1p(mapped_area.div(10_000)),
            index=transactions.index,
        )

    choice_frame = pd.concat(
        [
            transactions.loc[:, ["neighborhood_idx", "purchase_year"]].reset_index(
                drop=True,
            ),
            pd.DataFrame(
                dict(static_series.items()),
                index=transactions.index,
            ).reset_index(drop=True),
            pd.DataFrame(dynamic_features).reset_index(drop=True),
        ],
        axis=1,
    )
    model_columns = [
        "neighborhood_idx",
        "purchase_year",
        *[
            f"{feature}_{idx}"
            for idx in neighborhood_features.index
            for feature in model_feature_cols
        ],
    ]
    return choice_frame.loc[:, model_columns].copy(), model_feature_cols


def validate_choice_dataframe(
    choice_frame: pd.DataFrame,
    model_feature_cols: Sequence[str],
    n_alternatives: int,
    *,
    missing_value_sentinel: int = DEFAULT_MISSING_VALUE_SENTINEL,
) -> pd.DataFrame:
    numeric = choice_frame.drop(columns=["purchase_year"])
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
            "passed": choice_frame["neighborhood_idx"]
            .between(0, n_alternatives - 1)
            .all(),
            "value": f"0 to {n_alternatives - 1}",
        },
        {
            "check": "has_free_betas",
            "passed": len(model_feature_cols) > 0,
            "value": len(model_feature_cols),
        },
    ]
    return pd.DataFrame(checks)
