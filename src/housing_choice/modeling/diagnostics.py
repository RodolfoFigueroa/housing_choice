from __future__ import annotations

import math
from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Sequence


def compute_feature_diagnostics(
    feature_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, float]:
    diagnostics = feature_frame.astype(float).copy()
    correlation = diagnostics.corr().round(3)
    corr_abs = correlation.abs().where(~np.eye(len(correlation), dtype=bool))
    max_abs_correlation = (
        round(float(corr_abs.max().max()), 3) if len(correlation) else 0
    )

    vif_rows = []
    for feature in diagnostics.columns:
        if len(diagnostics.columns) == 1:
            vif_rows.append({"feature": feature, "vif": 1.0, "r2": 0.0})
            continue
        y = diagnostics[feature].to_numpy()
        x = diagnostics.drop(columns=[feature]).to_numpy()
        x = np.column_stack([np.ones(len(x)), x])
        beta = np.linalg.lstsq(x, y, rcond=None)[0]
        pred = x @ beta
        ss_res = ((y - pred) ** 2).sum()
        ss_tot = ((y - y.mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot else 0.0
        vif_rows.append(
            {
                "feature": feature,
                "vif": 1 / (1 - r2) if r2 < 1 else math.inf,
                "r2": r2,
            },
        )
    vif = (
        pd.DataFrame(vif_rows)
        .sort_values("vif", ascending=False)
        .reset_index(drop=True)
        .round({"vif": 3, "r2": 3})
    )
    return diagnostics, correlation, vif, max_abs_correlation


def build_feature_diagnostics_frame(
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    static_cols: Sequence[str],
    built_area_cols: Sequence[str],
) -> pd.DataFrame:
    diagnostics = neighborhood_features.loc[:, static_cols].astype(float).copy()
    diagnostics["log_built_area_ha"] = 0.0
    year_weights = transactions["purchase_year"].value_counts(normalize=True)
    for year, weight in year_weights.items():
        year_int = int(cast("int", year))
        built_col = f"built_area_{year_int}"
        if built_col not in built_area_cols:
            msg = f"Missing built area column for year {year}: {built_col}"
            raise ValueError(msg)
        diagnostics["log_built_area_ha"] += (
            np.log1p(neighborhood_features[built_col].astype(float).div(10_000))
            * weight
        )
    return diagnostics
