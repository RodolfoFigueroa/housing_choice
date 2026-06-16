from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from housing_choice.modeling._utils import safe_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

ModelSpec = dict[str, object]


def build_single_candidate_model_specs(
    base_control_cols: Sequence[str],
    job_candidate_cols: Sequence[str],
    mfg_candidate_cols: Sequence[str],
    logistics_candidate_cols: Sequence[str],
) -> tuple[dict[str, ModelSpec], pd.DataFrame]:
    base_controls = list(base_control_cols)
    model_specs: dict[str, ModelSpec] = {
        "baseline_no_jobs": {
            "family": "baseline",
            "static_cols": base_controls,
            "candidate_feature": None,
        },
    }
    for feature in job_candidate_cols:
        model_specs[f"job__{safe_identifier(feature)}"] = {
            "family": "job_accessibility",
            "static_cols": [feature, *base_controls],
            "candidate_feature": feature,
        }
    for feature in mfg_candidate_cols:
        model_specs[f"mfg__{safe_identifier(feature)}"] = {
            "family": "manufacturing_cluster",
            "static_cols": [feature, *base_controls],
            "candidate_feature": feature,
        }
    for feature in logistics_candidate_cols:
        model_specs[f"logistics__{safe_identifier(feature)}"] = {
            "family": "logistics_cluster",
            "static_cols": [feature, *base_controls],
            "candidate_feature": feature,
        }

    summary_rows = []
    for spec_id, spec in model_specs.items():
        static_cols = spec["static_cols"]
        if not isinstance(static_cols, list):
            msg = "model_specs static_cols must be a list"
            raise TypeError(msg)
        static_col_names = [str(column) for column in static_cols]
        summary_rows.append(
            {
                "spec_id": spec_id,
                "family": spec["family"],
                "candidate_feature": spec["candidate_feature"],
                "static_features": len(static_cols),
                "all_features": ", ".join(
                    [*static_col_names, "log_built_area_ha"],
                ),
            },
        )
    return model_specs, pd.DataFrame(summary_rows)
