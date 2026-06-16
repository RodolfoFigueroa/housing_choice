from __future__ import annotations

from itertools import combinations, product
from typing import TYPE_CHECKING, TypedDict

import pandas as pd

from housing_choice.modeling._utils import safe_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class ModelSpec(TypedDict):
    spec_kind: str
    static_cols: list[str]
    candidate_features: list[str]
    candidate_families: list[str]


def _validate_unique(label: str, values: Sequence[str]) -> None:
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        msg = f"{label} must be unique: {duplicates}"
        raise ValueError(msg)


def _validate_spec_static_cols(spec_id: str, static_cols: list[str]) -> None:
    _validate_unique(f"{spec_id} static_cols", static_cols)


def _summarize_model_specs(
    model_specs: dict[str, ModelSpec],
) -> pd.DataFrame:
    summary_rows = []
    for spec_id, spec in model_specs.items():
        static_cols = spec["static_cols"]
        if not isinstance(static_cols, list):
            msg = "model_specs static_cols must be a list"
            raise TypeError(msg)
        static_col_names = [str(column) for column in static_cols]
        candidate_features = list(spec["candidate_features"])
        candidate_families = list(spec["candidate_families"])
        summary_rows.append(
            {
                "spec_id": spec_id,
                "spec_kind": spec["spec_kind"],
                "candidate_features": candidate_features,
                "candidate_families": candidate_families,
                "candidate_count": len(candidate_features),
                "static_features": len(static_cols),
                "all_features": ", ".join(
                    [*static_col_names, "log_built_area_ha"],
                ),
            },
        )
    return pd.DataFrame(summary_rows)


def build_single_candidate_model_specs(
    base_control_cols: Sequence[str],
    job_candidate_cols: Sequence[str],
    mfg_candidate_cols: Sequence[str],
    logistics_candidate_cols: Sequence[str],
) -> tuple[dict[str, ModelSpec], pd.DataFrame]:
    base_controls = list(base_control_cols)
    _validate_unique("base_control_cols", base_controls)
    model_specs: dict[str, ModelSpec] = {
        "baseline_no_jobs": {
            "spec_kind": "baseline",
            "static_cols": base_controls,
            "candidate_features": [],
            "candidate_families": [],
        },
    }
    for feature in job_candidate_cols:
        static_cols = [feature, *base_controls]
        _validate_spec_static_cols(f"job__{safe_identifier(feature)}", static_cols)
        model_specs[f"job__{safe_identifier(feature)}"] = {
            "spec_kind": "single_candidate",
            "static_cols": static_cols,
            "candidate_features": [feature],
            "candidate_families": ["job_accessibility"],
        }
    for feature in mfg_candidate_cols:
        static_cols = [feature, *base_controls]
        _validate_spec_static_cols(f"mfg__{safe_identifier(feature)}", static_cols)
        model_specs[f"mfg__{safe_identifier(feature)}"] = {
            "spec_kind": "single_candidate",
            "static_cols": static_cols,
            "candidate_features": [feature],
            "candidate_families": ["manufacturing_cluster"],
        }
    for feature in logistics_candidate_cols:
        static_cols = [feature, *base_controls]
        _validate_spec_static_cols(
            f"logistics__{safe_identifier(feature)}",
            static_cols,
        )
        model_specs[f"logistics__{safe_identifier(feature)}"] = {
            "spec_kind": "single_candidate",
            "static_cols": static_cols,
            "candidate_features": [feature],
            "candidate_families": ["logistics_cluster"],
        }
    return model_specs, _summarize_model_specs(model_specs)


def build_combination_model_specs(
    base_control_cols: Sequence[str],
    candidate_groups: Mapping[str, Sequence[str]],
    *,
    min_candidates: int = 2,
    max_candidates: int = 3,
) -> tuple[dict[str, ModelSpec], pd.DataFrame]:
    if min_candidates < 2:
        msg = "min_candidates must be at least 2"
        raise ValueError(msg)
    if max_candidates < min_candidates:
        msg = "max_candidates must be greater than or equal to min_candidates"
        raise ValueError(msg)

    base_controls = list(base_control_cols)
    _validate_unique("base_control_cols", base_controls)

    groups = {
        family: list(features)
        for family, features in candidate_groups.items()
        if len(features) > 0
    }
    all_candidates = [feature for features in groups.values() for feature in features]
    _validate_unique("candidate features", all_candidates)

    control_overlap = sorted(set(base_controls).intersection(all_candidates))
    if control_overlap:
        msg = f"candidate features overlap with base controls: {control_overlap}"
        raise ValueError(msg)

    model_specs: dict[str, ModelSpec] = {}
    families = list(groups)
    upper = min(max_candidates, len(families))
    for candidate_count in range(min_candidates, upper + 1):
        for family_tuple in combinations(families, candidate_count):
            for feature_tuple in product(
                *(groups[family] for family in family_tuple),
            ):
                candidate_features = list(feature_tuple)
                static_cols = [*candidate_features, *base_controls]
                spec_id = "combo__" + "__".join(
                    safe_identifier(feature) for feature in candidate_features
                )
                _validate_spec_static_cols(spec_id, static_cols)
                if spec_id in model_specs:
                    msg = f"Duplicate combination spec_id: {spec_id}"
                    raise ValueError(msg)
                model_specs[spec_id] = {
                    "spec_kind": "combination",
                    "static_cols": static_cols,
                    "candidate_features": candidate_features,
                    "candidate_families": list(family_tuple),
                }
    return model_specs, _summarize_model_specs(model_specs)
