from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class JobGroupSpec:
    group_id: str
    model_column: str
    source_columns: tuple[str, ...]
    description: str
    horizon_minutes: int


JOB_GROUP_SOURCE_FAMILIES: dict[str, tuple[str, ...]] = {
    "all": ("all",),
    "industrial": ("manufacture", "logistics", "construction"),
    "services": ("business_services", "care_education_health", "local_services"),
    "commerce": ("commerce",),
}

JOB_GROUP_DESCRIPTIONS: dict[str, str] = {
    "all": "All jobs",
    "industrial": "Manufacturing, logistics, and construction jobs",
    "services": "Business services, care, education, health, and local services jobs",
    "commerce": "Commerce jobs",
}


def build_job_group_specs(
    horizons_minutes: Sequence[int] = (10, 20),
) -> tuple[JobGroupSpec, ...]:
    specs: list[JobGroupSpec] = []
    for horizon in horizons_minutes:
        for group_id, source_families in JOB_GROUP_SOURCE_FAMILIES.items():
            specs.append(
                JobGroupSpec(
                    group_id=f"{group_id}_{horizon}",
                    model_column=f"jobs_group_{group_id}_{horizon}_2025_scaled",
                    source_columns=tuple(
                        f"jobs_{family}_{horizon}_2025_scaled"
                        for family in source_families
                    ),
                    description=JOB_GROUP_DESCRIPTIONS[group_id],
                    horizon_minutes=int(horizon),
                ),
            )
    return tuple(specs)


def add_job_group_features(
    neighborhood_features: pd.DataFrame,
    specs: Sequence[JobGroupSpec] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs = tuple(specs or build_job_group_specs())
    missing_columns = sorted(
        {
            column
            for spec in specs
            for column in spec.source_columns
            if column not in neighborhood_features.columns
        },
    )
    if missing_columns:
        msg = f"neighborhood_features missing job group inputs: {missing_columns}"
        raise ValueError(msg)

    duplicated_outputs = sorted(
        {
            spec.model_column
            for spec in specs
            if spec.model_column in neighborhood_features.columns
        },
    )
    if duplicated_outputs:
        msg = f"job group outputs already exist: {duplicated_outputs}"
        raise ValueError(msg)

    with_groups = neighborhood_features.copy()
    rows: list[dict[str, object]] = []
    for spec in specs:
        with_groups[spec.model_column] = (
            with_groups.loc[:, list(spec.source_columns)].astype(float).mean(axis=1)
        )
        rows.append(
            {
                "group_id": spec.group_id,
                "model_column": spec.model_column,
                "source_columns": list(spec.source_columns),
                "source_count": len(spec.source_columns),
                "horizon_minutes": spec.horizon_minutes,
                "description": spec.description,
            },
        )

    return with_groups, pd.DataFrame(rows)
