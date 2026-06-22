from __future__ import annotations

from housing_choice.modeling._utils import safe_identifier
from housing_choice.modeling.availability import (
    ActiveChoiceSet,
    add_centroid_spatial_controls,
    build_active_choice_set,
    build_availability_choice_dataframe,
    prepare_baseline_transactions,
    summarize_availability_by_transaction,
    validate_availability_choice_dataframe,
)
from housing_choice.modeling.choice_data import (
    align_choice_data,
    build_choice_dataframe,
    prepare_transactions,
    validate_choice_dataframe,
)
from housing_choice.modeling.diagnostics import (
    build_feature_diagnostics_frame,
    compute_feature_diagnostics,
)
from housing_choice.modeling.estimation import (
    fit_biogeme_availability_model,
    fit_biogeme_model,
    fit_fast_mnl_screen,
    predict_availability_choice_shares,
    predict_choice_shares,
    run_derivative_check,
)
from housing_choice.modeling.features import (
    build_feature_catalog,
    compute_scale_audit,
    nice_scale_denominator,
    prepare_neighborhood_features,
)
from housing_choice.modeling.specs import (
    build_combination_model_specs,
    build_single_candidate_model_specs,
)

__all__ = [
    "ActiveChoiceSet",
    "add_centroid_spatial_controls",
    "align_choice_data",
    "build_active_choice_set",
    "build_availability_choice_dataframe",
    "build_choice_dataframe",
    "build_combination_model_specs",
    "build_feature_catalog",
    "build_feature_diagnostics_frame",
    "build_single_candidate_model_specs",
    "compute_feature_diagnostics",
    "compute_scale_audit",
    "fit_biogeme_availability_model",
    "fit_biogeme_model",
    "fit_fast_mnl_screen",
    "nice_scale_denominator",
    "predict_availability_choice_shares",
    "predict_choice_shares",
    "prepare_baseline_transactions",
    "prepare_neighborhood_features",
    "prepare_transactions",
    "run_derivative_check",
    "safe_identifier",
    "summarize_availability_by_transaction",
    "validate_availability_choice_dataframe",
    "validate_choice_dataframe",
]
