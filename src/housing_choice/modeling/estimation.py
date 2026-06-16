from __future__ import annotations

import math
from typing import TYPE_CHECKING, Protocol, cast

import biogeme.database as db
import numpy as np
import pandas as pd
from biogeme import models
from biogeme.biogeme import BIOGEME
from biogeme.expressions import Beta, Variable
from biogeme.parameters import Parameters
from biogeme.results_processing import get_pandas_estimated_parameters
from scipy.optimize import minimize
from scipy.special import logsumexp

from housing_choice.modeling._utils import safe_identifier
from housing_choice.modeling.choice_data import (
    build_choice_dataframe,
    validate_choice_dataframe,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ModelArtifact = dict[str, object]


class DerivativeCheckResult(Protocol):
    errors_gradient: np.ndarray
    errors_hessian: np.ndarray


class DerivativeCheckModel(Protocol):
    def check_derivatives(self, *, verbose: bool) -> DerivativeCheckResult: ...


def fit_fast_mnl_screen(
    spec_id: str,
    static_cols: Sequence[str],
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    built_area_cols: Sequence[str],
) -> tuple[dict[str, object], pd.DataFrame]:
    model_feature_cols = [*static_cols, "log_built_area_ha"]
    y = transactions["neighborhood_idx"].astype(int).to_numpy()
    years = transactions["purchase_year"].astype(int).to_numpy()
    n_obs = len(y)
    n_alt = len(neighborhood_features)
    choice_rows = np.arange(n_obs)

    year_to_log_built_area = {
        int(col.rsplit("_", maxsplit=1)[1]): np.log1p(
            neighborhood_features[col].astype(float).to_numpy() / 10_000,
        )
        for col in built_area_cols
    }
    log_built_area_by_choice_year = np.vstack(
        [year_to_log_built_area[int(year)] for year in years],
    )
    static_x = neighborhood_features.loc[:, static_cols].astype(float).to_numpy()
    x = np.empty((n_obs, n_alt, len(model_feature_cols)), dtype=float)
    x[:, :, : len(static_cols)] = static_x[None, :, :]
    x[:, :, -1] = log_built_area_by_choice_year
    chosen_x = x[choice_rows, y, :]

    def nll(beta: np.ndarray) -> float:
        utility = x @ beta
        return -float((chosen_x @ beta - logsumexp(utility, axis=1)).sum())

    opt = minimize(
        nll,
        np.zeros(x.shape[2]),
        method="L-BFGS-B",
        options={"maxiter": 1000, "ftol": 1e-10, "gtol": 1e-6},
    )
    final_ll = -float(opt.fun)
    n_params = len(model_feature_cols)
    coefficient_frame = pd.DataFrame(
        {
            "spec_id": spec_id,
            "feature": model_feature_cols,
            "screen_coef": opt.x,
        },
    )
    return (
        {
            "spec_id": spec_id,
            "parameters": n_params,
            "sample_size": n_obs,
            "final_log_likelihood": final_ll,
            "aic": 2 * n_params - 2 * final_ll,
            "bic": math.log(n_obs) * n_params - 2 * final_ll,
            "screen_converged": bool(opt.success),
            "screen_message": str(opt.message),
        },
        coefficient_frame,
    )


def make_biogeme_parameters() -> Parameters:
    params = Parameters()
    for name, value in {
        "generate_yaml": False,
        "generate_html": False,
        "generate_netcdf": False,
        "save_validation_results": False,
        "save_iterations": False,
        "use_jit": True,
        "numerically_safe": False,
        "seed": 42,
    }.items():
        params.set_value(name=name, value=value)
    return params


def fit_biogeme_model(  # noqa: PLR0913
    spec_id: str,
    static_cols: Sequence[str],
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    built_area_cols: Sequence[str],
    model_prefix: str = "m10",
    missing_value_sentinel: int = 99999,
) -> ModelArtifact:
    choice_frame, model_feature_cols = build_choice_dataframe(
        neighborhood_features,
        transactions,
        static_cols,
        built_area_cols,
    )
    validation = validate_choice_dataframe(
        choice_frame,
        model_feature_cols,
        len(neighborhood_features),
        missing_value_sentinel=missing_value_sentinel,
    )
    if not validation["passed"].all():
        failed = validation.loc[~validation["passed"], "check"].tolist()
        msg = f"Choice frame validation failed for {spec_id}: {failed}"
        raise ValueError(msg)

    database = db.Database(f"db_{safe_identifier(spec_id)}", choice_frame)
    choice = Variable("neighborhood_idx")
    beta_name_by_feature = {
        feature: f"b_{safe_identifier(feature)}" for feature in model_feature_cols
    }
    if len(set(beta_name_by_feature.values())) != len(beta_name_by_feature):
        msg = f"Duplicate beta names for {spec_id}"
        raise ValueError(msg)
    betas = {
        feature: Beta(beta_name, 0, None, None, 0)
        for feature, beta_name in beta_name_by_feature.items()
    }

    utilities = {}
    availability = {}
    for idx in range(len(neighborhood_features)):
        var_map = {
            feature: Variable(f"{feature}_{idx}") for feature in model_feature_cols
        }
        utilities[idx] = sum(
            betas[feature] * var_map[feature] for feature in model_feature_cols
        )
        availability[idx] = 1

    log_probability = models.loglogit(utilities, availability, choice)
    biogeme_model = BIOGEME(
        database=database,  # ty: ignore[unknown-argument]
        formulas=log_probability,  # ty: ignore[unknown-argument]
        parameters=make_biogeme_parameters(),  # ty: ignore[unknown-argument]
    )
    biogeme_model.model_name = f"{model_prefix}_{safe_identifier(spec_id)}"  # ty: ignore[unresolved-attribute]
    results = biogeme_model.estimate(recycle=False, run_bootstrap=False)  # ty: ignore[unresolved-attribute]
    estimated_parameters = get_pandas_estimated_parameters(estimation_results=results)
    feature_by_beta_name = {value: key for key, value in beta_name_by_feature.items()}
    estimated_parameters = estimated_parameters.assign(
        spec_id=spec_id,
        feature=lambda df: df["Name"].map(feature_by_beta_name).fillna(df["Name"]),
    )
    raw_results = getattr(results, "raw_estimation_results", None)
    optimization_messages = getattr(raw_results, "optimization_messages", {}) or {}

    return {
        "spec_id": spec_id,
        "static_cols": list(static_cols),
        "model_feature_cols": model_feature_cols,
        "choice_frame": choice_frame,
        "validation": validation,
        "biogeme_model": biogeme_model,
        "results": results,
        "estimated_parameters": estimated_parameters,
        "beta_name_by_feature": beta_name_by_feature,
        "optimization_messages": optimization_messages,
        "summary_row": {
            "spec_id": spec_id,
            "parameters": results.number_of_parameters,
            "sample_size": results.sample_size,
            "final_log_likelihood": results.final_log_likelihood,
            "aic": results.akaike_information_criterion,
            "bic": results.bayesian_information_criterion,
            "algorithm_has_converged": bool(
                getattr(results, "algorithm_has_converged", False),
            ),
        },
    }


def run_derivative_check(artifact: Mapping[str, object]) -> pd.DataFrame:
    try:
        biogeme_model = cast("DerivativeCheckModel", artifact["biogeme_model"])
        check = biogeme_model.check_derivatives(verbose=False)
        return pd.DataFrame(
            [
                {
                    "spec_id": artifact["spec_id"],
                    "check_completed": True,
                    "max_abs_gradient_error": float(
                        np.max(np.abs(check.errors_gradient)),
                    ),
                    "max_abs_hessian_error": float(
                        np.max(np.abs(check.errors_hessian)),
                    ),
                    "error": "",
                },
            ],
        )
    except Exception as exc:  # noqa: BLE001
        return pd.DataFrame(
            [
                {
                    "spec_id": artifact["spec_id"],
                    "check_completed": False,
                    "max_abs_gradient_error": np.nan,
                    "max_abs_hessian_error": np.nan,
                    "error": str(exc),
                },
            ],
        )


def predict_choice_shares(
    artifact: Mapping[str, object],
    neighborhood_features: pd.DataFrame,
    transactions: pd.DataFrame,
    built_area_cols: Sequence[str],
) -> pd.DataFrame:
    estimated_parameters = cast("pd.DataFrame", artifact["estimated_parameters"])
    params = estimated_parameters.set_index("feature")["Value"].to_dict()
    static_cols = cast("list[str]", artifact["static_cols"])
    static_utility = neighborhood_features.loc[:, static_cols].astype(
        float,
    ).to_numpy() @ np.array([params[col] for col in static_cols])
    built_beta = params["log_built_area_ha"]
    year_to_log_built_area = {
        int(col.rsplit("_", maxsplit=1)[1]): np.log1p(
            neighborhood_features[col].astype(float).to_numpy() / 10_000,
        )
        for col in built_area_cols
    }
    probabilities = []
    for year in transactions["purchase_year"].astype(int):
        utility = static_utility + built_beta * year_to_log_built_area[int(year)]
        probability = np.exp(utility - logsumexp(utility))
        probabilities.append(probability)
    predicted_share = np.vstack(probabilities).mean(axis=0)
    observed_share = (
        transactions["neighborhood_idx"]
        .value_counts(normalize=True)
        .reindex(neighborhood_features.index, fill_value=0)
        .sort_index()
        .to_numpy()
    )
    return pd.DataFrame(
        {
            "neighborhood_idx": neighborhood_features.index,
            "neighborhood": neighborhood_features["name_detail"].to_numpy(),
            "observed_share": observed_share,
            "predicted_share": predicted_share,
            "share_error": predicted_share - observed_share,
            "abs_share_error": np.abs(predicted_share - observed_share),
        },
    ).sort_values("observed_share", ascending=False)
