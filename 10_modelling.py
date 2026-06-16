import marimo

__generated_with = "0.23.9"
app = marimo.App()


@app.cell
def _():

    import math
    import re
    import warnings
    from pathlib import Path

    import biogeme.database as db
    import geopandas as gpd
    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import seaborn as sns
    from biogeme import models
    from biogeme.biogeme import BIOGEME
    from biogeme.expressions import Beta, Variable
    from biogeme.parameters import Parameters
    from biogeme.results_processing import get_pandas_estimated_parameters
    from scipy.optimize import minimize
    from scipy.special import logsumexp

    warnings.filterwarnings("ignore", category=FutureWarning)
    return (
        BIOGEME,
        Beta,
        Parameters,
        Path,
        Variable,
        db,
        get_pandas_estimated_parameters,
        gpd,
        logsumexp,
        math,
        minimize,
        mo,
        models,
        np,
        pd,
        plt,
        re,
        sns,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Housing Choice Modelling

    Clean modelling workflow for neighborhood choice models. The notebook consumes the canonical neighborhood feature export from 08_generate_neighborhood_features.py and keeps exploratory screening separate from final Biogeme estimation.
    """)
    return


@app.cell
def _(Path):

    NEIGHBORHOOD_FEATURES_PATH = Path("./data/processed/col_final.gpkg")
    TRANSACTIONS_PATH = Path("./data/processed/transactions_final.parquet")

    TRANSACTION_THRESH = 20
    MODELING_YEAR_MIN = 2020
    MODELING_YEAR_MAX = 2025
    BIOGEME_MODEL_PREFIX = "m10"

    # Number of non-baseline screened specifications to pass to Biogeme.
    # This keeps final estimation bounded while making finalist selection less brittle.
    FINALIST_COUNT = 8

    TARGET_SCALE_LOWER = 1.0
    TARGET_SCALE_UPPER = 10.0
    MISSING_VALUE_SENTINEL = 99999

    return (
        BIOGEME_MODEL_PREFIX,
        FINALIST_COUNT,
        MISSING_VALUE_SENTINEL,
        MODELING_YEAR_MAX,
        MODELING_YEAR_MIN,
        NEIGHBORHOOD_FEATURES_PATH,
        TARGET_SCALE_LOWER,
        TARGET_SCALE_UPPER,
        TRANSACTIONS_PATH,
        TRANSACTION_THRESH,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Setup Helpers

    These functions define the reusable modelling machinery: feature cataloging, scaling, diagnostics, choice-frame construction, fast screening, guarded Biogeme estimation, and prediction summaries.
    """)
    return


@app.cell
def _(
    BIOGEME,
    BIOGEME_MODEL_PREFIX,
    Beta,
    MISSING_VALUE_SENTINEL,
    Parameters,
    TARGET_SCALE_LOWER,
    TARGET_SCALE_UPPER,
    Variable,
    db,
    get_pandas_estimated_parameters,
    logsumexp,
    math,
    minimize,
    models,
    np,
    pd,
    re,
):

    def safe_identifier(value):
        safe = re.sub(r"[^0-9A-Za-z_]+", "_", str(value)).strip("_")
        if not safe:
            return "unnamed"
        if safe[0].isdigit():
            return f"v_{safe}"
        return safe

    def nice_scale_denominator(values, target_p75=5.0):
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
        candidates = []
        for exp in range(exponent - 2, exponent + 3):
            for multiplier in [1, 2, 5, 10]:
                candidates.append(multiplier * (10**exp))
        return float(
            min(
                candidates,
                key=lambda candidate: abs(math.log(candidate / raw_denominator)),
            )
        )

    def build_feature_catalog(neighborhood_raw):
        rows = []
        selected_mfg_features = {
            "mfg_distance_nearest_cluster_km": "scaled_distance",
            "log_mfg_jobs_within_2km": "already_log_scaled",
            "log_mfg_cluster_gravity_inv_sq": "already_log_scaled",
        }

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
                    }
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
                    }
                )
            elif column.startswith("jobs_") and column.endswith("_2025"):
                source = pd.to_numeric(neighborhood_raw[column], errors="coerce")
                eligible = source.nunique(dropna=True) > 1
                denominator = nice_scale_denominator(source) if eligible else np.nan
                rows.append(
                    {
                        "source_column": column,
                        "model_column": f"{column}_scaled",
                        "family": "job_accessibility",
                        "role": "job_screen" if eligible else "exclude",
                        "transform": f"divide by {denominator:g} jobs"
                        if eligible
                        else "zero variance",
                        "scale_denominator": denominator,
                        "eligible": bool(eligible),
                        "reason": "candidate job accessibility"
                        if eligible
                        else "zero variance",
                    }
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
                    }
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
                    }
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
                    }
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
                    }
                )
            elif column in selected_mfg_features:
                if selected_mfg_features[column] == "scaled_distance":
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
                        "family": "manufacturing_cluster",
                        "role": "mfg_screen",
                        "transform": transform,
                        "scale_denominator": denominator,
                        "eligible": True,
                        "reason": "selected interpretable cluster exposure",
                    }
                )
            elif (
                column.startswith("mfg_")
                or column.startswith("nearest_mfg_")
                or "_mfg_cluster" in column
            ):
                rows.append(
                    {
                        "source_column": column,
                        "model_column": None,
                        "family": "manufacturing_cluster",
                        "role": "available_not_screened",
                        "transform": "not used in v1 model specs",
                        "scale_denominator": np.nan,
                        "eligible": False,
                        "reason": "kept out to avoid over-specified cluster models",
                    }
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
                    }
                )

        nearest_crossing = neighborhood_raw[
            ["travel_time_crossing_west", "travel_time_crossing_east"]
        ].min(axis=1)
        nearest_denominator = nice_scale_denominator(nearest_crossing)
        rows.append(
            {
                "source_column": "min(travel_time_crossing_west, travel_time_crossing_east)",
                "model_column": "travel_time_nearest_crossing_scaled",
                "family": "travel_time",
                "role": "control",
                "transform": f"divide by {nearest_denominator:g} seconds",
                "scale_denominator": nearest_denominator,
                "eligible": True,
                "reason": "nearest border crossing control",
            }
        )

        return pd.DataFrame(rows)

    def prepare_neighborhood_features(neighborhood_raw, feature_catalog):
        prepared = pd.DataFrame(index=neighborhood_raw.index)
        prepared["name_detail"] = neighborhood_raw["name_detail"]
        prepared["name"] = neighborhood_raw["name"]
        prepared["geometry"] = neighborhood_raw.geometry

        for _, spec in feature_catalog.loc[
            lambda df: df["eligible"] & df["model_column"].notna()
        ].iterrows():
            model_column = spec["model_column"]
            source_column = spec["source_column"]
            if (
                model_column in prepared.columns
                or spec["role"] == "transaction_varying"
            ):
                continue
            if model_column == "access_is_restricted":
                prepared[model_column] = neighborhood_raw["access"].map(
                    {"LIBRE": 0, "RESTRINGIDO": 1}
                )
                continue
            if (
                source_column
                == "min(travel_time_crossing_west, travel_time_crossing_east)"
            ):
                values = neighborhood_raw[
                    ["travel_time_crossing_west", "travel_time_crossing_east"]
                ].min(axis=1)
            else:
                values = pd.to_numeric(neighborhood_raw[source_column], errors="coerce")
            denominator = float(spec["scale_denominator"])
            prepared[model_column] = values.astype(float) / denominator

        for column in neighborhood_raw.columns:
            if column.startswith("built_area_"):
                prepared[column] = pd.to_numeric(
                    neighborhood_raw[column], errors="coerce"
                )

        return prepared

    def prepare_transactions(
        transactions_raw, neighborhood_names, min_year, max_year, threshold
    ):
        transactions = (
            transactions_raw.loc[:, ["address", "purchase_date"]]
            .rename(columns={"address": "neighborhood"})
            .assign(
                purchase_year=lambda df: pd.to_datetime(df["purchase_date"]).dt.year
            )
            .loc[lambda df: df["purchase_year"].between(min_year, max_year)]
            .loc[lambda df: df["neighborhood"].isin(set(neighborhood_names))]
            .reset_index(drop=True)
        )
        counts = (
            transactions["neighborhood"]
            .value_counts()
            .rename_axis("neighborhood")
            .reset_index(name="transactions")
        )
        wanted_names = counts.loc[
            counts["transactions"] >= threshold, "neighborhood"
        ].tolist()
        filtered = transactions.loc[
            transactions["neighborhood"].isin(wanted_names)
        ].reset_index(drop=True)
        return filtered, counts, wanted_names

    def align_choice_data(neighborhood_features, transactions, wanted_names):
        choice_features = (
            neighborhood_features.loc[lambda df: df["name_detail"].isin(wanted_names)]
            .reset_index(drop=True)
            .assign(neighborhood_idx=lambda df: np.arange(len(df)))
            .set_index("neighborhood_idx")
        )
        name_to_idx = choice_features["name_detail"].to_dict()
        name_to_idx = {name: idx for idx, name in name_to_idx.items()}
        choice_transactions = transactions.assign(
            neighborhood_idx=lambda df: df["neighborhood"].map(name_to_idx).astype(int)
        ).reset_index(drop=True)
        return choice_features, choice_transactions, name_to_idx

    def compute_scale_audit(feature_frame, feature_columns):
        rows = []
        for column in feature_columns:
            values = pd.to_numeric(feature_frame[column], errors="coerce")
            finite = values.replace([np.inf, -np.inf], np.nan).dropna()
            uniques = sorted(finite.unique().tolist()) if len(finite) <= 1000 else []
            is_binary = len(uniques) <= 2 and set(uniques).issubset({0, 1})
            abs_finite = finite.abs()
            nonzero_abs = abs_finite.loc[abs_finite.gt(0)]
            if finite.empty:
                warning = "all missing"
            elif is_binary:
                warning = "binary"
            elif abs_finite.quantile(0.75) > TARGET_SCALE_UPPER:
                warning = "too large"
            elif nonzero_abs.empty or nonzero_abs.median() < TARGET_SCALE_LOWER:
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
                }
            )
        return pd.DataFrame(rows).round(4)

    def compute_feature_diagnostics(feature_frame):
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
                }
            )
        vif = (
            pd.DataFrame(vif_rows)
            .sort_values("vif", ascending=False)
            .reset_index(drop=True)
            .round({"vif": 3, "r2": 3})
        )
        return diagnostics, correlation, vif, max_abs_correlation

    def build_feature_diagnostics_frame(
        neighborhood_features, transactions, static_cols, built_area_cols
    ):
        diagnostics = neighborhood_features.loc[:, static_cols].astype(float).copy()
        diagnostics["log_built_area_ha"] = 0.0
        year_weights = transactions["purchase_year"].value_counts(normalize=True)
        for year, weight in year_weights.items():
            built_col = f"built_area_{int(year)}"
            if built_col not in built_area_cols:
                raise ValueError(
                    f"Missing built area column for year {year}: {built_col}"
                )
            diagnostics["log_built_area_ha"] += (
                np.log1p(neighborhood_features[built_col].astype(float).div(10_000))
                * weight
            )
        return diagnostics

    def build_choice_dataframe(
        neighborhood_features, transactions, static_cols, built_area_cols
    ):
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

        dynamic_features = {}
        for idx, row in neighborhood_features.loc[:, built_area_cols].iterrows():
            area_by_year = {
                int(col.rsplit("_", maxsplit=1)[1]): float(row[col])
                for col in built_area_cols
            }
            dynamic_features[f"log_built_area_ha_{idx}"] = np.log1p(
                transactions["purchase_year"]
                .map(area_by_year)
                .astype(float)
                .div(10_000)
            )

        choice_frame = pd.concat(
            [
                transactions.loc[:, ["neighborhood_idx", "purchase_year"]].reset_index(
                    drop=True
                ),
                pd.DataFrame(
                    {key: value for key, value in static_series.items()},
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

    def validate_choice_dataframe(choice_frame, model_feature_cols, n_alternatives):
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
                "passed": not numeric.eq(MISSING_VALUE_SENTINEL).any().any(),
                "value": MISSING_VALUE_SENTINEL,
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

    def fit_fast_mnl_screen(
        spec_id, static_cols, neighborhood_features, transactions, built_area_cols
    ):
        model_feature_cols = [*static_cols, "log_built_area_ha"]
        y = transactions["neighborhood_idx"].astype(int).to_numpy()
        years = transactions["purchase_year"].astype(int).to_numpy()
        n_obs = len(y)
        n_alt = len(neighborhood_features)
        choice_rows = np.arange(n_obs)

        year_to_log_built_area = {
            int(col.rsplit("_", maxsplit=1)[1]): np.log1p(
                neighborhood_features[col].astype(float).to_numpy() / 10_000
            )
            for col in built_area_cols
        }
        log_built_area_by_choice_year = np.vstack(
            [year_to_log_built_area[int(year)] for year in years]
        )
        static_x = neighborhood_features.loc[:, static_cols].astype(float).to_numpy()
        x = np.empty((n_obs, n_alt, len(model_feature_cols)), dtype=float)
        x[:, :, : len(static_cols)] = static_x[None, :, :]
        x[:, :, -1] = log_built_area_by_choice_year
        chosen_x = x[choice_rows, y, :]

        def nll(beta):
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
            }
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

    def make_biogeme_parameters():
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

    def fit_biogeme_model(
        spec_id, static_cols, neighborhood_features, transactions, built_area_cols
    ):
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
        )
        if not validation["passed"].all():
            failed = validation.loc[~validation["passed"], "check"].tolist()
            raise ValueError(f"Choice frame validation failed for {spec_id}: {failed}")

        database = db.Database(f"db_{safe_identifier(spec_id)}", choice_frame)
        choice = Variable("neighborhood_idx")
        beta_name_by_feature = {
            feature: f"b_{safe_identifier(feature)}" for feature in model_feature_cols
        }
        if len(set(beta_name_by_feature.values())) != len(beta_name_by_feature):
            raise ValueError(f"Duplicate beta names for {spec_id}")
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
            database=database,
            formulas=log_probability,
            parameters=make_biogeme_parameters(),
        )
        biogeme_model.model_name = f"{BIOGEME_MODEL_PREFIX}_{safe_identifier(spec_id)}"
        results = biogeme_model.estimate(recycle=False, run_bootstrap=False)
        estimated_parameters = get_pandas_estimated_parameters(
            estimation_results=results
        )
        feature_by_beta_name = {
            value: key for key, value in beta_name_by_feature.items()
        }
        estimated_parameters = estimated_parameters.assign(
            spec_id=spec_id,
            feature=lambda df: df["Name"].map(feature_by_beta_name).fillna(df["Name"]),
        )
        raw_results = getattr(results, "raw_estimation_results", None)
        optimization_messages = getattr(raw_results, "optimization_messages", {}) or {}

        return {
            "spec_id": spec_id,
            "static_cols": static_cols,
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
                    getattr(results, "algorithm_has_converged", False)
                ),
            },
        }

    def run_derivative_check(artifact):
        try:
            check = artifact["biogeme_model"].check_derivatives(verbose=False)
            return pd.DataFrame(
                [
                    {
                        "spec_id": artifact["spec_id"],
                        "check_completed": True,
                        "max_abs_gradient_error": float(
                            np.max(np.abs(check.errors_gradient))
                        ),
                        "max_abs_hessian_error": float(
                            np.max(np.abs(check.errors_hessian))
                        ),
                        "error": "",
                    }
                ]
            )
        except Exception as exc:
            return pd.DataFrame(
                [
                    {
                        "spec_id": artifact["spec_id"],
                        "check_completed": False,
                        "max_abs_gradient_error": np.nan,
                        "max_abs_hessian_error": np.nan,
                        "error": str(exc),
                    }
                ]
            )

    def predict_choice_shares(
        artifact, neighborhood_features, transactions, built_area_cols
    ):
        params = (
            artifact["estimated_parameters"].set_index("feature")["Value"].to_dict()
        )
        static_cols = artifact["static_cols"]
        static_utility = neighborhood_features.loc[:, static_cols].astype(
            float
        ).to_numpy() @ np.array([params[col] for col in static_cols])
        built_beta = params["log_built_area_ha"]
        year_to_log_built_area = {
            int(col.rsplit("_", maxsplit=1)[1]): np.log1p(
                neighborhood_features[col].astype(float).to_numpy() / 10_000
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
            }
        ).sort_values("observed_share", ascending=False)

    return (
        align_choice_data,
        build_feature_catalog,
        build_feature_diagnostics_frame,
        compute_feature_diagnostics,
        compute_scale_audit,
        fit_biogeme_model,
        fit_fast_mnl_screen,
        predict_choice_shares,
        prepare_neighborhood_features,
        prepare_transactions,
        run_derivative_check,
        safe_identifier,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Data Inputs

    Load the canonical processed inputs. The neighborhood table is produced by 08_generate_neighborhood_features.py; the transaction table is filtered to the same neighborhood naming convention.
    """)
    return


@app.cell
def _(NEIGHBORHOOD_FEATURES_PATH, TRANSACTIONS_PATH, gpd, pd):

    df_neighborhood_raw = gpd.read_file(NEIGHBORHOOD_FEATURES_PATH)
    df_transactions_raw = pd.read_parquet(TRANSACTIONS_PATH)

    input_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhood_features",
                "path": str(NEIGHBORHOOD_FEATURES_PATH),
                "rows": len(df_neighborhood_raw),
                "columns": len(df_neighborhood_raw.columns),
            },
            {
                "artifact": "transactions",
                "path": str(TRANSACTIONS_PATH),
                "rows": len(df_transactions_raw),
                "columns": len(df_transactions_raw.columns),
            },
        ]
    )
    input_summary
    return df_neighborhood_raw, df_transactions_raw


@app.cell
def _(df_transactions_raw, pd):

    transaction_year_counts_raw = (
        df_transactions_raw.assign(
            purchase_year=lambda df: pd.to_datetime(df["purchase_date"]).dt.year
        )["purchase_year"]
        .value_counts()
        .sort_index()
        .rename_axis("purchase_year")
        .reset_index(name="transactions")
    )
    transaction_year_counts_raw
    return (transaction_year_counts_raw,)


@app.cell
def _(transaction_year_counts_raw):

    transaction_year_plot = transaction_year_counts_raw.plot.bar(
        x="purchase_year",
        y="transactions",
        legend=False,
        figsize=(7, 4),
    )
    transaction_year_plot.set_xlabel("Purchase year")
    transaction_year_plot.set_ylabel("Transactions")
    transaction_year_plot.set_title("Transactions by year")
    transaction_year_plot
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Feature Catalog

    The feature catalog classifies every column from col_final.gpkg and defines the model-ready transformed columns used downstream. This keeps source columns, scaling choices, and modelling roles visible.
    """)
    return


@app.cell
def _(build_feature_catalog, df_neighborhood_raw):

    feature_catalog = build_feature_catalog(df_neighborhood_raw)
    feature_catalog_summary = (
        feature_catalog.groupby(["family", "role", "eligible"], dropna=False)
        .size()
        .reset_index(name="columns")
        .sort_values(["family", "role", "eligible"])
    )
    feature_catalog_summary
    return (feature_catalog,)


@app.cell
def _(feature_catalog):

    feature_catalog_display = feature_catalog.loc[
        :,
        [
            "source_column",
            "model_column",
            "family",
            "role",
            "transform",
            "eligible",
            "reason",
        ],
    ]
    feature_catalog_display
    return


@app.cell
def _(df_neighborhood_raw, feature_catalog, pd, prepare_neighborhood_features):

    prepared_neighborhood_features = prepare_neighborhood_features(
        df_neighborhood_raw,
        feature_catalog,
    )

    built_area_cols = sorted(
        [
            col
            for col in prepared_neighborhood_features.columns
            if col.startswith("built_area_")
        ]
    )
    base_control_cols = [
        "accessibility_services_scaled",
        "travel_time_city_center_scaled",
        "travel_time_nearest_crossing_scaled",
        "access_is_restricted",
    ]
    job_candidate_cols = feature_catalog.loc[
        lambda df: df["role"].eq("job_screen") & df["eligible"], "model_column"
    ].tolist()
    mfg_candidate_cols = feature_catalog.loc[
        lambda df: df["role"].eq("mfg_screen") & df["eligible"], "model_column"
    ].tolist()
    model_ready_feature_cols = [
        *base_control_cols,
        *job_candidate_cols,
        *mfg_candidate_cols,
    ]

    prepared_feature_summary = pd.DataFrame(
        [
            {"family": "base_controls", "features": len(base_control_cols)},
            {"family": "job_candidates", "features": len(job_candidate_cols)},
            {
                "family": "manufacturing_cluster_candidates",
                "features": len(mfg_candidate_cols),
            },
            {"family": "built_area_history", "features": len(built_area_cols)},
        ]
    )
    prepared_feature_summary
    return (
        base_control_cols,
        built_area_cols,
        job_candidate_cols,
        mfg_candidate_cols,
        model_ready_feature_cols,
        prepared_neighborhood_features,
    )


@app.cell
def _(
    compute_scale_audit,
    model_ready_feature_cols,
    prepared_neighborhood_features,
):

    scale_audit = compute_scale_audit(
        prepared_neighborhood_features, model_ready_feature_cols
    )
    scale_audit
    return (scale_audit,)


@app.cell
def _(scale_audit):

    scale_warning_summary = (
        scale_audit["scale_warning"]
        .value_counts()
        .rename_axis("scale_warning")
        .reset_index(name="features")
    )
    scale_warning_summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Choice Set Preparation

    The model keeps purchase year so built area can vary by transaction. The temporary transaction threshold limits estimation cost while the workflow is being developed.
    """)
    return


@app.cell
def _(
    MODELING_YEAR_MAX,
    MODELING_YEAR_MIN,
    TRANSACTION_THRESH,
    align_choice_data,
    df_neighborhood_raw,
    df_transactions_raw,
    pd,
    prepare_transactions,
    prepared_neighborhood_features,
):

    (
        df_transactions_model_base,
        transaction_count_by_neighborhood,
        wanted_neighborhoods,
    ) = prepare_transactions(
        df_transactions_raw,
        df_neighborhood_raw["name_detail"],
        MODELING_YEAR_MIN,
        MODELING_YEAR_MAX,
        TRANSACTION_THRESH,
    )
    (
        choice_neighborhood_features,
        df_transactions_model,
        name_to_idx_map,
    ) = align_choice_data(
        prepared_neighborhood_features,
        df_transactions_model_base,
        wanted_neighborhoods,
    )

    choice_set_summary = pd.DataFrame(
        [
            {
                "transaction_threshold": TRANSACTION_THRESH,
                "candidate_neighborhoods": len(df_neighborhood_raw),
                "model_neighborhoods": len(choice_neighborhood_features),
                "raw_transactions": len(df_transactions_raw),
                "model_transactions": len(df_transactions_model),
                "min_purchase_year": int(df_transactions_model["purchase_year"].min()),
                "max_purchase_year": int(df_transactions_model["purchase_year"].max()),
            }
        ]
    )
    choice_set_summary
    return (
        choice_neighborhood_features,
        df_transactions_model,
        transaction_count_by_neighborhood,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Sample Scope Caveat

    The transaction table is interpreted as a social-housing purchase sample, not a representative sample of all home purchases in the city. That scope is appropriate for this model, but it changes the interpretation of the coefficients: the model describes neighborhoods where social-housing purchases are observed, not unconstrained citywide household preferences.

    Social-housing supply is also spatially structured. If new developments are concentrated in the east or southeast because land is cheaper there, coefficients can partly reflect where projects were built, phased, and sold. The final results should therefore be read as associations with observed social-housing purchases, with supply-side constraints in mind.
    """)
    return


@app.cell
def _(df_transactions_model):

    model_transaction_year_counts = (
        df_transactions_model["purchase_year"]
        .value_counts()
        .sort_index()
        .rename_axis("purchase_year")
        .reset_index(name="transactions")
    )
    model_transaction_year_counts
    return


@app.cell
def _(transaction_count_by_neighborhood):

    top_neighborhood_transaction_counts = transaction_count_by_neighborhood.head(20)
    top_neighborhood_transaction_counts_plot = (
        top_neighborhood_transaction_counts.sort_values("transactions").plot.barh(
            x="neighborhood",
            y="transactions",
            legend=False,
            figsize=(8, 7),
        )
    )
    top_neighborhood_transaction_counts_plot.set_xlabel("Transactions")
    top_neighborhood_transaction_counts_plot.set_ylabel("")
    top_neighborhood_transaction_counts_plot.set_title(
        "Top neighborhoods by transactions"
    )
    top_neighborhood_transaction_counts_plot
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Model Specifications

    Specifications are data. Each exploratory model adds one candidate variable to the shared controls and the transaction-year built-area term.
    """)
    return


@app.cell
def _(
    base_control_cols,
    job_candidate_cols,
    mfg_candidate_cols,
    pd,
    safe_identifier,
):

    model_specs = {
        "baseline_no_jobs": {
            "family": "baseline",
            "static_cols": base_control_cols,
            "candidate_feature": None,
        }
    }
    for _feature in job_candidate_cols:
        model_specs[f"job__{safe_identifier(_feature)}"] = {
            "family": "job_accessibility",
            "static_cols": [_feature, *base_control_cols],
            "candidate_feature": _feature,
        }
    for _feature in mfg_candidate_cols:
        model_specs[f"mfg__{safe_identifier(_feature)}"] = {
            "family": "manufacturing_cluster",
            "static_cols": [_feature, *base_control_cols],
            "candidate_feature": _feature,
        }

    model_spec_summary = pd.DataFrame(
        [
            {
                "spec_id": _spec_id,
                "family": _spec["family"],
                "candidate_feature": _spec["candidate_feature"],
                "static_features": len(_spec["static_cols"]),
                "all_features": ", ".join(_spec["static_cols"] + ["log_built_area_ha"]),
            }
            for _spec_id, _spec in model_specs.items()
        ]
    )
    model_spec_summary
    return model_spec_summary, model_specs


@app.cell
def _(
    build_feature_diagnostics_frame,
    built_area_cols,
    choice_neighborhood_features,
    compute_feature_diagnostics,
    df_transactions_model,
    model_specs,
    pd,
):

    _spec_diag_rows = []
    _spec_vif_frames = []
    for _spec_id, _spec in model_specs.items():
        _diag_frame = build_feature_diagnostics_frame(
            choice_neighborhood_features,
            df_transactions_model,
            _spec["static_cols"],
            built_area_cols,
        )
        _, _, _vif, _max_corr = compute_feature_diagnostics(_diag_frame)
        _spec_diag_rows.append(
            {
                "spec_id": _spec_id,
                "family": _spec["family"],
                "candidate_feature": _spec["candidate_feature"],
                "max_abs_correlation": _max_corr,
                "max_vif": float(_vif["vif"].max()),
            }
        )
        _spec_vif_frames.append(_vif.assign(spec_id=_spec_id))

    spec_diagnostics_summary = (
        pd.DataFrame(_spec_diag_rows)
        .sort_values(["max_vif", "max_abs_correlation"], ascending=False)
        .reset_index(drop=True)
        .round({"max_abs_correlation": 3, "max_vif": 3})
    )
    spec_vif_detail = pd.concat(_spec_vif_frames, ignore_index=True)
    spec_diagnostics_summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Fast Screening

    The screening pass uses the same utility structure as the final model, but estimates quickly with SciPy. Biogeme is reserved for the finalist set.
    """)
    return


@app.cell
def _(
    built_area_cols,
    choice_neighborhood_features,
    df_transactions_model,
    fit_fast_mnl_screen,
    model_specs,
    pd,
):

    _screen_rows = []
    _screen_coef_frames = []
    for _spec_id, _spec in model_specs.items():
        _row, _coef_frame = fit_fast_mnl_screen(
            _spec_id,
            _spec["static_cols"],
            choice_neighborhood_features,
            df_transactions_model,
            built_area_cols,
        )
        _screen_rows.append(
            _row
            | {
                "family": _spec["family"],
                "candidate_feature": _spec["candidate_feature"],
            }
        )
        _screen_coef_frames.append(_coef_frame)

    screening_comparison = (
        pd.DataFrame(_screen_rows).sort_values("aic").reset_index(drop=True)
    )
    screening_comparison["delta_aic_vs_best"] = (
        screening_comparison["aic"] - screening_comparison["aic"].iloc[0]
    ).round(2)
    screening_comparison["delta_aic_vs_baseline"] = (
        screening_comparison["aic"]
        - screening_comparison.loc[
            screening_comparison["spec_id"] == "baseline_no_jobs", "aic"
        ].iloc[0]
    ).round(2)
    screening_coefficients = pd.concat(_screen_coef_frames, ignore_index=True)
    screening_comparison = screening_comparison.round(
        {
            "final_log_likelihood": 3,
            "aic": 3,
            "bic": 3,
        }
    )
    screening_comparison
    return screening_coefficients, screening_comparison


@app.cell
def _(screening_comparison):

    screening_top_table = screening_comparison.loc[
        :,
        [
            "spec_id",
            "family",
            "candidate_feature",
            "aic",
            "delta_aic_vs_baseline",
            "screen_converged",
        ],
    ].head(12)
    screening_top_table
    return


@app.cell
def _(screening_comparison):

    screening_aic_plot_data = (
        screening_comparison.loc[lambda df: df["spec_id"].ne("baseline_no_jobs")]
        .head(15)
        .assign(aic_improvement_vs_baseline=lambda df: -df["delta_aic_vs_baseline"])
        .sort_values("aic_improvement_vs_baseline")
    )
    screening_aic_plot = screening_aic_plot_data.plot.barh(
        x="spec_id",
        y="aic_improvement_vs_baseline",
        legend=False,
        figsize=(9, 7),
    )
    screening_aic_plot.set_xlabel("AIC improvement vs baseline")
    screening_aic_plot.set_ylabel("")
    screening_aic_plot.set_title("Fast-screened candidate specs")
    screening_aic_plot
    return


@app.cell
def _(model_spec_summary, screening_coefficients):

    screening_candidate_coefficients = (
        screening_coefficients.merge(
            model_spec_summary.loc[:, ["spec_id", "candidate_feature", "family"]],
            on="spec_id",
            how="left",
        )
        .loc[lambda df: df["feature"].eq(df["candidate_feature"])]
        .sort_values("screen_coef")
    )
    screening_candidate_coefficient_plot = screening_candidate_coefficients.plot.barh(
        x="spec_id",
        y="screen_coef",
        legend=False,
        figsize=(9, 7),
    )
    screening_candidate_coefficient_plot.axvline(0, color="black", linewidth=0.8)
    screening_candidate_coefficient_plot.set_xlabel("Screened coefficient")
    screening_candidate_coefficient_plot.set_ylabel("")
    screening_candidate_coefficient_plot.set_title("Candidate coefficient signs")
    screening_candidate_coefficient_plot
    return


@app.cell(hide_code=True)
def _(FINALIST_COUNT, mo):
    mo.md(f"""
    ## Biogeme Finalist Selection

    The fast screen ranks candidate one-covariate specifications by AIC. The notebook passes the top `{FINALIST_COUNT}` non-baseline screened candidates, plus the baseline, into Biogeme so the final comparison uses the full estimator while keeping runtime bounded.
    """)
    return


@app.cell
def _(FINALIST_COUNT, model_spec_summary, model_specs, screening_comparison):

    _screened_candidates = (
        screening_comparison.loc[lambda df: df["spec_id"].ne("baseline_no_jobs")]
        .copy()
        .reset_index(drop=True)
    )
    _screened_candidates["screen_rank"] = range(1, len(_screened_candidates) + 1)
    _selected_screened_candidates = _screened_candidates.head(FINALIST_COUNT)

    finalist_spec_ids = [
        "baseline_no_jobs",
        *_selected_screened_candidates["spec_id"].tolist(),
    ]
    finalist_specs = {spec_id: model_specs[spec_id] for spec_id in finalist_spec_ids}

    _screening_columns = (
        screening_comparison.loc[
            :,
            [
                "spec_id",
                "aic",
                "delta_aic_vs_best",
                "delta_aic_vs_baseline",
                "screen_converged",
            ],
        ]
        .rename(
            columns={
                "aic": "screen_aic",
                "delta_aic_vs_best": "screen_delta_aic_vs_best",
                "delta_aic_vs_baseline": "screen_delta_aic_vs_baseline",
            }
        )
        .merge(
            _screened_candidates.loc[:, ["spec_id", "screen_rank"]],
            on="spec_id",
            how="left",
        )
    )
    _finalist_order_by_spec = {
        _spec_id: _rank for _rank, _spec_id in enumerate(finalist_spec_ids)
    }

    finalist_table = (
        model_spec_summary.loc[lambda df: df["spec_id"].isin(finalist_spec_ids)]
        .merge(_screening_columns, on="spec_id", how="left")
        .assign(finalist_order=lambda df: df["spec_id"].map(_finalist_order_by_spec))
        .sort_values("finalist_order")
        .drop(columns="finalist_order")
    )
    finalist_table["screen_rank"] = finalist_table["screen_rank"].fillna(0).astype(int)
    finalist_table["finalist_role"] = finalist_table["screen_rank"].map(
        lambda rank: "baseline" if rank == 0 else f"screen_rank_{rank}"
    )

    finalist_selection_table = finalist_table.loc[
        :,
        [
            "finalist_role",
            "spec_id",
            "family",
            "candidate_feature",
            "screen_aic",
            "screen_delta_aic_vs_baseline",
            "screen_converged",
        ],
    ]
    finalist_selection_table

    return (finalist_specs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Final Biogeme Estimation

    Each finalist receives a fresh Biogeme run with output files, saved iterations, recycle, and bootstrap disabled. The comparison below decides among the baseline and selected finalists using the full Biogeme estimates.
    """)
    return


@app.cell
def _(
    built_area_cols,
    choice_neighborhood_features,
    df_transactions_model,
    finalist_specs,
    fit_biogeme_model,
    model_spec_summary,
    pd,
):

    biogeme_artifacts = {}
    for _spec_id, _spec in finalist_specs.items():
        biogeme_artifacts[_spec_id] = fit_biogeme_model(
            _spec_id,
            _spec["static_cols"],
            choice_neighborhood_features,
            df_transactions_model,
            built_area_cols,
        )

    biogeme_model_comparison = (
        pd.DataFrame(
            [artifact["summary_row"] for artifact in biogeme_artifacts.values()]
        )
        .merge(
            model_spec_summary.loc[:, ["spec_id", "family", "candidate_feature"]],
            on="spec_id",
            how="left",
        )
        .sort_values("aic")
        .reset_index(drop=True)
    )
    biogeme_model_comparison["delta_aic_vs_best"] = (
        biogeme_model_comparison["aic"] - biogeme_model_comparison["aic"].iloc[0]
    ).round(2)
    biogeme_model_comparison["delta_aic_vs_baseline"] = (
        biogeme_model_comparison["aic"]
        - biogeme_model_comparison.loc[
            biogeme_model_comparison["spec_id"] == "baseline_no_jobs", "aic"
        ].iloc[0]
    ).round(2)
    biogeme_model_comparison = biogeme_model_comparison.round(
        {"final_log_likelihood": 3, "aic": 3, "bic": 3}
    )
    biogeme_model_comparison
    return biogeme_artifacts, biogeme_model_comparison


@app.cell
def _(biogeme_artifacts, biogeme_model_comparison, model_specs, pd):

    selected_spec_id = str(biogeme_model_comparison.iloc[0]["spec_id"])
    selected_artifact = biogeme_artifacts[selected_spec_id]
    selected_static_cols = selected_artifact["static_cols"]
    selected_model_feature_cols = selected_artifact["model_feature_cols"]
    selected_results = selected_artifact["results"]
    selected_estimated_parameters = selected_artifact["estimated_parameters"]
    selected_spec_summary = pd.DataFrame(
        [
            {
                "selected_spec_id": selected_spec_id,
                "candidate_feature": model_specs[selected_spec_id]["candidate_feature"],
                "family": model_specs[selected_spec_id]["family"],
                "aic": selected_results.akaike_information_criterion,
                "bic": selected_results.bayesian_information_criterion,
                "final_log_likelihood": selected_results.final_log_likelihood,
                "algorithm_has_converged": bool(
                    getattr(selected_results, "algorithm_has_converged", False)
                ),
            }
        ]
    ).round({"aic": 3, "bic": 3, "final_log_likelihood": 3})
    selected_spec_summary
    return (
        selected_artifact,
        selected_estimated_parameters,
        selected_static_cols,
    )


@app.cell
def _(run_derivative_check, selected_artifact):

    selected_derivative_check = run_derivative_check(selected_artifact)
    selected_derivative_check
    return (selected_derivative_check,)


@app.cell
def _(BIOGEME_MODEL_PREFIX, pd, selected_derivative_check):

    biogeme_guardrail_summary = pd.DataFrame(
        [
            {"guardrail": "generate_yaml", "value": False},
            {"guardrail": "generate_html", "value": False},
            {"guardrail": "save_iterations", "value": False},
            {"guardrail": "estimate_recycle", "value": False},
            {"guardrail": "run_bootstrap", "value": False},
            {"guardrail": "unique_model_name_prefix", "value": BIOGEME_MODEL_PREFIX},
            {
                "guardrail": "selected_derivative_check_completed",
                "value": bool(selected_derivative_check["check_completed"].iloc[0]),
            },
        ]
    )
    biogeme_guardrail_summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Fit, Coefficients, And Prediction Checks

    The final section focuses on the selected Biogeme model: coefficient estimates, diagnostics, and how well predicted shares match observed neighborhood shares.
    """)
    return


@app.cell
def _(selected_estimated_parameters):

    selected_coefficient_summary = (
        selected_estimated_parameters.assign(
            value=lambda df: df["Value"].round(4),
            robust_se=lambda df: df["Robust std err."].round(4),
            robust_t=lambda df: df["Robust t-stat."].round(3),
            robust_p=lambda df: df["Robust p-value"].round(4),
        )
        .loc[:, ["feature", "value", "robust_se", "robust_t", "robust_p"]]
        .sort_values("feature")
    )
    selected_coefficient_summary
    return


@app.cell
def _(selected_estimated_parameters):

    selected_coefficient_plot_data = selected_estimated_parameters.sort_values("Value")
    selected_coefficient_plot = selected_coefficient_plot_data.plot.barh(
        x="feature",
        y="Value",
        xerr="Robust std err.",
        legend=False,
        figsize=(8, 5),
    )
    selected_coefficient_plot.axvline(0, color="black", linewidth=0.8)
    selected_coefficient_plot.set_xlabel("Coefficient estimate")
    selected_coefficient_plot.set_ylabel("")
    selected_coefficient_plot.set_title("Selected Biogeme coefficients")
    selected_coefficient_plot
    return


@app.cell
def _(
    build_feature_diagnostics_frame,
    built_area_cols,
    choice_neighborhood_features,
    compute_feature_diagnostics,
    df_transactions_model,
    selected_static_cols,
):

    selected_diagnostics_frame = build_feature_diagnostics_frame(
        choice_neighborhood_features,
        df_transactions_model,
        selected_static_cols,
        built_area_cols,
    )
    (
        selected_feature_diagnostics,
        selected_feature_correlation,
        selected_feature_vif,
        selected_max_abs_correlation,
    ) = compute_feature_diagnostics(selected_diagnostics_frame)
    selected_feature_vif
    return selected_diagnostics_frame, selected_feature_correlation


@app.cell
def _(plt, selected_feature_correlation, sns):

    selected_correlation_heatmap_figure, selected_correlation_heatmap_axis = (
        plt.subplots(figsize=(7, 6))
    )
    sns.heatmap(
        selected_feature_correlation,
        annot=True,
        cmap="vlag",
        center=0,
        vmin=-1,
        vmax=1,
        ax=selected_correlation_heatmap_axis,
    )
    selected_correlation_heatmap_axis.set_title("Selected spec feature correlation")
    selected_correlation_heatmap_figure.tight_layout()
    selected_correlation_heatmap_figure
    return


@app.cell
def _(
    built_area_cols,
    choice_neighborhood_features,
    df_transactions_model,
    predict_choice_shares,
    selected_artifact,
):

    selected_choice_share_summary = predict_choice_shares(
        selected_artifact,
        choice_neighborhood_features,
        df_transactions_model,
        built_area_cols,
    ).round(
        {
            "observed_share": 4,
            "predicted_share": 4,
            "share_error": 4,
            "abs_share_error": 4,
        }
    )
    selected_choice_share_summary.head(15)
    return (selected_choice_share_summary,)


@app.cell
def _(selected_choice_share_summary):

    choice_share_plot_data = selected_choice_share_summary.head(15).sort_values(
        "observed_share"
    )
    choice_share_plot_axis = choice_share_plot_data.plot.barh(
        x="neighborhood",
        y=["observed_share", "predicted_share"],
        figsize=(9, 7),
    )
    choice_share_plot_axis.set_xlabel("Share")
    choice_share_plot_axis.set_ylabel("")
    choice_share_plot_axis.set_title("Observed vs predicted shares")
    choice_share_plot_axis
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Interpreting The Logistics Coefficient

    The selected model assigns a large negative coefficient to `jobs_logistics_20_2025_scaled`. This does not necessarily mean that social-housing buyers dislike access to logistics jobs in a direct preference sense.

    In this sample, transactions mostly come from social-housing developments, and those developments are not spread uniformly across the city. The logistics coefficient can therefore capture several things at once: industrial land exposure, truck-corridor proximity, the geography of cheaper developable land, omitted developer or project characteristics, and the fact that some high-logistics areas may simply have fewer social-housing units available to purchase.

    The diagnostics below check whether the sign is already visible in the raw modeled choice set by comparing neighborhood transaction shares with logistics accessibility.
    """)
    return


@app.cell
def _(selected_choice_share_summary, selected_diagnostics_frame):

    _logistics_feature = "jobs_logistics_20_2025_scaled"
    _share_frame = selected_choice_share_summary.set_index("neighborhood_idx").loc[
        :,
        ["neighborhood", "observed_share", "predicted_share", "share_error"],
    ]

    logistics_sample_diagnostic_frame = selected_diagnostics_frame.join(
        _share_frame
    ).reset_index(names="neighborhood_idx")

    _logistics_correlation_features = [
        _logistics_feature,
        "accessibility_services_scaled",
        "travel_time_city_center_scaled",
        "travel_time_nearest_crossing_scaled",
        "access_is_restricted",
        "log_built_area_ha",
    ]
    logistics_correlation_summary = (
        logistics_sample_diagnostic_frame.loc[
            :,
            [*_logistics_correlation_features, "observed_share"],
        ]
        .corr(numeric_only=True)["observed_share"]
        .drop("observed_share")
        .rename("correlation_with_observed_share")
        .reset_index(name="correlation_with_observed_share")
        .rename(columns={"index": "feature"})
        .sort_values("correlation_with_observed_share")
        .round(3)
    )

    logistics_correlation_summary

    return (logistics_sample_diagnostic_frame,)


@app.cell
def _(logistics_sample_diagnostic_frame):

    _logistics_feature = "jobs_logistics_20_2025_scaled"
    logistics_high_accessibility_table = (
        logistics_sample_diagnostic_frame.sort_values(
            _logistics_feature, ascending=False
        )
        .assign(
            observed_share=lambda df: df["observed_share"].round(4),
            predicted_share=lambda df: df["predicted_share"].round(4),
            jobs_logistics_20_2025_scaled=lambda df: df[_logistics_feature].round(3),
            travel_time_city_center_scaled=lambda df: df[
                "travel_time_city_center_scaled"
            ].round(3),
            travel_time_nearest_crossing_scaled=lambda df: df[
                "travel_time_nearest_crossing_scaled"
            ].round(3),
            accessibility_services_scaled=lambda df: df[
                "accessibility_services_scaled"
            ].round(3),
            log_built_area_ha=lambda df: df["log_built_area_ha"].round(3),
        )
        .loc[
            :,
            [
                "neighborhood",
                _logistics_feature,
                "observed_share",
                "predicted_share",
                "travel_time_city_center_scaled",
                "travel_time_nearest_crossing_scaled",
                "accessibility_services_scaled",
                "log_built_area_ha",
            ],
        ]
        .head(15)
    )
    logistics_high_accessibility_table

    return


@app.cell
def _(logistics_sample_diagnostic_frame, plt, sns):

    _logistics_feature = "jobs_logistics_20_2025_scaled"
    logistics_share_plot_figure, logistics_share_plot_axis = plt.subplots(
        figsize=(7, 5)
    )
    sns.regplot(
        data=logistics_sample_diagnostic_frame,
        x=_logistics_feature,
        y="observed_share",
        ax=logistics_share_plot_axis,
        scatter_kws={"s": 55, "alpha": 0.8},
        line_kws={"color": "black", "linewidth": 1.2},
    )
    logistics_share_plot_axis.set_xlabel(
        "Logistics job accessibility, 20-minute catchment (scaled)"
    )
    logistics_share_plot_axis.set_ylabel("Observed transaction share")
    logistics_share_plot_axis.set_title(
        "Observed social-housing transaction share vs logistics accessibility"
    )
    logistics_share_plot_figure.tight_layout()
    logistics_share_plot_figure

    return


if __name__ == "__main__":
    app.run()
