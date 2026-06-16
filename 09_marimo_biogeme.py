import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import math
    import warnings

    import biogeme.database as db
    import geopandas as gpd
    import marimo as mo
    import numpy as np
    import pandas as pd
    import scipy.optimize as scipy_optimize
    import scipy.special as scipy_special
    from biogeme import models
    from biogeme.biogeme import BIOGEME
    from biogeme.expressions import Beta, Variable
    from biogeme.results_processing import get_pandas_estimated_parameters
    from pandas.errors import PerformanceWarning


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Housing choice model with Biogeme

    This notebook prepares neighborhood-level alternatives, estimates a baseline multinomial logit model, screens candidate job-accessibility covariates, and compares the selected model against the baseline.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Configuration and feature definitions

    Shared imports live in the setup cell. The constants below define the feature groups reused by the baseline, screening, and selected Biogeme models.
    """)
    return


@app.cell
def _():
    built_area_cols = [f"built_area_{year}" for year in range(2020, 2026)]

    static_feature_cols = [
        "jobs_accessibility_2025",
        "accessibility_services",
        "travel_time_city_center_min",
        "travel_time_nearest_crossing_min",
        "access_is_restricted",
    ]

    model_feature_cols = [*static_feature_cols, "log_built_area_ha"]

    wanted_cols = [
        "name_detail",
        *static_feature_cols,
        *built_area_cols,
    ]
    print(wanted_cols)
    return (
        built_area_cols,
        model_feature_cols,
        static_feature_cols,
        wanted_cols,
    )


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Shared helper functions

    These helpers keep repeated feature diagnostics, wide choice-frame construction, and Biogeme model setup in one place.
    """)
    return


@app.function
def compute_feature_diagnostics(feature_frame):
    """Return a copied feature frame, correlations, VIFs, and max absolute correlation."""
    _diagnostics = feature_frame.astype(float).copy()
    _correlation = _diagnostics.corr().round(3)

    _vif_rows = []
    for _feature in _diagnostics.columns:
        _y = _diagnostics[_feature].to_numpy()
        _x = _diagnostics.drop(columns=[_feature]).to_numpy()
        _x = np.column_stack([np.ones(len(_x)), _x])
        _beta = np.linalg.lstsq(_x, _y, rcond=None)[0]
        _pred = _x @ _beta
        _ss_res = ((_y - _pred) ** 2).sum()
        _ss_tot = ((_y - _y.mean()) ** 2).sum()
        _r2 = 1 - _ss_res / _ss_tot if _ss_tot else 0.0
        _vif_rows.append(
            {
                "feature": _feature,
                "vif": 1 / (1 - _r2) if _r2 < 1 else math.inf,
                "r2": _r2,
            }
        )

    _vif = (
        pd.DataFrame(_vif_rows)
        .sort_values("vif", ascending=False)
        .reset_index(drop=True)
        .round({"vif": 3, "r2": 3})
    )
    _corr_abs = _correlation.abs().where(~np.eye(len(_correlation), dtype=bool))
    _max_abs_correlation = round(float(_corr_abs.max().max()), 3)
    return _diagnostics, _correlation, _vif, _max_abs_correlation


@app.function
def build_feature_diagnostics_frame(
    neighborhood_features,
    transactions,
    static_cols,
    built_area_cols,
):
    """Build the feature matrix used for correlation and VIF diagnostics."""
    _year_weights = transactions["purchase_year"].value_counts(normalize=True)
    _diagnostics = neighborhood_features.loc[:, static_cols].copy()
    _diagnostics["log_built_area_ha"] = 0.0

    # Weight built area by the observed transaction-year distribution.
    for _year, _weight in _year_weights.items():
        _built_area_col = f"built_area_{int(_year)}"
        _diagnostics["log_built_area_ha"] += (
            np.log1p(neighborhood_features[_built_area_col].astype(float).div(10_000))
            * _weight
        )
    return _diagnostics


@app.function
def build_choice_frame(
    transactions,
    neighborhood_features,
    static_cols,
    model_cols,
    built_area_cols,
):
    """Expand transaction choices into Biogeme alternative-specific columns."""
    _static_values = (
        neighborhood_features.loc[:, static_cols]
        .reset_index(names="index")
        .melt(id_vars="index")
        .assign(index=lambda df: df["index"].astype(str))
        .assign(variable=lambda df: df["variable"] + "_" + df["index"])
        .drop(columns=["index"])
        .set_index("variable")["value"]
        .to_dict()
    )

    _dynamic_feature_values = {}
    for _idx, _row in neighborhood_features.loc[:, built_area_cols].iterrows():
        _area_by_year = {
            int(_col.rsplit("_", maxsplit=1)[1]): float(_row[_col])
            for _col in built_area_cols
        }
        _dynamic_feature_values[f"log_built_area_ha_{_idx}"] = np.log1p(
            transactions["purchase_year"].map(_area_by_year).astype(float).div(10_000)
        )

    # Assigning many alternative-specific columns can fragment pandas internals.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PerformanceWarning)
        _choice_frame = transactions.assign(
            **_static_values,
            **_dynamic_feature_values,
        )

    _model_columns = [
        "neighborhood",
        "purchase_year",
        *[
            f"{_feature}_{_idx}"
            for _idx in neighborhood_features.index
            for _feature in model_cols
        ],
    ]
    return _choice_frame.loc[:, _model_columns].copy()


@app.function
def fit_biogeme_model(
    model_name,
    database_name,
    transactions_augmented,
    neighborhood_features,
    model_cols,
    *,
    beta_name_prefix="",
    generate_html=False,
    parameters="./params/test_1.yaml",
):
    """Estimate a Biogeme multinomial-logit model for a wide choice frame."""
    _database = db.Database(database_name, transactions_augmented)
    _choice = Variable("neighborhood")
    _betas = {
        _col: Beta(f"{beta_name_prefix}beta_{_col}", 0, None, None, 0)
        for _col in model_cols
    }

    _utilities = {}
    _availability = {}
    for _idx in neighborhood_features.index:
        _var_map = {_col: Variable(f"{_col}_{_idx}") for _col in model_cols}
        _utilities[_idx] = sum(_betas[_col] * _var_map[_col] for _col in model_cols)
        _availability[_idx] = 1

    _logprob = models.loglogit(_utilities, _availability, _choice)
    _model = BIOGEME(
        _database,
        _logprob,
        parameters=parameters,
        generate_yaml=False,
        generate_html=generate_html,
        save_iterations=False,
    )
    _model.model_name = model_name
    _results = _model.estimate()
    return {
        "database": _database,
        "model": _model,
        "results": _results,
    }


@app.function
def parse_job_covariate(job_col):
    """Split a jobs_*_2025 column into sector and travel-time threshold."""
    if job_col is None:
        return "none", None
    _body = job_col.removeprefix("jobs_").removesuffix("_2025")
    _parts = _body.rsplit("_", maxsplit=1)
    if len(_parts) == 2 and _parts[1].isdigit():
        return _parts[0], int(_parts[1])
    return _body, None


@app.function
def fit_job_covariate_screen(
    job_col,
    control_cols,
    job_covariate_features,
    choice_indices,
    log_built_area_by_choice_year,
    year_to_log_built_area,
    year_weights,
):
    """Fit one screened job-covariate specification with scipy."""
    _static_cols = list(control_cols) if job_col is None else [job_col, *control_cols]
    _static_x = job_covariate_features.loc[:, _static_cols].astype(float).to_numpy()
    _n = len(choice_indices)
    _j = len(job_covariate_features)
    _x = np.empty((_n, _j, len(_static_cols) + 1), dtype=float)
    _x[:, :, : len(_static_cols)] = _static_x[None, :, :]
    _x[:, :, -1] = log_built_area_by_choice_year
    _chosen_x = _x[np.arange(_n), choice_indices, :]

    def _negative_log_likelihood(_beta):
        _utility = _x @ _beta
        return -float(
            (_chosen_x @ _beta - scipy_special.logsumexp(_utility, axis=1)).sum()
        )

    _opt = scipy_optimize.minimize(
        _negative_log_likelihood,
        np.zeros(_x.shape[2]),
        method="L-BFGS-B",
        options={"maxiter": 1000, "ftol": 1e-10, "gtol": 1e-6},
    )

    _diagnostics_frame = job_covariate_features.loc[:, _static_cols].copy()
    _diagnostics_frame["log_built_area_ha"] = 0.0
    for _year, _weight in year_weights.items():
        _diagnostics_frame["log_built_area_ha"] += (
            year_to_log_built_area[int(_year)] * _weight
        )
    _, _, _feature_vif, _max_corr = compute_feature_diagnostics(_diagnostics_frame)

    _ll = -float(_opt.fun)
    _parameters = _x.shape[2]
    _sector, _minutes = parse_job_covariate(job_col)
    return {
        "model": job_col if job_col is not None else "no_jobs",
        "job_feature": job_col if job_col is not None else "none",
        "sector": _sector,
        "minutes": _minutes,
        "screen_converged": bool(_opt.success),
        "parameters": _parameters,
        "sample_size": _n,
        "final_log_likelihood": _ll,
        "aic": 2 * _parameters - 2 * _ll,
        "bic": math.log(_n) * _parameters - 2 * _ll,
        "job_coef": np.nan if job_col is None else float(_opt.x[0]),
        "max_abs_feature_correlation": _max_corr,
        "max_vif": float(_feature_vif["vif"].max()),
    }


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Neighborhood and transaction data

    Load the neighborhood attributes and transaction choices, then keep only neighborhoods with enough observed transactions for estimation.
    """)
    return


@app.cell
def _(wanted_cols):
    df_neighborhood_features = (
        gpd.read_file("./data/processed/col_final.gpkg")
        .assign(
            # Scale raw columns into units that make coefficients easier to read.
            jobs_accessibility_2025=lambda df: df["jobs_all_20_2025"].div(10_000),
            accessibility_services=lambda df: df["accessibility_services"].mul(10),
            travel_time_city_center_min=lambda df: df["travel_time_city_center"].div(
                60
            ),
            travel_time_nearest_crossing_min=lambda df: (
                df[["travel_time_crossing_west", "travel_time_crossing_east"]]
                .min(axis=1)
                .div(60)
            ),
            access_is_restricted=lambda df: df["access"].map(
                {"LIBRE": 0, "RESTRINGIDO": 1}
            ),
        )
        .loc[:, wanted_cols]
        .rename(columns={"name_detail": "name"})
    )
    return (df_neighborhood_features,)


@app.cell
def _(df_neighborhood_features):
    TRANSACTION_THRESH = 20

    _df_transactions_raw = (
        pd.read_parquet("./data/processed/transactions_final.parquet")
        .loc[:, ["address", "purchase_date"]]
        .rename(columns={"address": "neighborhood"})
        .assign(purchase_year=lambda df: pd.to_datetime(df["purchase_date"]).dt.year)
        .loc[lambda df: df["purchase_year"].between(2020, 2025)]
        .drop(columns=["purchase_date"])
    )

    # Keep alternatives with enough observations to support stable estimation.
    transaction_count = _df_transactions_raw["neighborhood"].value_counts()
    wanted_neighborhoods = transaction_count[
        transaction_count >= TRANSACTION_THRESH
    ].index

    df_transactions = _df_transactions_raw.loc[
        lambda df: df["neighborhood"].isin(wanted_neighborhoods)
    ]

    df_neighborhood_features_trimmed = df_neighborhood_features.loc[
        df_neighborhood_features["name"].isin(wanted_neighborhoods)
    ].reset_index(drop=True)

    name_to_idx_map = (
        df_neighborhood_features_trimmed["name"]
        .reset_index()
        .set_index("name")["index"]
        .to_dict()
    )

    df_neighborhood_features_trimmed = (
        df_neighborhood_features_trimmed.assign(
            neighborhood_idx=lambda df: df["name"].map(name_to_idx_map)
        )
        .set_index("neighborhood_idx")
        .drop(columns=["name"])
    )

    df_transactions = df_transactions.assign(
        neighborhood=lambda df: df["neighborhood"].map(name_to_idx_map)
    )
    return df_neighborhood_features_trimmed, df_transactions, name_to_idx_map


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Baseline model

    The baseline uses all-job accessibility, local services, travel-time controls, restricted-access status, and year-specific built area.
    """)
    return


@app.cell
def _(
    built_area_cols,
    df_neighborhood_features_trimmed,
    df_transactions,
    static_feature_cols,
):
    _baseline_diagnostics_frame = build_feature_diagnostics_frame(
        df_neighborhood_features_trimmed,
        df_transactions,
        static_feature_cols,
        built_area_cols,
    )
    (
        df_feature_diagnostics,
        feature_correlation,
        feature_vif,
        max_abs_feature_correlation,
    ) = compute_feature_diagnostics(_baseline_diagnostics_frame)

    feature_correlation, feature_vif
    return feature_vif, max_abs_feature_correlation


@app.cell
def _(
    built_area_cols,
    df_neighborhood_features_trimmed,
    df_transactions,
    model_feature_cols,
    static_feature_cols,
):
    df_transactions_augmented = build_choice_frame(
        df_transactions,
        df_neighborhood_features_trimmed,
        static_feature_cols,
        model_feature_cols,
        built_area_cols,
    )
    return (df_transactions_augmented,)


@app.cell
def _(
    df_neighborhood_features_trimmed,
    df_transactions_augmented,
    model_feature_cols,
):
    baseline_model_artifacts = fit_biogeme_model(
        "test_1",
        "housing_choice_model",
        df_transactions_augmented,
        df_neighborhood_features_trimmed,
        model_feature_cols,
        generate_html=True,
    )
    results = baseline_model_artifacts["results"]
    return (results,)


@app.cell
def _(results):
    print(results.short_summary())
    return


@app.cell
def _(results):
    estimated_parameters = get_pandas_estimated_parameters(estimation_results=results)
    estimated_parameters
    return (estimated_parameters,)


@app.cell
def _(estimated_parameters):
    baseline_coefficient_plot_data = estimated_parameters.assign(
        feature=lambda df: df["Name"].str.replace("beta_", "", regex=False),
    ).sort_values("Value")

    baseline_coefficient_plot = baseline_coefficient_plot_data.plot.barh(
        x="feature",
        y="Value",
        xerr="Robust std err.",
        legend=False,
        figsize=(8, 4),
    )
    baseline_coefficient_plot.axvline(0, color="black", linewidth=0.8)
    baseline_coefficient_plot.set_xlabel("Coefficient estimate")
    baseline_coefficient_plot.set_ylabel("")
    baseline_coefficient_plot.set_title("Baseline model coefficients")
    baseline_coefficient_plot
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Job covariate screen

    Candidate job-accessibility variables are screened with a lightweight multinomial-logit fit before the selected candidate is estimated with Biogeme.
    """)
    return


@app.cell
def _(built_area_cols, df_transactions, name_to_idx_map):
    job_covariate_control_cols = [
        "accessibility_services",
        "travel_time_city_center_min",
        "travel_time_nearest_crossing_min",
        "access_is_restricted",
    ]

    _job_covariate_raw_features = gpd.read_file("./data/processed/col_final.gpkg")
    _job_covariate_all_cols = [
        _col
        for _col in _job_covariate_raw_features.columns
        if _col.startswith("jobs_") and _col.endswith("_2025")
    ]
    job_covariate_candidate_cols = sorted(
        [
            _col
            for _col in _job_covariate_all_cols
            if _job_covariate_raw_features[_col].astype(float).nunique(dropna=True) > 1
        ]
    )
    job_covariate_skipped_cols = sorted(
        set(_job_covariate_all_cols) - set(job_covariate_candidate_cols)
    )

    job_covariate_features = (
        _job_covariate_raw_features.assign(
            accessibility_services=lambda df: df["accessibility_services"].mul(10),
            travel_time_city_center_min=lambda df: df["travel_time_city_center"].div(
                60
            ),
            travel_time_nearest_crossing_min=lambda df: (
                df[["travel_time_crossing_west", "travel_time_crossing_east"]]
                .min(axis=1)
                .div(60)
            ),
            access_is_restricted=lambda df: df["access"].map(
                {"LIBRE": 0, "RESTRINGIDO": 1}
            ),
        )
        .loc[lambda df: df["name_detail"].isin(name_to_idx_map)]
        .assign(neighborhood_idx=lambda df: df["name_detail"].map(name_to_idx_map))
        .set_index("neighborhood_idx")
        .sort_index()
    )
    for _col in job_covariate_candidate_cols:
        job_covariate_features[_col] = (
            job_covariate_features[_col].astype(float).div(10_000)
        )

    # Precompute choice arrays reused across all screened specifications.
    _job_choice_indices = df_transactions["neighborhood"].astype(int).to_numpy()
    _job_purchase_years = df_transactions["purchase_year"].astype(int).to_numpy()
    _job_year_to_log_built_area = {
        int(_col.rsplit("_", maxsplit=1)[1]): np.log1p(
            job_covariate_features[_col].astype(float).to_numpy() / 10_000
        )
        for _col in built_area_cols
    }
    _job_log_built_area_by_choice_year = np.vstack(
        [_job_year_to_log_built_area[int(_year)] for _year in _job_purchase_years]
    )
    _job_year_weights = df_transactions["purchase_year"].value_counts(normalize=True)

    _job_covariate_rows = [
        fit_job_covariate_screen(
            _job_col,
            job_covariate_control_cols,
            job_covariate_features,
            _job_choice_indices,
            _job_log_built_area_by_choice_year,
            _job_year_to_log_built_area,
            _job_year_weights,
        )
        for _job_col in [None, *job_covariate_candidate_cols]
    ]

    job_covariate_screen_comparison = (
        pd.DataFrame(_job_covariate_rows).sort_values("aic").reset_index(drop=True)
    )
    job_covariate_screen_comparison["delta_aic_vs_best"] = (
        job_covariate_screen_comparison["aic"]
        - job_covariate_screen_comparison["aic"].iloc[0]
    ).round(2)
    job_covariate_screen_comparison["delta_aic_vs_no_jobs"] = (
        job_covariate_screen_comparison["aic"]
        - job_covariate_screen_comparison.loc[
            job_covariate_screen_comparison["model"] == "no_jobs",
            "aic",
        ].iloc[0]
    ).round(2)
    job_covariate_screen_comparison = job_covariate_screen_comparison.round(
        {
            "final_log_likelihood": 4,
            "aic": 4,
            "bic": 4,
            "job_coef": 4,
            "max_abs_feature_correlation": 3,
            "max_vif": 3,
        }
    )
    job_covariate_positive_coefficients = job_covariate_screen_comparison.loc[
        lambda df: df["job_feature"].ne("none") & df["job_coef"].gt(0)
    ].reset_index(drop=True)

    best_job_feature_col = str(job_covariate_screen_comparison.iloc[0]["job_feature"])
    best_job_screen_model_label = "best_" + best_job_feature_col.removeprefix(
        "jobs_"
    ).removesuffix("_2025")

    job_covariate_screen_comparison
    return (
        best_job_feature_col,
        best_job_screen_model_label,
        job_covariate_control_cols,
        job_covariate_features,
        job_covariate_positive_coefficients,
        job_covariate_screen_comparison,
        job_covariate_skipped_cols,
    )


@app.cell
def _(job_covariate_screen_comparison):

    job_covariate_screen_display = job_covariate_screen_comparison.loc[
        :,
        [
            "model",
            "sector",
            "minutes",
            "aic",
            "delta_aic_vs_no_jobs",
            "job_coef",
            "max_abs_feature_correlation",
            "max_vif",
            "screen_converged",
        ],
    ].copy()
    job_covariate_screen_display
    return


@app.cell
def _(job_covariate_screen_comparison):

    job_covariate_aic_plot_data = (
        job_covariate_screen_comparison.loc[lambda df: df["model"].ne("no_jobs")]
        .assign(aic_improvement_vs_no_jobs=lambda df: -df["delta_aic_vs_no_jobs"])
        .sort_values("aic_improvement_vs_no_jobs", ascending=True)
    )
    job_covariate_aic_plot = job_covariate_aic_plot_data.plot.barh(
        x="model",
        y="aic_improvement_vs_no_jobs",
        legend=False,
        figsize=(8, 7),
    )
    job_covariate_aic_plot.set_xlabel("AIC improvement vs no-job model")
    job_covariate_aic_plot.set_ylabel("")
    job_covariate_aic_plot.set_title("Job covariate screen")
    job_covariate_aic_plot
    return


@app.cell
def _(job_covariate_screen_comparison):

    job_covariate_coefficient_plot_data = job_covariate_screen_comparison.loc[
        lambda df: df["model"].ne("no_jobs")
    ].sort_values("job_coef", ascending=True)
    job_covariate_coefficient_plot = job_covariate_coefficient_plot_data.plot.barh(
        x="model",
        y="job_coef",
        legend=False,
        figsize=(8, 7),
    )
    job_covariate_coefficient_plot.axvline(0, color="black", linewidth=0.8)
    job_covariate_coefficient_plot.set_xlabel("Screened job coefficient")
    job_covariate_coefficient_plot.set_ylabel("")
    job_covariate_coefficient_plot.set_title("Job coefficient signs")
    job_covariate_coefficient_plot
    return


@app.cell
def _(job_covariate_screen_comparison):

    job_covariate_diagnostics_summary = (
        job_covariate_screen_comparison.loc[
            :,
            [
                "model",
                "max_abs_feature_correlation",
                "max_vif",
                "delta_aic_vs_best",
            ],
        ]
        .sort_values("max_vif", ascending=False)
        .reset_index(drop=True)
    )
    job_covariate_diagnostics_summary
    return


@app.cell
def _(
    best_job_feature_col,
    job_covariate_positive_coefficients,
    job_covariate_screen_comparison,
    job_covariate_skipped_cols,
):

    job_covariate_sign_summary = pd.DataFrame(
        [
            {
                "screened_job_covariates": int(
                    job_covariate_screen_comparison["job_feature"].ne("none").sum()
                ),
                "positive_job_coefficients": len(job_covariate_positive_coefficients),
                "skipped_zero_variance_covariates": ", ".join(
                    job_covariate_skipped_cols
                ),
                "selected_best_job_covariate": best_job_feature_col,
            }
        ]
    )
    job_covariate_sign_summary
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Selected model

    The best job covariate from the screen replaces the all-job accessibility feature in the final Biogeme specification.
    """)
    return


@app.cell
def _(
    best_job_feature_col,
    built_area_cols,
    df_transactions,
    job_covariate_control_cols,
    job_covariate_features,
):
    best_static_feature_cols = [
        best_job_feature_col,
        *job_covariate_control_cols,
    ]
    best_model_feature_cols = [*best_static_feature_cols, "log_built_area_ha"]

    best_neighborhood_features = job_covariate_features.loc[
        :, ["name_detail", *best_static_feature_cols, *built_area_cols]
    ].copy()
    best_neighborhood_names = best_neighborhood_features["name_detail"]
    best_neighborhood_features = best_neighborhood_features.loc[
        :, [*best_static_feature_cols, *built_area_cols]
    ]

    _best_diagnostics_frame = build_feature_diagnostics_frame(
        best_neighborhood_features,
        df_transactions,
        best_static_feature_cols,
        built_area_cols,
    )
    (
        best_feature_diagnostics,
        best_feature_correlation,
        best_feature_vif,
        best_max_abs_feature_correlation,
    ) = compute_feature_diagnostics(_best_diagnostics_frame)
    return (
        best_feature_vif,
        best_max_abs_feature_correlation,
        best_model_feature_cols,
        best_neighborhood_features,
        best_neighborhood_names,
        best_static_feature_cols,
    )


@app.cell
def _(
    best_model_feature_cols,
    best_neighborhood_features,
    best_static_feature_cols,
    built_area_cols,
    df_transactions,
):
    best_transactions_augmented = build_choice_frame(
        df_transactions,
        best_neighborhood_features,
        best_static_feature_cols,
        best_model_feature_cols,
        built_area_cols,
    )
    return (best_transactions_augmented,)


@app.cell
def _(
    best_model_feature_cols,
    best_neighborhood_features,
    best_transactions_augmented,
):
    best_model_artifacts = fit_biogeme_model(
        "test_1_best_manufacture_20",
        "housing_choice_best_manufacture_20",
        best_transactions_augmented,
        best_neighborhood_features,
        best_model_feature_cols,
        beta_name_prefix="best_",
        generate_html=False,
    )
    best_results = best_model_artifacts["results"]
    best_estimated_parameters = get_pandas_estimated_parameters(
        estimation_results=best_results
    )
    return best_estimated_parameters, best_results


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Diagnostics and reporting

    These tables and plots compare model fit, coefficient estimates, feature diagnostics, and observed versus predicted choice shares.
    """)
    return


@app.cell
def _(
    best_estimated_parameters,
    best_feature_vif,
    best_job_screen_model_label,
    best_max_abs_feature_correlation,
    best_results,
    feature_vif,
    max_abs_feature_correlation,
    results,
):

    best_model_comparison = pd.DataFrame(
        [
            {
                "model": "current_all_jobs_20min",
                "parameters": results.number_of_parameters,
                "sample_size": results.sample_size,
                "final_log_likelihood": results.final_log_likelihood,
                "aic": results.akaike_information_criterion,
                "bic": results.bayesian_information_criterion,
                "max_abs_feature_correlation": max_abs_feature_correlation,
                "max_vif": feature_vif["vif"].max(),
            },
            {
                "model": best_job_screen_model_label,
                "parameters": best_results.number_of_parameters,
                "sample_size": best_results.sample_size,
                "final_log_likelihood": best_results.final_log_likelihood,
                "aic": best_results.akaike_information_criterion,
                "bic": best_results.bayesian_information_criterion,
                "max_abs_feature_correlation": best_max_abs_feature_correlation,
                "max_vif": best_feature_vif["vif"].max(),
            },
        ]
    ).round(
        {
            "final_log_likelihood": 2,
            "aic": 2,
            "bic": 2,
            "max_abs_feature_correlation": 3,
            "max_vif": 3,
        }
    )
    best_model_comparison["delta_aic_vs_current_all_jobs_20min"] = (
        best_model_comparison["aic"]
        - best_model_comparison.loc[
            best_model_comparison["model"] == "current_all_jobs_20min",
            "aic",
        ].iloc[0]
    ).round(2)

    best_coefficient_summary = (
        best_estimated_parameters.assign(
            feature=lambda df: df["Name"].str.replace("best_beta_", "", regex=False),
            value=lambda df: df["Value"].round(3),
            robust_se=lambda df: df["Robust std err."].round(3),
            robust_t=lambda df: df["Robust t-stat."].round(3),
            robust_p=lambda df: df["Robust p-value"].round(4),
        )
        .loc[:, ["feature", "value", "robust_se", "robust_t", "robust_p"]]
        .sort_values("feature")
    )

    best_model_comparison, best_coefficient_summary, best_feature_vif
    return


@app.cell
def _(best_estimated_parameters):
    best_coefficient_plot_data = best_estimated_parameters.assign(
        feature=lambda df: df["Name"].str.replace("best_beta_", "", regex=False),
    ).sort_values("Value")

    best_coefficient_plot = best_coefficient_plot_data.plot.barh(
        x="feature",
        y="Value",
        xerr="Robust std err.",
        legend=False,
        figsize=(8, 4),
    )
    best_coefficient_plot.axvline(0, color="black", linewidth=0.8)
    best_coefficient_plot.set_xlabel("Coefficient estimate")
    best_coefficient_plot.set_ylabel("")
    best_coefficient_plot.set_title("Best model coefficients")
    best_coefficient_plot
    return


@app.cell
def _(
    best_estimated_parameters,
    best_model_feature_cols,
    best_neighborhood_features,
    best_neighborhood_names,
    best_transactions_augmented,
    df_transactions,
):
    _best_beta_by_feature = (
        best_estimated_parameters.set_index("Name")["Value"]
        .rename(index=lambda name: name.replace("best_beta_", ""))
        .to_dict()
    )

    _best_utilities = pd.DataFrame(index=best_transactions_augmented.index)
    for _idx in best_neighborhood_features.index:
        _utility = 0
        for _feature in best_model_feature_cols:
            _utility = _utility + (
                _best_beta_by_feature[_feature]
                * best_transactions_augmented[f"{_feature}_{_idx}"]
            )
        _best_utilities[_idx] = _utility

    _best_utilities = _best_utilities.sub(_best_utilities.max(axis=1), axis=0)
    _best_probabilities = np.exp(_best_utilities)
    _best_probabilities = _best_probabilities.div(
        _best_probabilities.sum(axis=1), axis=0
    )

    best_choice_share_summary = (
        pd.DataFrame(
            {
                "neighborhood": best_neighborhood_names,
                "observed_share": df_transactions["neighborhood"].value_counts(
                    normalize=True
                ),
                "predicted_share": _best_probabilities.mean(axis=0),
            }
        )
        .fillna(0)
        .assign(
            share_error=lambda df: df["predicted_share"] - df["observed_share"],
            abs_share_error=lambda df: df["share_error"].abs(),
        )
        .sort_values("observed_share", ascending=False)
    )

    best_choice_share_summary.head(15)
    return (best_choice_share_summary,)


@app.cell
def _(best_choice_share_summary):
    best_choice_share_plot = (
        best_choice_share_summary.head(15)
        .sort_values("observed_share")
        .set_index("neighborhood")[["observed_share", "predicted_share"]]
        .plot.barh(figsize=(8, 6))
    )
    best_choice_share_plot.set_xlabel("Share of transactions")
    best_choice_share_plot.set_ylabel("")
    best_choice_share_plot.set_title("Observed vs predicted shares, top neighborhoods")
    best_choice_share_plot
    return


if __name__ == "__main__":
    app.run()
