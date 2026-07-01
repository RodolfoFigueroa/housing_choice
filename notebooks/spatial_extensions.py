import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import os
    import warnings
    from pathlib import Path

    import marimo as mo
    import numpy as np
    import pandas as pd

    from housing_choice.modeling import (
        DEFAULT_BASELINE_STATIC_COLS,
        DEFAULT_SUPPLY_FEATURE,
        add_centroid_grid_features,
        add_centroid_quadratic_features,
        add_job_group_features,
        build_job_group_specs,
        build_structural_baseline_inputs,
        compute_scale_audit,
        fit_biogeme_availability_model,
        fit_fast_availability_mnl_screen,
        predict_availability_choice_shares,
    )

    warnings.filterwarnings("ignore", category=FutureWarning)


@app.cell(hide_code=True)
def title():
    mo.md("""
    # Spatial Extensions for the Structural Baseline

    This notebook tests whether the positive industrial job-accessibility signal survives stronger spatial controls. It keeps the same active choice set, supply proxy, and built-area term as the structural baseline.
    """)
    return


@app.cell
def constants():
    DATA_PATH = Path(os.environ["DATA_PATH"])
    MODELING_YEAR_MIN = 2020
    MODELING_YEAR_MAX = 2025
    ACTIVE_WINDOW_DAYS = 365
    MIN_AVAILABLE_ALTERNATIVES = 2
    MISSING_VALUE_SENTINEL = 99999
    BIOGEME_MODEL_PREFIX = "spatial_extensions"
    BASELINE_SPEC_ID = "structural_baseline"
    MIN_JOB_AIC_IMPROVEMENT = 2.0
    MIN_JOB_ROBUST_P_THRESHOLD = 0.10

    BASELINE_STATIC_COLS = list(DEFAULT_BASELINE_STATIC_COLS)
    BASELINE_NON_SPATIAL_COLS = [
        column
        for column in BASELINE_STATIC_COLS
        if column not in {"centroid_east_km", "centroid_north_km"}
    ]
    SUPPLY_FEATURE = DEFAULT_SUPPLY_FEATURE
    INDUSTRIAL_JOB_FEATURES = {
        "industrial_10": "jobs_group_industrial_10_2025_scaled",
        "industrial_20": "jobs_group_industrial_20_2025_scaled",
    }
    return (
        ACTIVE_WINDOW_DAYS,
        BASELINE_NON_SPATIAL_COLS,
        BASELINE_SPEC_ID,
        BASELINE_STATIC_COLS,
        BIOGEME_MODEL_PREFIX,
        DATA_PATH,
        INDUSTRIAL_JOB_FEATURES,
        MIN_AVAILABLE_ALTERNATIVES,
        MIN_JOB_AIC_IMPROVEMENT,
        MIN_JOB_ROBUST_P_THRESHOLD,
        MISSING_VALUE_SENTINEL,
        MODELING_YEAR_MAX,
        MODELING_YEAR_MIN,
        SUPPLY_FEATURE,
    )


@app.cell(hide_code=True)
def contract(
    ACTIVE_WINDOW_DAYS,
    MODELING_YEAR_MAX,
    MODELING_YEAR_MIN,
    SUPPLY_FEATURE,
):
    mo.md(f"""
    ## Modeling Contract

    All models use purchases from `{MODELING_YEAR_MIN}` through `{MODELING_YEAR_MAX}`, the same `{ACTIVE_WINDOW_DAYS}`-day active choice set, transaction-year built area, and `{SUPPLY_FEATURE}`. Spatial variants change only the static spatial controls. Job variants add exactly one industrial grouped job-accessibility feature.
    """)
    return


@app.cell
def baseline_inputs(
    ACTIVE_WINDOW_DAYS,
    BASELINE_STATIC_COLS,
    DATA_PATH,
    MIN_AVAILABLE_ALTERNATIVES,
    MODELING_YEAR_MAX,
    MODELING_YEAR_MIN,
    SUPPLY_FEATURE,
):
    baseline_inputs = build_structural_baseline_inputs(
        DATA_PATH,
        min_year=MODELING_YEAR_MIN,
        max_year=MODELING_YEAR_MAX,
        window_days=ACTIVE_WINDOW_DAYS,
        min_available_alternatives=MIN_AVAILABLE_ALTERNATIVES,
        baseline_static_cols=BASELINE_STATIC_COLS,
        supply_feature=SUPPLY_FEATURE,
    )
    choice_neighborhood_features = baseline_inputs.choice_neighborhood_features
    active_choice_set = baseline_inputs.active_choice_set
    built_area_cols = baseline_inputs.built_area_cols
    dynamic_alt_features = dict(baseline_inputs.dynamic_alt_features)
    choice_set_summary = baseline_inputs.choice_set_summary.round(3)

    input_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhood_features",
                "path": str(baseline_inputs.neighborhood_features_path),
                "rows": len(baseline_inputs.df_neighborhood_raw),
                "columns": len(baseline_inputs.df_neighborhood_raw.columns),
            },
            {
                "artifact": "transactions",
                "path": str(baseline_inputs.transactions_path),
                "rows": len(baseline_inputs.df_transactions_raw),
                "columns": len(baseline_inputs.df_transactions_raw.columns),
            },
        ],
    )

    mo.vstack([input_summary, choice_set_summary])
    return (
        active_choice_set,
        built_area_cols,
        choice_neighborhood_features,
        dynamic_alt_features,
    )


@app.cell(hide_code=True)
def features_md():
    mo.md("""
    ## Spatial and Job Features

    The compact spatial test compares the current linear centroid controls against quadratic centroid terms and a coarse 3x3 centroid grid. The grid uses `central_central` as the omitted reference zone.
    """)
    return


@app.cell
def feature_build(INDUSTRIAL_JOB_FEATURES, choice_neighborhood_features):
    job_group_specs = build_job_group_specs((10, 20))
    (
        choice_neighborhood_features_jobs,
        job_group_catalog,
    ) = add_job_group_features(choice_neighborhood_features, job_group_specs)
    choice_neighborhood_features_quadratic = add_centroid_quadratic_features(
        choice_neighborhood_features_jobs,
    )
    (
        choice_neighborhood_features_spatial,
        grid_zone_catalog,
    ) = add_centroid_grid_features(choice_neighborhood_features_quadratic)

    quadratic_spatial_cols = [
        "centroid_east_km_sq",
        "centroid_north_km_sq",
        "centroid_east_x_north_km2",
    ]
    grid_spatial_cols = grid_zone_catalog.loc[
        ~grid_zone_catalog["is_reference"],
        "model_column",
    ].tolist()
    spatial_feature_cols = [*quadratic_spatial_cols, *grid_spatial_cols]
    job_group_display = job_group_catalog.assign(
        source_columns=job_group_catalog["source_columns"].str.join(", "),
    )

    mo.vstack(
        [
            mo.md("### Industrial Job Features"),
            job_group_display.loc[
                job_group_display["model_column"].isin(
                    INDUSTRIAL_JOB_FEATURES.values(),
                ),
                ["group_id", "model_column", "source_columns", "description"],
            ],
            mo.md("### Grid Zones"),
            grid_zone_catalog,
        ],
    )
    return (
        choice_neighborhood_features_spatial,
        grid_spatial_cols,
        quadratic_spatial_cols,
        spatial_feature_cols,
    )


@app.cell
def audit(choice_neighborhood_features_spatial, spatial_feature_cols):
    spatial_scale_audit = compute_scale_audit(
        choice_neighborhood_features_spatial,
        spatial_feature_cols,
    )
    spatial_correlation = (
        choice_neighborhood_features_spatial.loc[
            :,
            ["centroid_east_km", "centroid_north_km", *spatial_feature_cols],
        ]
        .corr()
        .round(3)
    )

    mo.vstack(
        [
            mo.md("### Spatial Feature Scale Audit"),
            spatial_scale_audit,
            mo.md("### Spatial Feature Correlations"),
            spatial_correlation,
        ],
    )
    return


@app.cell(hide_code=True)
def specs_md():
    mo.md("""
    ## Model Specifications

    The comparison keeps the specification grid deliberately small: three spatial baselines and the two industrial job extensions under each spatial baseline.
    """)
    return


@app.cell
def model_specs(
    BASELINE_NON_SPATIAL_COLS,
    BASELINE_SPEC_ID,
    BASELINE_STATIC_COLS,
    INDUSTRIAL_JOB_FEATURES,
    grid_spatial_cols,
    quadratic_spatial_cols,
):
    model_spec_rows = [
        {
            "spec_id": BASELINE_SPEC_ID,
            "spatial_spec": "baseline_linear",
            "spec_kind": "baseline",
            "candidate_feature": "",
            "comparison_baseline_spec_id": "",
            "description": "Current structural baseline with linear centroid controls",
            "static_cols": BASELINE_STATIC_COLS,
        },
        {
            "spec_id": "baseline_linear__industrial_10",
            "spatial_spec": "baseline_linear",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_10"],
            "comparison_baseline_spec_id": BASELINE_SPEC_ID,
            "description": "Linear centroid baseline plus 10-minute industrial jobs",
            "static_cols": [
                *BASELINE_STATIC_COLS,
                INDUSTRIAL_JOB_FEATURES["industrial_10"],
            ],
        },
        {
            "spec_id": "baseline_linear__industrial_20",
            "spatial_spec": "baseline_linear",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_20"],
            "comparison_baseline_spec_id": BASELINE_SPEC_ID,
            "description": "Linear centroid baseline plus 20-minute industrial jobs",
            "static_cols": [
                *BASELINE_STATIC_COLS,
                INDUSTRIAL_JOB_FEATURES["industrial_20"],
            ],
        },
        {
            "spec_id": "spatial_quadratic",
            "spatial_spec": "spatial_quadratic",
            "spec_kind": "spatial_baseline",
            "candidate_feature": "",
            "comparison_baseline_spec_id": "",
            "description": "Linear centroid baseline plus quadratic centroid terms",
            "static_cols": [*BASELINE_STATIC_COLS, *quadratic_spatial_cols],
        },
        {
            "spec_id": "spatial_quadratic__industrial_10",
            "spatial_spec": "spatial_quadratic",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_10"],
            "comparison_baseline_spec_id": "spatial_quadratic",
            "description": "Quadratic spatial baseline plus 10-minute industrial jobs",
            "static_cols": [
                *BASELINE_STATIC_COLS,
                *quadratic_spatial_cols,
                INDUSTRIAL_JOB_FEATURES["industrial_10"],
            ],
        },
        {
            "spec_id": "spatial_quadratic__industrial_20",
            "spatial_spec": "spatial_quadratic",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_20"],
            "comparison_baseline_spec_id": "spatial_quadratic",
            "description": "Quadratic spatial baseline plus 20-minute industrial jobs",
            "static_cols": [
                *BASELINE_STATIC_COLS,
                *quadratic_spatial_cols,
                INDUSTRIAL_JOB_FEATURES["industrial_20"],
            ],
        },
        {
            "spec_id": "spatial_grid_3x3",
            "spatial_spec": "spatial_grid_3x3",
            "spec_kind": "spatial_baseline",
            "candidate_feature": "",
            "comparison_baseline_spec_id": "",
            "description": "Non-spatial baseline controls plus 3x3 centroid grid dummies",
            "static_cols": [*BASELINE_NON_SPATIAL_COLS, *grid_spatial_cols],
        },
        {
            "spec_id": "spatial_grid_3x3__industrial_10",
            "spatial_spec": "spatial_grid_3x3",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_10"],
            "comparison_baseline_spec_id": "spatial_grid_3x3",
            "description": "3x3 grid spatial baseline plus 10-minute industrial jobs",
            "static_cols": [
                *BASELINE_NON_SPATIAL_COLS,
                *grid_spatial_cols,
                INDUSTRIAL_JOB_FEATURES["industrial_10"],
            ],
        },
        {
            "spec_id": "spatial_grid_3x3__industrial_20",
            "spatial_spec": "spatial_grid_3x3",
            "spec_kind": "job_extension",
            "candidate_feature": INDUSTRIAL_JOB_FEATURES["industrial_20"],
            "comparison_baseline_spec_id": "spatial_grid_3x3",
            "description": "3x3 grid spatial baseline plus 20-minute industrial jobs",
            "static_cols": [
                *BASELINE_NON_SPATIAL_COLS,
                *grid_spatial_cols,
                INDUSTRIAL_JOB_FEATURES["industrial_20"],
            ],
        },
    ]
    model_specs = pd.DataFrame(model_spec_rows)
    model_specs_display = model_specs.assign(
        parameters_before_dynamic_terms=model_specs["static_cols"].map(len),
    )
    model_specs_display.loc[
        :,
        [
            "spec_id",
            "spatial_spec",
            "spec_kind",
            "candidate_feature",
            "parameters_before_dynamic_terms",
            "description",
        ],
    ]
    return (model_specs,)


@app.cell(hide_code=True)
def screen_md():
    mo.md("""
    ## Fast Screen

    The fast screen estimates the same availability-aware likelihood with SciPy. It is used here as a quick diagnostic before the full Biogeme comparison.
    """)
    return


@app.cell
def fast_screen(
    BASELINE_SPEC_ID,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features_spatial,
    dynamic_alt_features,
    model_specs,
):
    fast_screen_rows = []
    fast_screen_coefficients = []
    for _screen_spec in model_specs.itertuples(index=False):
        screen_row, coefficient_frame = fit_fast_availability_mnl_screen(
            _screen_spec.spec_id,
            list(_screen_spec.static_cols),
            choice_neighborhood_features_spatial,
            active_choice_set.transactions,
            built_area_cols,
            active_choice_set.availability,
            dynamic_alt_features=dynamic_alt_features,
            missing_value_sentinel=MISSING_VALUE_SENTINEL,
        )
        fast_screen_rows.append(screen_row)
        fast_screen_coefficients.append(coefficient_frame)

    fast_screen_coefficients = pd.concat(fast_screen_coefficients, ignore_index=True)
    fast_screen_table = pd.DataFrame(fast_screen_rows).merge(
        model_specs.drop(columns=["static_cols"]),
        on="spec_id",
        how="left",
    )
    _fast_structural_aic = fast_screen_table.loc[
        fast_screen_table["spec_id"].eq(BASELINE_SPEC_ID),
        "aic",
    ].iloc[0]
    _fast_null_log_likelihood = -float(
        np.log(active_choice_set.availability.sum(axis=1)).sum(),
    )
    if "null_log_likelihood" not in fast_screen_table.columns:
        fast_screen_table = fast_screen_table.assign(
            null_log_likelihood=_fast_null_log_likelihood,
        )
    if "mcfadden_r_squared" not in fast_screen_table.columns:
        fast_screen_table = fast_screen_table.assign(
            mcfadden_r_squared=lambda df: (
                1 - df["final_log_likelihood"] / df["null_log_likelihood"]
            ),
        )
    fast_screen_table = (
        fast_screen_table.assign(
            delta_aic_vs_structural_baseline=lambda df: (
                df["aic"] - _fast_structural_aic
            ),
        )
        .sort_values(["aic", "bic"])
        .round(
            {
                "final_log_likelihood": 3,
                "null_log_likelihood": 3,
                "mcfadden_r_squared": 4,
                "aic": 3,
                "bic": 3,
                "delta_aic_vs_structural_baseline": 3,
            },
        )
    )
    fast_screen_table.loc[
        :,
        [
            "spec_id",
            "spatial_spec",
            "candidate_feature",
            "parameters",
            "aic",
            "mcfadden_r_squared",
            "delta_aic_vs_structural_baseline",
            "screen_converged",
        ],
    ]
    return (fast_screen_table,)


@app.cell(hide_code=True)
def biogeme_md():
    mo.md("""
    ## Biogeme Decision Set

    The fast screen ranks all nine compact specifications, including the 3x3 grid. Biogeme is kept to a practical four-model decision set: the current structural baseline, its best screened industrial extension, the quadratic spatial baseline, and its best screened industrial extension. The grid remains a fast-screen diagnostic because its Biogeme fits were too slow for this notebook.
    """)
    return


@app.cell
def biogeme_fit(
    BASELINE_SPEC_ID,
    BIOGEME_MODEL_PREFIX,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features_spatial,
    dynamic_alt_features,
    fast_screen_table,
    model_specs,
):
    _existing_biogeme_artifacts = globals().get("biogeme_artifacts", {})
    if not isinstance(_existing_biogeme_artifacts, dict):
        _existing_biogeme_artifacts = {}

    _biogeme_finalist_ids = [BASELINE_SPEC_ID]
    for _spatial_spec, _baseline_spec_id in {
        "baseline_linear": BASELINE_SPEC_ID,
        "spatial_quadratic": "spatial_quadratic",
    }.items():
        if _baseline_spec_id not in _biogeme_finalist_ids:
            _biogeme_finalist_ids.append(_baseline_spec_id)
        _candidate_mask = fast_screen_table["spatial_spec"].eq(
            _spatial_spec,
        ) & fast_screen_table["spec_kind"].eq("job_extension")
        _best_job_spec_id = (
            fast_screen_table.loc[_candidate_mask]
            .sort_values(["aic", "bic"])["spec_id"]
            .iloc[0]
        )
        _biogeme_finalist_ids.append(_best_job_spec_id)

    biogeme_finalist_specs = model_specs.loc[
        model_specs["spec_id"].isin(_biogeme_finalist_ids)
    ].copy()
    biogeme_finalist_specs["biogeme_fit_order"] = biogeme_finalist_specs["spec_id"].map(
        {spec_id: order for order, spec_id in enumerate(_biogeme_finalist_ids)},
    )
    biogeme_finalist_specs = biogeme_finalist_specs.sort_values("biogeme_fit_order")

    biogeme_artifacts = {}
    for _biogeme_spec in biogeme_finalist_specs.itertuples(index=False):
        if _biogeme_spec.spec_id in _existing_biogeme_artifacts:
            biogeme_artifacts[_biogeme_spec.spec_id] = _existing_biogeme_artifacts[
                _biogeme_spec.spec_id
            ]
        else:
            biogeme_artifacts[_biogeme_spec.spec_id] = fit_biogeme_availability_model(
                _biogeme_spec.spec_id,
                list(_biogeme_spec.static_cols),
                choice_neighborhood_features_spatial,
                active_choice_set.transactions,
                built_area_cols,
                active_choice_set.availability,
                dynamic_alt_features=dynamic_alt_features,
                model_prefix=BIOGEME_MODEL_PREFIX,
                missing_value_sentinel=MISSING_VALUE_SENTINEL,
                use_jit=False,
            )

    biogeme_error_table = pd.DataFrame()
    biogeme_model_summary = pd.DataFrame(
        [artifact["summary_row"] for artifact in biogeme_artifacts.values()],
    ).merge(
        biogeme_finalist_specs.drop(columns=["static_cols"]),
        on="spec_id",
        how="left",
    )
    _biogeme_structural_aic = biogeme_model_summary.loc[
        biogeme_model_summary["spec_id"].eq(BASELINE_SPEC_ID),
        "aic",
    ].iloc[0]
    _biogeme_null_log_likelihood = -float(
        np.log(active_choice_set.availability.sum(axis=1)).sum(),
    )
    if "null_log_likelihood" not in biogeme_model_summary.columns:
        biogeme_model_summary = biogeme_model_summary.assign(
            null_log_likelihood=_biogeme_null_log_likelihood,
        )
    if "mcfadden_r_squared" not in biogeme_model_summary.columns:
        biogeme_model_summary = biogeme_model_summary.assign(
            mcfadden_r_squared=lambda df: (
                1 - df["final_log_likelihood"] / df["null_log_likelihood"]
            ),
        )
    biogeme_model_summary = (
        biogeme_model_summary.assign(
            delta_aic_vs_structural_baseline=lambda df: (
                df["aic"] - _biogeme_structural_aic
            ),
        )
        .sort_values(["aic", "bic"])
        .round(
            {
                "final_log_likelihood": 3,
                "null_log_likelihood": 3,
                "mcfadden_r_squared": 4,
                "aic": 3,
                "bic": 3,
                "delta_aic_vs_structural_baseline": 3,
            },
        )
    )

    mo.vstack(
        [
            mo.md("### Biogeme Finalist Specs"),
            biogeme_finalist_specs.loc[
                :,
                ["spec_id", "spatial_spec", "spec_kind", "candidate_feature"],
            ],
            mo.md("### Biogeme Results"),
            biogeme_model_summary.loc[
                :,
                [
                    "spec_id",
                    "spatial_spec",
                    "candidate_feature",
                    "parameters",
                    "sample_size",
                    "final_log_likelihood",
                    "aic",
                    "mcfadden_r_squared",
                    "delta_aic_vs_structural_baseline",
                    "algorithm_has_converged",
                ],
            ],
        ],
    )
    return biogeme_artifacts, biogeme_model_summary


@app.cell(hide_code=True)
def survival_md():
    mo.md("""
    ## Industrial Job Survival

    Each fitted job extension is judged against its matched no-job spatial baseline. A candidate survives if it improves AIC by at least 2, keeps a positive job coefficient, and has robust p-value below 0.10.
    """)
    return


@app.cell
def job_survival(
    MIN_JOB_AIC_IMPROVEMENT,
    MIN_JOB_ROBUST_P_THRESHOLD,
    biogeme_artifacts,
    biogeme_model_summary,
    model_specs,
):
    biogeme_coefficient_summary = pd.concat(
        [
            artifact["estimated_parameters"].assign(spec_id=spec_id)
            for spec_id, artifact in biogeme_artifacts.items()
        ],
        ignore_index=True,
    ).assign(
        value=lambda df: df["Value"].round(4),
        robust_se=lambda df: df["Robust std err."].round(4),
        robust_t=lambda df: df["Robust t-stat."].round(3),
        robust_p=lambda df: df["Robust p-value"].round(4),
    )
    job_coefficient_summary = (
        biogeme_coefficient_summary.merge(
            model_specs.loc[
                :,
                [
                    "spec_id",
                    "spatial_spec",
                    "candidate_feature",
                    "comparison_baseline_spec_id",
                ],
            ],
            on="spec_id",
            how="left",
        )
        .loc[lambda df: df["candidate_feature"].ne("")]
        .loc[lambda df: df["feature"].eq(df["candidate_feature"])]
        .loc[
            :,
            [
                "spec_id",
                "spatial_spec",
                "candidate_feature",
                "comparison_baseline_spec_id",
                "value",
                "robust_se",
                "robust_t",
                "robust_p",
            ],
        ]
    )
    _aic_lookup = biogeme_model_summary.set_index("spec_id")["aic"]
    job_survival_table = (
        job_coefficient_summary.assign(
            model_aic=lambda df: df["spec_id"].map(_aic_lookup),
            comparison_baseline_aic=lambda df: df["comparison_baseline_spec_id"].map(
                _aic_lookup,
            ),
            aic_improvement=lambda df: df["comparison_baseline_aic"] - df["model_aic"],
            improves_aic=lambda df: df["aic_improvement"].ge(MIN_JOB_AIC_IMPROVEMENT),
            positive_job_coefficient=lambda df: df["value"].gt(0),
            robust_evidence=lambda df: df["robust_p"].lt(MIN_JOB_ROBUST_P_THRESHOLD),
            continue_candidate=lambda df: df[
                ["improves_aic", "positive_job_coefficient", "robust_evidence"]
            ].all(axis=1),
        )
        .round({"model_aic": 3, "comparison_baseline_aic": 3, "aic_improvement": 3})
        .sort_values(["spatial_spec", "candidate_feature"])
    )
    job_survival_table.loc[
        :,
        [
            "spec_id",
            "spatial_spec",
            "candidate_feature",
            "value",
            "robust_p",
            "aic_improvement",
            "improves_aic",
            "positive_job_coefficient",
            "robust_evidence",
            "continue_candidate",
        ],
    ]
    return (job_survival_table,)


@app.cell(hide_code=True)
def fit_md():
    mo.md("""
    ## Residual Share Fit

    The share-fit check summarizes how much each specification reduces observed-versus-predicted neighborhood share errors. It is a diagnostic, not the selection rule.
    """)
    return


@app.cell
def share_fit(
    biogeme_artifacts,
    choice_neighborhood_features_spatial,
    model_specs,
):
    choice_share_summaries = {
        spec_id: predict_availability_choice_shares(
            artifact,
            choice_neighborhood_features_spatial,
        )
        for spec_id, artifact in biogeme_artifacts.items()
    }
    share_fit_metrics = pd.DataFrame(
        [
            {
                "spec_id": spec_id,
                "mean_abs_share_error": share_frame["abs_share_error"].mean(),
                "rmse_share_error": float(
                    np.sqrt((share_frame["share_error"] ** 2).mean()),
                ),
                "max_abs_share_error": share_frame["abs_share_error"].max(),
            }
            for spec_id, share_frame in choice_share_summaries.items()
        ],
    ).merge(
        model_specs.drop(columns=["static_cols"]),
        on="spec_id",
        how="left",
    )
    share_fit_metrics = (
        share_fit_metrics.sort_values(["mean_abs_share_error", "rmse_share_error"])
        .round(4)
        .loc[
            :,
            [
                "spec_id",
                "spatial_spec",
                "candidate_feature",
                "mean_abs_share_error",
                "rmse_share_error",
                "max_abs_share_error",
            ],
        ]
    )
    share_fit_metrics
    return


@app.cell(hide_code=True)
def reading(biogeme_model_summary, job_survival_table):
    _best_model_row = biogeme_model_summary.sort_values("aic").iloc[0]
    _surviving_jobs = job_survival_table.loc[job_survival_table["continue_candidate"]]
    _survival_text = (
        ", ".join(_surviving_jobs["spec_id"].tolist())
        if len(_surviving_jobs)
        else "none"
    )
    mo.md(f"""
    ## Reading

    The lowest-AIC fitted Biogeme model is `{_best_model_row["spec_id"]}` with AIC `{_best_model_row["aic"]}`. Industrial job extensions that survive their matched fitted spatial baseline are: `{_survival_text}`. Grid specifications are retained in the fast screen as spatial-fragility diagnostics, but are not baseline-ready until a dedicated grid Biogeme run is completed.
    """)
    return


@app.cell
def _(biogeme_model_summary):
    biogeme_model_summary.sort_values("aic").iloc[0]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
