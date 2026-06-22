import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import os
    import warnings
    from pathlib import Path

    import marimo as mo
    import numpy as np
    import pandas as pd

    from housing_choice.modeling import (
        DEFAULT_BASELINE_STATIC_COLS,
        DEFAULT_SUPPLY_FEATURE,
        add_job_group_features,
        build_job_group_specs,
        build_structural_baseline_inputs,
        fit_biogeme_availability_model,
        fit_fast_availability_mnl_screen,
        predict_availability_choice_shares,
    )

    warnings.filterwarnings("ignore", category=FutureWarning)


@app.cell(hide_code=True)
def title():
    mo.md("""
    # Grouped Job-Accessibility Extensions

    This notebook tests whether compact, interpretable job-accessibility groups improve the structural baseline. It reuses the baseline choice set, supply/activity proxy, and static controls so each job extension is measured against the same baseline.
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
    BIOGEME_MODEL_PREFIX = "job_extensions"
    BASELINE_SPEC_ID = "structural_baseline"
    FINALIST_COUNT = 2
    MIN_JOB_AIC_IMPROVEMENT = 2.0
    MIN_JOB_ROBUST_P_THRESHOLD = 0.10

    BASELINE_STATIC_COLS = list(DEFAULT_BASELINE_STATIC_COLS)
    SUPPLY_FEATURE = DEFAULT_SUPPLY_FEATURE
    return (
        ACTIVE_WINDOW_DAYS,
        BASELINE_SPEC_ID,
        BASELINE_STATIC_COLS,
        BIOGEME_MODEL_PREFIX,
        DATA_PATH,
        FINALIST_COUNT,
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

    All models use purchases from `{MODELING_YEAR_MIN}` through `{MODELING_YEAR_MAX}`, the same `{ACTIVE_WINDOW_DAYS}`-day active choice set, the same transaction-year built-area term, and the same `{SUPPLY_FEATURE}` supply/activity proxy. The only difference between a job extension and the baseline is one grouped job-accessibility feature.
    """)
    return


@app.cell
def baseline_load(
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
def group_md():
    mo.md("""
    ## Grouped Job Features

    The extension candidates use four understandable job groups at 10 and 20 minutes: all jobs, industrial jobs, services jobs, and commerce jobs. Industrial and services are averages of their component accessibility measures, which avoids fitting separate highly collinear sector variables.
    """)
    return


@app.cell
def job_groups(choice_neighborhood_features):
    job_group_specs = build_job_group_specs((10, 20))
    (
        choice_neighborhood_features_with_jobs,
        job_group_catalog,
    ) = add_job_group_features(choice_neighborhood_features, job_group_specs)
    job_feature_cols = job_group_catalog["model_column"].tolist()
    job_group_display = job_group_catalog.assign(
        source_columns=job_group_catalog["source_columns"].str.join(", "),
    )
    job_group_display
    return (
        choice_neighborhood_features_with_jobs,
        job_feature_cols,
        job_group_specs,
    )


@app.cell
def job_audit(choice_neighborhood_features_with_jobs, job_feature_cols):
    job_group_summary = (
        choice_neighborhood_features_with_jobs.loc[:, job_feature_cols]
        .describe()
        .T.loc[:, ["mean", "std", "min", "25%", "50%", "75%", "max"]]
        .round(3)
    )
    job_group_correlation = (
        choice_neighborhood_features_with_jobs.loc[:, job_feature_cols].corr().round(3)
    )

    _upper_triangle = np.triu(np.ones(job_group_correlation.shape, dtype=bool), k=1)
    high_job_correlations = (
        job_group_correlation.where(_upper_triangle)
        .reset_index(names="feature_a")
        .melt(id_vars="feature_a", var_name="feature_b", value_name="correlation")
        .dropna(subset=["correlation"])
        .assign(correlation=lambda df: df["correlation"].abs())
        .sort_values("correlation", ascending=False)
        .head(12)
    )

    mo.vstack(
        [
            mo.md("### Scale Summary"),
            job_group_summary,
            mo.md("### Highest Absolute Correlations"),
            high_job_correlations,
        ],
    )
    return


@app.cell(hide_code=True)
def screen_md():
    mo.md("""
    ## Fast Screen

    The fast screen uses the same availability mask and dynamic supply term as the Biogeme model, but estimates the likelihood directly with SciPy. It is only a ranking device; finalist coefficients and fit statistics come from Biogeme.
    """)
    return


@app.cell
def screen_specs(BASELINE_SPEC_ID, BASELINE_STATIC_COLS, job_group_specs):
    screen_spec_rows = [
        {
            "spec_id": BASELINE_SPEC_ID,
            "spec_kind": "baseline",
            "candidate_feature": "",
            "horizon_minutes": np.nan,
            "description": "Structural baseline without job accessibility",
            "static_cols": BASELINE_STATIC_COLS,
        },
    ]
    screen_spec_rows.extend(
        [
            {
                "spec_id": f"job_group__{_spec.group_id}",
                "spec_kind": "job_group",
                "candidate_feature": _spec.model_column,
                "horizon_minutes": _spec.horizon_minutes,
                "description": _spec.description,
                "static_cols": [*BASELINE_STATIC_COLS, _spec.model_column],
            }
            for _spec in job_group_specs
        ],
    )

    screen_specs = pd.DataFrame(screen_spec_rows)
    screen_specs.loc[:, ["spec_id", "spec_kind", "candidate_feature", "description"]]
    return (screen_specs,)


@app.cell
def fast_screen(
    BASELINE_SPEC_ID,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features_with_jobs,
    dynamic_alt_features,
    screen_specs,
):
    _fast_rows = []
    _fast_coefficients = []
    for _spec in screen_specs.itertuples(index=False):
        _screen_row, _coefficient_frame = fit_fast_availability_mnl_screen(
            _spec.spec_id,
            list(_spec.static_cols),
            choice_neighborhood_features_with_jobs,
            active_choice_set.transactions,
            built_area_cols,
            active_choice_set.availability,
            dynamic_alt_features=dynamic_alt_features,
            missing_value_sentinel=MISSING_VALUE_SENTINEL,
        )
        _fast_rows.append(_screen_row)
        _fast_coefficients.append(_coefficient_frame)

    fast_screen_coefficients = pd.concat(_fast_coefficients, ignore_index=True)
    fast_screen_table = pd.DataFrame(_fast_rows).merge(
        screen_specs.drop(columns=["static_cols"]),
        on="spec_id",
        how="left",
    )
    _fast_baseline_aic = fast_screen_table.loc[
        fast_screen_table["spec_id"].eq(BASELINE_SPEC_ID),
        "aic",
    ].iloc[0]
    fast_screen_table = (
        fast_screen_table.assign(
            delta_aic_vs_screen_baseline=lambda df: df["aic"] - _fast_baseline_aic,
        )
        .sort_values(["aic", "bic"])
        .round(
            {
                "final_log_likelihood": 3,
                "aic": 3,
                "bic": 3,
                "delta_aic_vs_screen_baseline": 3,
            },
        )
    )
    fast_job_coefficients = (
        fast_screen_coefficients.merge(
            screen_specs.loc[:, ["spec_id", "candidate_feature", "description"]],
            on="spec_id",
            how="left",
        )
        .loc[lambda df: df["feature"].eq(df["candidate_feature"])]
        .assign(screen_coef=lambda df: df["screen_coef"].round(4))
        .loc[:, ["spec_id", "description", "candidate_feature", "screen_coef"]]
    )

    mo.vstack(
        [
            fast_screen_table.loc[
                :,
                [
                    "spec_id",
                    "spec_kind",
                    "candidate_feature",
                    "parameters",
                    "final_log_likelihood",
                    "aic",
                    "delta_aic_vs_screen_baseline",
                    "screen_converged",
                ],
            ],
            mo.md("### Screen Coefficients for Job Candidates"),
            fast_job_coefficients,
        ],
    )
    return (fast_screen_table,)


@app.cell
def finalists(
    BASELINE_SPEC_ID,
    FINALIST_COUNT,
    fast_screen_table,
    screen_specs,
):
    job_screen_rank = fast_screen_table.loc[
        lambda df: df["spec_id"].ne(BASELINE_SPEC_ID)
    ].sort_values(["aic", "bic"])
    finalist_spec_ids = [
        BASELINE_SPEC_ID,
        *job_screen_rank.head(FINALIST_COUNT)["spec_id"].tolist(),
    ]
    finalist_specs = screen_specs.loc[
        screen_specs["spec_id"].isin(finalist_spec_ids)
    ].copy()
    finalist_specs["finalist_order"] = finalist_specs["spec_id"].map(
        {spec_id: idx for idx, spec_id in enumerate(finalist_spec_ids)},
    )
    finalist_specs = finalist_specs.sort_values("finalist_order")
    finalist_specs.loc[:, ["spec_id", "spec_kind", "candidate_feature", "description"]]
    return (finalist_specs,)


@app.cell(hide_code=True)
def biogeme_md(FINALIST_COUNT):
    mo.md(f"""
    ## Biogeme Finalists

    The finalist set is the structural baseline plus the top `{FINALIST_COUNT}` non-baseline job extensions from the fast screen. These are the only models in this notebook treated as comparable final estimates.
    """)
    return


@app.cell
def biogeme_fit(
    BASELINE_SPEC_ID,
    BIOGEME_MODEL_PREFIX,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features_with_jobs,
    dynamic_alt_features,
    finalist_specs,
):
    biogeme_artifacts = {}
    for _spec in finalist_specs.itertuples(index=False):
        biogeme_artifacts[_spec.spec_id] = fit_biogeme_availability_model(
            _spec.spec_id,
            list(_spec.static_cols),
            choice_neighborhood_features_with_jobs,
            active_choice_set.transactions,
            built_area_cols,
            active_choice_set.availability,
            dynamic_alt_features=dynamic_alt_features,
            model_prefix=BIOGEME_MODEL_PREFIX,
            missing_value_sentinel=MISSING_VALUE_SENTINEL,
            use_jit=False,
        )

    biogeme_model_summary = pd.DataFrame(
        [artifact["summary_row"] for artifact in biogeme_artifacts.values()],
    ).merge(
        finalist_specs.drop(columns=["static_cols"]),
        on="spec_id",
        how="left",
    )
    _biogeme_baseline_aic = biogeme_model_summary.loc[
        biogeme_model_summary["spec_id"].eq(BASELINE_SPEC_ID),
        "aic",
    ].iloc[0]
    biogeme_model_summary = (
        biogeme_model_summary.assign(
            delta_aic_vs_baseline=lambda df: df["aic"] - _biogeme_baseline_aic,
        )
        .sort_values("finalist_order")
        .round(
            {
                "final_log_likelihood": 3,
                "aic": 3,
                "bic": 3,
                "delta_aic_vs_baseline": 3,
            },
        )
    )
    biogeme_model_summary.loc[
        :,
        [
            "spec_id",
            "candidate_feature",
            "parameters",
            "sample_size",
            "final_log_likelihood",
            "aic",
            "delta_aic_vs_baseline",
            "algorithm_has_converged",
        ],
    ]
    return biogeme_artifacts, biogeme_model_summary


@app.cell(hide_code=True)
def coef_md():
    mo.md("""
    ## Coefficients and Continuation Flags

    A grouped job extension is worth carrying forward only if it improves Biogeme AIC by at least 2, has a positive job coefficient, and has robust p-value below 0.10. These thresholds are a screen for model-building, not a final inferential claim.
    """)
    return


@app.cell
def coefficients(
    MIN_JOB_AIC_IMPROVEMENT,
    MIN_JOB_ROBUST_P_THRESHOLD,
    biogeme_artifacts,
    biogeme_model_summary,
    finalist_specs,
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
    job_finalist_coefficients = (
        biogeme_coefficient_summary.merge(
            finalist_specs.loc[:, ["spec_id", "candidate_feature", "description"]],
            on="spec_id",
            how="left",
        )
        .loc[lambda df: df["candidate_feature"].ne("")]
        .loc[lambda df: df["feature"].eq(df["candidate_feature"])]
        .loc[
            :,
            [
                "spec_id",
                "description",
                "candidate_feature",
                "value",
                "robust_se",
                "robust_t",
                "robust_p",
            ],
        ]
    )
    continuation_flags = (
        job_finalist_coefficients.merge(
            biogeme_model_summary.loc[:, ["spec_id", "delta_aic_vs_baseline"]],
            on="spec_id",
            how="left",
        )
        .assign(
            aic_improvement=lambda df: -df["delta_aic_vs_baseline"],
            improves_aic=lambda df: df["aic_improvement"].ge(MIN_JOB_AIC_IMPROVEMENT),
            positive_job_coefficient=lambda df: df["value"].gt(0),
            robust_evidence=lambda df: df["robust_p"].lt(MIN_JOB_ROBUST_P_THRESHOLD),
            continue_candidate=lambda df: df[
                ["improves_aic", "positive_job_coefficient", "robust_evidence"]
            ].all(axis=1),
        )
        .loc[
            :,
            [
                "spec_id",
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
        .round({"aic_improvement": 3})
    )

    mo.vstack(
        [
            mo.md("### Finalist Job Coefficients"),
            job_finalist_coefficients,
            mo.md("### Continuation Flags"),
            continuation_flags,
        ],
    )
    return (continuation_flags,)


@app.cell(hide_code=True)
def fit_md():
    mo.md("""
    ## Share-Fit Checks

    The residual check compares observed neighborhood transaction shares with predicted shares under each finalist model. This is a practical diagnostic for whether a job extension reduces the baseline's largest neighborhood-level misses.
    """)
    return


@app.cell
def share_fit(
    BASELINE_SPEC_ID,
    biogeme_artifacts,
    biogeme_model_summary,
    choice_neighborhood_features_with_jobs,
    finalist_specs,
):
    choice_share_summaries = {
        spec_id: predict_availability_choice_shares(
            artifact,
            choice_neighborhood_features_with_jobs,
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
        finalist_specs.loc[:, ["spec_id", "candidate_feature", "finalist_order"]],
        on="spec_id",
        how="left",
    )
    share_fit_metrics = share_fit_metrics.sort_values("finalist_order").round(4)

    best_job_spec_id = (
        biogeme_model_summary.loc[lambda df: df["spec_id"].ne(BASELINE_SPEC_ID)]
        .sort_values("aic")["spec_id"]
        .iloc[0]
    )
    _baseline_share_errors = (
        choice_share_summaries[BASELINE_SPEC_ID]
        .loc[
            :, ["neighborhood_idx", "neighborhood", "observed_share", "abs_share_error"]
        ]
        .rename(columns={"abs_share_error": "baseline_abs_share_error"})
    )
    _best_job_share_errors = (
        choice_share_summaries[best_job_spec_id]
        .loc[
            :, ["neighborhood_idx", "predicted_share", "share_error", "abs_share_error"]
        ]
        .rename(
            columns={
                "predicted_share": "best_job_predicted_share",
                "share_error": "best_job_share_error",
                "abs_share_error": "best_job_abs_share_error",
            },
        )
    )
    residual_delta_vs_baseline = (
        _baseline_share_errors.merge(_best_job_share_errors, on="neighborhood_idx")
        .assign(
            abs_error_reduction=lambda df: (
                df["baseline_abs_share_error"] - df["best_job_abs_share_error"]
            ),
        )
        .sort_values("abs_error_reduction", ascending=False)
        .head(15)
        .round(4)
    )

    mo.vstack(
        [
            mo.md("### Finalist Share-Fit Metrics"),
            share_fit_metrics.loc[
                :,
                [
                    "spec_id",
                    "candidate_feature",
                    "mean_abs_share_error",
                    "rmse_share_error",
                    "max_abs_share_error",
                ],
            ],
            mo.md(f"### Largest Residual Improvements for `{best_job_spec_id}`"),
            residual_delta_vs_baseline,
        ],
    )
    return


@app.cell(hide_code=True)
def reading(biogeme_model_summary, continuation_flags):
    _best_row = biogeme_model_summary.sort_values("aic").iloc[0]
    _continue_count = int(continuation_flags["continue_candidate"].sum())
    mo.md(f"""
    ## Reading

    The lowest-AIC finalist is `{_best_row["spec_id"]}` with Biogeme AIC `{_best_row["aic"]}`. `{_continue_count}` grouped job extension(s) satisfy the continuation flags. A candidate that fails these flags can still be useful diagnostically, but it should not become the next baseline without a clearer spatial or supply explanation.
    """)
    return


if __name__ == "__main__":
    app.run()
