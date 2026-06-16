import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import os
    import warnings
    from pathlib import Path

    import geopandas as gpd
    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns

    from housing_choice.modeling import (
        align_choice_data,
        build_feature_catalog,
        build_feature_diagnostics_frame,
        build_single_candidate_model_specs,
        compute_feature_diagnostics,
        compute_scale_audit,
        fit_biogeme_model,
        fit_fast_mnl_screen,
        predict_choice_shares,
        prepare_neighborhood_features,
        prepare_transactions,
        run_derivative_check,
    )

    warnings.filterwarnings("ignore", category=FutureWarning)


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Housing Choice Modelling

    Clean modelling workflow for neighborhood choice models. The notebook consumes the canonical neighborhood feature export from `09_generate_neighborhood_features.py` and keeps exploratory screening separate from final Biogeme estimation.
    """)
    return


@app.cell
def _():
    GENERATED_PATH = Path(os.environ["DATA_PATH"]) / "generated"
    NEIGHBORHOOD_FEATURES_PATH = Path(GENERATED_PATH / "col_final.gpkg")
    TRANSACTIONS_PATH = Path(GENERATED_PATH / "transactions_final.parquet")

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
def _():
    mo.md("""
    ## Setup Helpers

    Reusable modelling machinery now lives in `housing_choice.modeling`: feature cataloging, scaling, diagnostics, choice-frame construction, fast screening, guarded Biogeme estimation, and prediction summaries.
    """)
    return


@app.cell
def _():
    modeling_helpers_summary = pd.DataFrame(
        [
            {
                "module": "features",
                "responsibility": "feature catalog, scaling, prepared feature frame",
            },
            {
                "module": "choice_data",
                "responsibility": "transaction filter, choice set, choice frame validation",
            },
            {
                "module": "diagnostics",
                "responsibility": "correlations, VIF, selected-spec diagnostics",
            },
            {
                "module": "specs",
                "responsibility": "baseline and one-candidate specification tables",
            },
            {
                "module": "estimation",
                "responsibility": "fast MNL screen, Biogeme fit, prediction summaries",
            },
        ]
    )
    modeling_helpers_summary
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Data Inputs

    Load the canonical processed inputs. The neighborhood table is produced by `09_generate_neighborhood_features.py`; the transaction table is filtered to the same neighborhood naming convention.
    """)
    return


@app.cell
def _(NEIGHBORHOOD_FEATURES_PATH, TRANSACTIONS_PATH):

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
def _(df_transactions_raw):

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
def _():
    mo.md("""
    ## Feature Catalog

    The feature catalog classifies every column from col_final.gpkg and defines the model-ready transformed columns used downstream. This keeps source columns, scaling choices, and modelling roles visible.
    """)
    return


@app.cell
def _(df_neighborhood_raw):

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
def _(df_neighborhood_raw, feature_catalog):
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
    logistics_candidate_cols = feature_catalog.loc[
        lambda df: df["role"].eq("logistics_screen") & df["eligible"],
        "model_column",
    ].tolist()
    model_ready_feature_cols = [
        *base_control_cols,
        *job_candidate_cols,
        *mfg_candidate_cols,
        *logistics_candidate_cols,
    ]

    prepared_feature_summary = pd.DataFrame(
        [
            {"family": "base_controls", "features": len(base_control_cols)},
            {"family": "job_candidates", "features": len(job_candidate_cols)},
            {
                "family": "manufacturing_cluster_candidates",
                "features": len(mfg_candidate_cols),
            },
            {
                "family": "logistics_cluster_candidates",
                "features": len(logistics_candidate_cols),
            },
            {"family": "built_area_history", "features": len(built_area_cols)},
        ]
    )
    prepared_feature_summary
    return (
        base_control_cols,
        built_area_cols,
        job_candidate_cols,
        logistics_candidate_cols,
        mfg_candidate_cols,
        model_ready_feature_cols,
        prepared_neighborhood_features,
    )


@app.cell
def _(
    TARGET_SCALE_LOWER,
    TARGET_SCALE_UPPER,
    model_ready_feature_cols,
    prepared_neighborhood_features,
):
    scale_audit = compute_scale_audit(
        prepared_neighborhood_features,
        model_ready_feature_cols,
        target_scale_lower=TARGET_SCALE_LOWER,
        target_scale_upper=TARGET_SCALE_UPPER,
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
def _():
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
    df_neighborhood_raw,
    df_transactions_raw,
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
        _name_to_idx_map,
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
def _():
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
def _():
    mo.md("""
    ## Model Specifications

    Specifications are data. Each exploratory model adds one candidate variable to the shared controls and the transaction-year built-area term.
    """)
    return


@app.cell
def _(
    base_control_cols,
    job_candidate_cols,
    logistics_candidate_cols,
    mfg_candidate_cols,
):
    model_specs, model_spec_summary = build_single_candidate_model_specs(
        base_control_cols,
        job_candidate_cols,
        mfg_candidate_cols,
        logistics_candidate_cols,
    )
    model_spec_summary
    return model_spec_summary, model_specs


@app.cell
def _(
    built_area_cols,
    choice_neighborhood_features,
    df_transactions_model,
    model_specs,
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
                "spec_kind": _spec["spec_kind"],
                "candidate_families": _spec["candidate_families"],
                "candidate_features": _spec["candidate_features"],
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
    _spec_vif_detail = pd.concat(_spec_vif_frames, ignore_index=True)
    spec_diagnostics_summary
    return


@app.cell(hide_code=True)
def _():
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
    model_specs,
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
                "spec_kind": _spec["spec_kind"],
                "candidate_families": _spec["candidate_families"],
                "candidate_features": _spec["candidate_features"],
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
            "spec_kind",
            "candidate_families",
            "candidate_features",
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
            model_spec_summary.loc[
                :, ["spec_id", "candidate_features", "candidate_families"]
            ].explode(["candidate_features", "candidate_families"]),
            on="spec_id",
            how="left",
        )
        .loc[lambda df: df["feature"].eq(df["candidate_features"])]
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
def _(FINALIST_COUNT):
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
            "spec_kind",
            "candidate_families",
            "candidate_features",
            "screen_aic",
            "screen_delta_aic_vs_baseline",
            "screen_converged",
        ],
    ]
    finalist_selection_table
    return (finalist_specs,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Final Biogeme Estimation

    Each finalist receives a fresh Biogeme run with output files, saved iterations, recycle, and bootstrap disabled. The comparison below decides among the baseline and selected finalists using the full Biogeme estimates.
    """)
    return


@app.cell
def _(
    BIOGEME_MODEL_PREFIX,
    MISSING_VALUE_SENTINEL,
    built_area_cols,
    choice_neighborhood_features,
    df_transactions_model,
    finalist_specs,
    model_spec_summary,
):
    biogeme_artifacts = {}
    for _spec_id, _spec in finalist_specs.items():
        biogeme_artifacts[_spec_id] = fit_biogeme_model(
            _spec_id,
            _spec["static_cols"],
            choice_neighborhood_features,
            df_transactions_model,
            built_area_cols,
            BIOGEME_MODEL_PREFIX,
            MISSING_VALUE_SENTINEL,
        )

    biogeme_model_comparison = (
        pd.DataFrame(
            [artifact["summary_row"] for artifact in biogeme_artifacts.values()]
        )
        .merge(
            model_spec_summary.loc[
                :, ["spec_id", "spec_kind", "candidate_families", "candidate_features"]
            ],
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
def _(biogeme_artifacts, biogeme_model_comparison, model_specs):

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
                "candidate_features": model_specs[selected_spec_id][
                    "candidate_features"
                ],
                "candidate_families": model_specs[selected_spec_id][
                    "candidate_families"
                ],
                "spec_kind": model_specs[selected_spec_id]["spec_kind"],
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
def _(selected_artifact):

    selected_derivative_check = run_derivative_check(selected_artifact)
    selected_derivative_check
    return (selected_derivative_check,)


@app.cell
def _(BIOGEME_MODEL_PREFIX, selected_derivative_check):

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
def _():
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
    built_area_cols,
    choice_neighborhood_features,
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
        _,
        selected_feature_correlation,
        selected_feature_vif,
        _,
    ) = compute_feature_diagnostics(selected_diagnostics_frame)
    selected_feature_vif
    return selected_diagnostics_frame, selected_feature_correlation


@app.cell
def _(selected_feature_correlation):

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
def _():
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
def _(logistics_sample_diagnostic_frame):

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
