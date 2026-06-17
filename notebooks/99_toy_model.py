import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Toy Jobs + Supply Choice Model

    A small Biogeme multinomial logit baseline for neighborhood choice. The main model uses automatically filtered decayed job-accessibility covariates plus transaction-year built area as a supply proxy; the jobs-only model is kept as a quick sensitivity check.
    """)
    return


@app.cell
def _():
    import math  # noqa: PLC0415
    import os  # noqa: PLC0415
    import warnings  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415
    from typing import cast  # noqa: PLC0415

    import biogeme.database as db  # noqa: PLC0415
    import geopandas as gpd  # noqa: PLC0415
    import marimo as mo  # noqa: PLC0415
    import matplotlib.pyplot as plt  # noqa: PLC0415
    import numpy as np  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415
    import seaborn as sns  # noqa: PLC0415
    from biogeme import models  # noqa: PLC0415
    from biogeme.biogeme import BIOGEME  # noqa: PLC0415
    from biogeme.expressions import Beta, Variable  # noqa: PLC0415
    from biogeme.parameters import Parameters  # noqa: PLC0415
    from biogeme.results_processing import (  # noqa: PLC0415
        get_pandas_estimated_parameters,
    )
    from scipy.special import logsumexp  # noqa: PLC0415

    from housing_choice.modeling import (  # noqa: PLC0415
        align_choice_data,
        build_feature_catalog,
        build_feature_diagnostics_frame,
        compute_feature_diagnostics,
        compute_scale_audit,
        nice_scale_denominator,
        prepare_neighborhood_features,
        prepare_transactions,
        safe_identifier,
        validate_choice_dataframe,
    )

    warnings.filterwarnings("ignore", category=FutureWarning)
    return (
        BIOGEME,
        Beta,
        Parameters,
        Path,
        Variable,
        align_choice_data,
        build_feature_catalog,
        build_feature_diagnostics_frame,
        cast,
        compute_feature_diagnostics,
        compute_scale_audit,
        db,
        get_pandas_estimated_parameters,
        gpd,
        logsumexp,
        math,
        mo,
        models,
        nice_scale_denominator,
        np,
        os,
        pd,
        plt,
        prepare_neighborhood_features,
        prepare_transactions,
        safe_identifier,
        sns,
        validate_choice_dataframe,
    )


@app.cell
def _(Path, os):
    GENERATED_PATH = Path(os.environ["DATA_PATH"]) / "generated"
    NEIGHBORHOOD_FEATURES_PATH = GENERATED_PATH / "col_final.gpkg"
    TRANSACTIONS_PATH = GENERATED_PATH / "transactions_final.parquet"

    MODELING_YEAR_MIN = 2020
    MODELING_YEAR_MAX = 2025
    TRANSACTION_THRESH = 20
    MISSING_VALUE_SENTINEL = 99999
    TOY_MODEL_PREFIX = "toy_jobs"

    DECAY_WEIGHT = 0.5
    JOB_SECTORS = [
        "business_services",
        "care_education_health",
        "commerce",
        "construction",
        "local_services",
        "logistics",
        "manufacture",
    ]
    CANDIDATE_JOB_FEATURE_COLS = [
        f"jobs_{sector}_decay_10_20_2025_scaled" for sector in JOB_SECTORS
    ]
    JOB_FEATURE_PRIORITY = [
        "jobs_logistics_decay_10_20_2025_scaled",
        "jobs_manufacture_decay_10_20_2025_scaled",
        "jobs_commerce_decay_10_20_2025_scaled",
        "jobs_business_services_decay_10_20_2025_scaled",
        "jobs_care_education_health_decay_10_20_2025_scaled",
        "jobs_local_services_decay_10_20_2025_scaled",
        "jobs_construction_decay_10_20_2025_scaled",
    ]
    PAIRWISE_CORRELATION_THRESHOLD = 0.95
    VIF_THRESHOLD = 20.0

    MAIN_SPEC_ID = "jobs_filtered_plus_built_area"
    SENSITIVITY_SPEC_ID = "jobs_filtered"
    return (
        CANDIDATE_JOB_FEATURE_COLS,
        DECAY_WEIGHT,
        JOB_FEATURE_PRIORITY,
        JOB_SECTORS,
        MAIN_SPEC_ID,
        MISSING_VALUE_SENTINEL,
        MODELING_YEAR_MAX,
        MODELING_YEAR_MIN,
        NEIGHBORHOOD_FEATURES_PATH,
        PAIRWISE_CORRELATION_THRESHOLD,
        SENSITIVITY_SPEC_ID,
        TOY_MODEL_PREFIX,
        TRANSACTIONS_PATH,
        TRANSACTION_THRESH,
        VIF_THRESHOLD,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Inputs

    The notebook uses the canonical generated feature and transaction exports, with the same year window and neighborhood threshold as the main modelling workflow.
    """)
    return


@app.cell
def _(NEIGHBORHOOD_FEATURES_PATH, TRANSACTIONS_PATH, gpd, pd):
    df_neighborhood_raw = gpd.read_file(NEIGHBORHOOD_FEATURES_PATH)
    df_transactions_raw = pd.read_parquet(TRANSACTIONS_PATH)

    toy_input_summary = pd.DataFrame(
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
    toy_input_summary
    return df_neighborhood_raw, df_transactions_raw


@app.cell
def _(
    DECAY_WEIGHT,
    JOB_SECTORS,
    build_feature_catalog,
    df_neighborhood_raw,
    nice_scale_denominator,
    pd,
    prepare_neighborhood_features,
):
    feature_catalog = build_feature_catalog(df_neighborhood_raw)
    prepared_neighborhood_features = prepare_neighborhood_features(
        df_neighborhood_raw,
        feature_catalog,
    )
    toy_built_area_cols = sorted(
        [
            column
            for column in prepared_neighborhood_features.columns
            if column.startswith("built_area_")
        ]
    )

    missing_decay_source_columns = sorted(
        [
            source_column
            for sector in JOB_SECTORS
            for source_column in (
                f"jobs_{sector}_10_2025",
                f"jobs_{sector}_20_2025",
            )
            if source_column not in df_neighborhood_raw.columns
        ]
    )
    if missing_decay_source_columns:
        _missing_decay_message = (
            f"Decay source columns are missing: {missing_decay_source_columns}"
        )
        raise ValueError(_missing_decay_message)

    toy_decayed_feature_rows = []
    for sector in JOB_SECTORS:
        source_10_column = f"jobs_{sector}_10_2025"
        source_20_column = f"jobs_{sector}_20_2025"
        model_column = f"jobs_{sector}_decay_10_20_2025_scaled"
        jobs_10 = pd.to_numeric(
            df_neighborhood_raw[source_10_column],
            errors="coerce",
        ).astype(float)
        jobs_20 = pd.to_numeric(
            df_neighborhood_raw[source_20_column],
            errors="coerce",
        ).astype(float)
        ring_jobs = jobs_20 - jobs_10
        negative_ring_rows = int(ring_jobs.lt(-1e-9).sum())
        if negative_ring_rows:
            _negative_ring_message = (
                f"{source_20_column} is smaller than {source_10_column} "
                f"for {negative_ring_rows} rows."
            )
            raise ValueError(_negative_ring_message)
        decayed_jobs = jobs_10 + DECAY_WEIGHT * ring_jobs
        scale_denominator = nice_scale_denominator(decayed_jobs)
        prepared_neighborhood_features[model_column] = decayed_jobs / scale_denominator
        scaled_values = prepared_neighborhood_features[model_column]
        toy_decayed_feature_rows.append(
            {
                "sector": sector,
                "source_10_column": source_10_column,
                "source_20_column": source_20_column,
                "model_column": model_column,
                "decay_weight_10_20_band": DECAY_WEIGHT,
                "scale_denominator": scale_denominator,
                "ring_min": float(ring_jobs.min()),
                "ring_max": float(ring_jobs.max()),
                "negative_ring_rows": negative_ring_rows,
                "decayed_jobs_min": float(decayed_jobs.min()),
                "decayed_jobs_p75": float(decayed_jobs.quantile(0.75)),
                "decayed_jobs_max": float(decayed_jobs.max()),
                "scaled_min": float(scaled_values.min()),
                "scaled_p75": float(scaled_values.quantile(0.75)),
                "scaled_max": float(scaled_values.max()),
                "transform": (
                    "jobs_10 + "
                    f"{DECAY_WEIGHT:g} * (jobs_20 - jobs_10), "
                    f"then divide by {scale_denominator:g} jobs"
                ),
            }
        )

    toy_decayed_feature_build_report = pd.DataFrame(toy_decayed_feature_rows).round(
        {
            "scale_denominator": 4,
            "ring_min": 3,
            "ring_max": 3,
            "decayed_jobs_min": 3,
            "decayed_jobs_p75": 3,
            "decayed_jobs_max": 3,
            "scaled_min": 4,
            "scaled_p75": 4,
            "scaled_max": 4,
        }
    )
    toy_decayed_feature_build_report
    return (
        prepared_neighborhood_features,
        toy_built_area_cols,
        toy_decayed_feature_build_report,
    )


@app.cell
def _(
    CANDIDATE_JOB_FEATURE_COLS,
    compute_scale_audit,
    prepared_neighborhood_features,
):
    toy_candidate_scale_audit = compute_scale_audit(
        prepared_neighborhood_features,
        CANDIDATE_JOB_FEATURE_COLS,
    )
    toy_candidate_scale_audit
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
        df_transactions_toy_base,
        _toy_transaction_count_by_neighborhood,
        toy_wanted_neighborhoods,
    ) = prepare_transactions(
        df_transactions_raw,
        df_neighborhood_raw["name_detail"],
        MODELING_YEAR_MIN,
        MODELING_YEAR_MAX,
        TRANSACTION_THRESH,
    )
    (
        choice_neighborhood_features,
        df_transactions_toy,
        _toy_name_to_idx_map,
    ) = align_choice_data(
        prepared_neighborhood_features,
        df_transactions_toy_base,
        toy_wanted_neighborhoods,
    )

    toy_choice_set_summary = pd.DataFrame(
        [
            {
                "transaction_threshold": TRANSACTION_THRESH,
                "candidate_neighborhoods": len(df_neighborhood_raw),
                "model_neighborhoods": len(choice_neighborhood_features),
                "raw_transactions": len(df_transactions_raw),
                "model_transactions": len(df_transactions_toy),
                "min_purchase_year": int(df_transactions_toy["purchase_year"].min()),
                "max_purchase_year": int(df_transactions_toy["purchase_year"].max()),
            }
        ]
    )
    toy_choice_set_summary
    return choice_neighborhood_features, df_transactions_toy


@app.cell
def _(df_transactions_toy):
    toy_transaction_year_counts = (
        df_transactions_toy["purchase_year"]
        .value_counts()
        .sort_index()
        .rename_axis("purchase_year")
        .reset_index(name="transactions")
    )
    toy_transaction_year_counts
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Covariate Screening

    The toy model starts from one decayed accessibility metric per job sector. Jobs within 10 minutes count fully, while jobs in the 10-20 minute band count partially. Highly redundant decayed sector metrics are removed before fitting so the baseline stays small and the coefficients are less dominated by multicollinearity.
    """)
    return


@app.cell
def _(build_feature_diagnostics_frame, compute_feature_diagnostics, np, pd):
    def filter_redundant_job_covariates(
        feature_frame: pd.DataFrame,
        transactions: pd.DataFrame,
        candidate_cols: list[str],
        priority_cols: list[str],
        built_area_cols: list[str],
        *,
        correlation_threshold: float,
        vif_threshold: float,
    ) -> tuple[list[str], pd.DataFrame]:
        priority_rank = {feature: rank for rank, feature in enumerate(priority_cols)}
        ordered_candidates = sorted(
            candidate_cols,
            key=lambda feature: priority_rank.get(feature, len(priority_rank)),
        )
        drop_rows = []
        valid_candidates = []
        for feature in ordered_candidates:
            if feature not in feature_frame.columns:
                drop_rows.append(
                    {
                        "feature": feature,
                        "stage": "validity",
                        "reason": "missing_feature",
                        "conflicting_feature": "",
                        "correlation": np.nan,
                        "vif": np.nan,
                        "threshold": np.nan,
                    }
                )
                continue
            values = pd.to_numeric(feature_frame[feature], errors="coerce")
            finite_values = values.replace([np.inf, -np.inf], np.nan).dropna()
            if len(finite_values) != len(values):
                drop_rows.append(
                    {
                        "feature": feature,
                        "stage": "validity",
                        "reason": "non_finite_or_missing",
                        "conflicting_feature": "",
                        "correlation": np.nan,
                        "vif": np.nan,
                        "threshold": np.nan,
                    }
                )
                continue
            if finite_values.eq(finite_values.iloc[0]).all():
                drop_rows.append(
                    {
                        "feature": feature,
                        "stage": "validity",
                        "reason": "zero_variance",
                        "conflicting_feature": "",
                        "correlation": np.nan,
                        "vif": np.nan,
                        "threshold": np.nan,
                    }
                )
                continue
            valid_candidates.append(feature)

        retained = []
        correlation = feature_frame.loc[:, valid_candidates].astype(float).corr()
        for feature in valid_candidates:
            conflicting_pairs = [
                (
                    retained_feature,
                    abs(float(correlation.loc[feature, retained_feature])),
                )
                for retained_feature in retained
                if abs(float(correlation.loc[feature, retained_feature]))
                >= correlation_threshold
            ]
            if conflicting_pairs:
                conflicting_feature, correlation_value = max(
                    conflicting_pairs,
                    key=lambda pair: pair[1],
                )
                drop_rows.append(
                    {
                        "feature": feature,
                        "stage": "pairwise_correlation",
                        "reason": "high_pairwise_correlation",
                        "conflicting_feature": conflicting_feature,
                        "correlation": correlation_value,
                        "vif": np.nan,
                        "threshold": correlation_threshold,
                    }
                )
                continue
            retained.append(feature)

        while len(retained) > 1:
            diagnostics_frame = build_feature_diagnostics_frame(
                feature_frame,
                transactions,
                retained,
                built_area_cols,
            )
            _, _, vif, _ = compute_feature_diagnostics(diagnostics_frame)
            vif_candidates = vif.loc[lambda df: df["feature"].isin(retained)].copy()
            max_vif = float(vif_candidates["vif"].max())
            if max_vif <= vif_threshold:
                break
            max_vif_candidates = vif_candidates.loc[
                vif_candidates["vif"].eq(max_vif)
            ].copy()
            max_vif_candidates = max_vif_candidates.assign(
                priority_rank=lambda df: df["feature"].map(priority_rank).fillna(9999)
            )
            feature_to_drop = str(
                max_vif_candidates.sort_values(
                    ["priority_rank", "feature"],
                    ascending=[False, True],
                ).iloc[0]["feature"]
            )
            drop_rows.append(
                {
                    "feature": feature_to_drop,
                    "stage": "vif",
                    "reason": "high_vif",
                    "conflicting_feature": "",
                    "correlation": np.nan,
                    "vif": max_vif,
                    "threshold": vif_threshold,
                }
            )
            retained.remove(feature_to_drop)

        screening_report = pd.DataFrame(
            drop_rows,
            columns=[
                "feature",
                "stage",
                "reason",
                "conflicting_feature",
                "correlation",
                "vif",
                "threshold",
            ],
        )
        return retained, screening_report

    return (filter_redundant_job_covariates,)


@app.cell
def _(
    CANDIDATE_JOB_FEATURE_COLS,
    build_feature_diagnostics_frame,
    choice_neighborhood_features,
    compute_feature_diagnostics,
    df_transactions_toy,
    toy_built_area_cols,
):
    toy_candidate_diagnostics_frame = build_feature_diagnostics_frame(
        choice_neighborhood_features,
        df_transactions_toy,
        CANDIDATE_JOB_FEATURE_COLS,
        toy_built_area_cols,
    )
    (
        _,
        toy_candidate_feature_correlation,
        toy_candidate_feature_vif,
        _toy_candidate_max_abs_correlation,
    ) = compute_feature_diagnostics(toy_candidate_diagnostics_frame)
    toy_candidate_feature_vif
    return toy_candidate_feature_correlation, toy_candidate_feature_vif


@app.cell
def _(plt, sns, toy_candidate_feature_correlation):
    toy_candidate_correlation_heatmap_figure, toy_candidate_correlation_heatmap_axis = (
        plt.subplots(figsize=(8, 6))
    )
    sns.heatmap(
        toy_candidate_feature_correlation,
        annot=True,
        cmap="vlag",
        center=0,
        vmin=-1,
        vmax=1,
        ax=toy_candidate_correlation_heatmap_axis,
    )
    toy_candidate_correlation_heatmap_axis.set_title(
        "Candidate decayed job covariate correlation before filtering"
    )
    toy_candidate_correlation_heatmap_figure.tight_layout()
    toy_candidate_correlation_heatmap_figure
    return


@app.cell(hide_code=True)
def _(
    DECAY_WEIGHT,
    mo,
    np,
    toy_candidate_feature_correlation,
    toy_candidate_feature_vif,
):
    _max_before_filter_correlation = float(
        toy_candidate_feature_correlation.abs()
        .where(~np.eye(len(toy_candidate_feature_correlation), dtype=bool))
        .max()
        .max()
    )
    _max_before_filter_vif = float(toy_candidate_feature_vif["vif"].max())
    mo.md(
        f"""
        **Screening note.** Each candidate job covariate is a decayed 10-20 minute accessibility metric: 0-10 minute jobs count fully and 10-20 minute jobs count `{DECAY_WEIGHT:.2f}`. Before filtering, the maximum absolute feature correlation is `{_max_before_filter_correlation:.3f}` and the maximum VIF is `{_max_before_filter_vif:.1f}`. Coefficients from the filtered toy model are still a sanity check, but the worst redundancy is removed before fitting.
        """
    )
    return


@app.cell
def _(
    CANDIDATE_JOB_FEATURE_COLS,
    JOB_FEATURE_PRIORITY,
    PAIRWISE_CORRELATION_THRESHOLD,
    VIF_THRESHOLD,
    choice_neighborhood_features,
    df_transactions_toy,
    filter_redundant_job_covariates,
    pd,
    toy_built_area_cols,
):
    (
        FILTERED_JOB_FEATURE_COLS,
        toy_covariate_screening_report,
    ) = filter_redundant_job_covariates(
        choice_neighborhood_features,
        df_transactions_toy,
        CANDIDATE_JOB_FEATURE_COLS,
        JOB_FEATURE_PRIORITY,
        toy_built_area_cols,
        correlation_threshold=PAIRWISE_CORRELATION_THRESHOLD,
        vif_threshold=VIF_THRESHOLD,
    )
    if not FILTERED_JOB_FEATURE_COLS:
        _empty_filter_message = "Covariate filtering removed every job covariate."
        raise ValueError(_empty_filter_message)

    toy_covariate_retention_summary = pd.DataFrame(
        [
            {
                "candidate_job_features": len(CANDIDATE_JOB_FEATURE_COLS),
                "retained_job_features": len(FILTERED_JOB_FEATURE_COLS),
                "dropped_job_features": len(toy_covariate_screening_report),
                "pairwise_correlation_threshold": PAIRWISE_CORRELATION_THRESHOLD,
                "vif_threshold": VIF_THRESHOLD,
            }
        ]
    )
    toy_covariate_retention_summary
    return FILTERED_JOB_FEATURE_COLS, toy_covariate_screening_report


@app.cell
def _(toy_covariate_screening_report):
    toy_covariate_screening_report.round(
        {
            "correlation": 3,
            "vif": 3,
            "threshold": 3,
        }
    )
    return


@app.cell
def _(FILTERED_JOB_FEATURE_COLS, toy_decayed_feature_build_report):
    toy_filtered_feature_catalog = (
        toy_decayed_feature_build_report.loc[
            lambda df: df["model_column"].isin(FILTERED_JOB_FEATURE_COLS),
            [
                "sector",
                "source_10_column",
                "source_20_column",
                "model_column",
                "decay_weight_10_20_band",
                "scale_denominator",
                "transform",
            ],
        ]
        .assign(
            retained_order=lambda df: df["model_column"].map(
                {feature: idx for idx, feature in enumerate(FILTERED_JOB_FEATURE_COLS)}
            )
        )
        .sort_values("retained_order")
        .drop(columns="retained_order")
        .reset_index(drop=True)
    )
    toy_filtered_feature_catalog
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Local Toy Model Helpers

    The project fitting helper always includes transaction-year built area. These notebook-local wrappers keep that behavior for the main model while also allowing a true jobs-only sensitivity fit.
    """)
    return


@app.cell
def _(Parameters, np, pd):
    def make_toy_biogeme_parameters() -> Parameters:
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

    def build_toy_choice_dataframe(
        neighborhood_features: pd.DataFrame,
        transactions: pd.DataFrame,
        static_cols: list[str],
        built_area_cols: list[str],
        *,
        include_built_area: bool,
    ) -> tuple[pd.DataFrame, list[str]]:
        model_feature_cols = [*static_cols]
        if include_built_area:
            model_feature_cols.append("log_built_area_ha")

        static_series = (
            neighborhood_features.loc[:, static_cols]
            .reset_index(names="index")
            .melt(id_vars="index")
            .assign(index=lambda df: df["index"].astype(str))
            .assign(variable=lambda df: df["variable"] + "_" + df["index"])
            .drop(columns="index")
            .set_index("variable")["value"]
        )

        dynamic_features: dict[str, pd.Series] = {}
        if include_built_area:
            for idx, row in neighborhood_features.loc[:, built_area_cols].iterrows():
                area_by_year = {
                    int(col.rsplit("_", maxsplit=1)[1]): float(row[col])
                    for col in built_area_cols
                }
                mapped_area = (
                    transactions["purchase_year"].map(area_by_year).astype(float)
                )
                dynamic_features[f"log_built_area_ha_{idx}"] = pd.Series(
                    np.log1p(mapped_area.div(10_000)),
                    index=transactions.index,
                )

        choice_frame = pd.concat(
            [
                transactions.loc[:, ["neighborhood_idx", "purchase_year"]].reset_index(
                    drop=True
                ),
                pd.DataFrame(
                    dict(static_series.items()),
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

    return build_toy_choice_dataframe, make_toy_biogeme_parameters


@app.cell
def _(
    BIOGEME,
    Beta,
    MISSING_VALUE_SENTINEL,
    TOY_MODEL_PREFIX,
    Variable,
    build_toy_choice_dataframe,
    db,
    get_pandas_estimated_parameters,
    make_toy_biogeme_parameters,
    math,
    models,
    pd,
    safe_identifier,
    validate_choice_dataframe,
):
    def fit_toy_biogeme_model(
        spec_id: str,
        static_cols: list[str],
        neighborhood_features: pd.DataFrame,
        transactions: pd.DataFrame,
        built_area_cols: list[str],
        *,
        include_built_area: bool,
    ) -> dict[str, object]:
        choice_frame, model_feature_cols = build_toy_choice_dataframe(
            neighborhood_features,
            transactions,
            static_cols,
            built_area_cols,
            include_built_area=include_built_area,
        )
        validation = validate_choice_dataframe(
            choice_frame,
            model_feature_cols,
            len(neighborhood_features),
            missing_value_sentinel=MISSING_VALUE_SENTINEL,
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
            parameters=make_toy_biogeme_parameters(),
        )
        biogeme_model.model_name = f"{TOY_MODEL_PREFIX}_{safe_identifier(spec_id)}"
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
        final_log_likelihood = float(results.final_log_likelihood)
        null_log_likelihood = -float(
            len(transactions) * math.log(len(neighborhood_features))
        )
        mcfadden_pseudo_r2 = 1 - final_log_likelihood / null_log_likelihood

        return {
            "spec_id": spec_id,
            "static_cols": list(static_cols),
            "model_feature_cols": model_feature_cols,
            "include_built_area": include_built_area,
            "choice_frame": choice_frame,
            "validation": validation,
            "biogeme_model": biogeme_model,
            "results": results,
            "estimated_parameters": estimated_parameters,
            "beta_name_by_feature": beta_name_by_feature,
            "summary_row": {
                "spec_id": spec_id,
                "parameters": results.number_of_parameters,
                "sample_size": results.sample_size,
                "alternatives": len(neighborhood_features),
                "final_log_likelihood": final_log_likelihood,
                "null_log_likelihood": null_log_likelihood,
                "mcfadden_pseudo_r2": mcfadden_pseudo_r2,
                "aic": results.akaike_information_criterion,
                "bic": results.bayesian_information_criterion,
                "algorithm_has_converged": bool(
                    getattr(results, "algorithm_has_converged", False)
                ),
            },
        }

    return (fit_toy_biogeme_model,)


@app.cell
def _(cast, logsumexp, np, pd):
    def predict_toy_choice_shares(
        artifact: dict[str, object],
        neighborhood_features: pd.DataFrame,
        transactions: pd.DataFrame,
        built_area_cols: list[str],
    ) -> pd.DataFrame:
        estimated_parameters = cast(pd.DataFrame, artifact["estimated_parameters"])
        params = estimated_parameters.set_index("feature")["Value"].to_dict()
        static_cols = cast(list[str], artifact["static_cols"])
        include_built_area = bool(artifact["include_built_area"])
        static_utility = neighborhood_features.loc[:, static_cols].astype(
            float
        ).to_numpy() @ np.array([params[col] for col in static_cols])

        year_to_log_built_area = {}
        if include_built_area:
            year_to_log_built_area = {
                int(col.rsplit("_", maxsplit=1)[1]): np.log1p(
                    neighborhood_features[col].astype(float).to_numpy() / 10_000
                )
                for col in built_area_cols
            }
            built_beta = params["log_built_area_ha"]
        else:
            built_beta = 0.0

        probabilities = []
        for year in transactions["purchase_year"].astype(int):
            utility = static_utility.copy()
            if include_built_area:
                utility = utility + built_beta * year_to_log_built_area[int(year)]
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

    return (predict_toy_choice_shares,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Biogeme Fits

    The main model adds a transaction-year supply proxy to the automatically filtered decayed job-sector covariates. The sensitivity model shows what changes when the fit is constrained to filtered decayed jobs only.
    """)
    return


@app.cell
def _(
    FILTERED_JOB_FEATURE_COLS,
    MAIN_SPEC_ID,
    SENSITIVITY_SPEC_ID,
    choice_neighborhood_features,
    df_transactions_toy,
    fit_toy_biogeme_model,
    pd,
    toy_built_area_cols,
):
    toy_model_specs = {
        SENSITIVITY_SPEC_ID: {
            "description": "Filtered decayed jobs only",
            "static_cols": FILTERED_JOB_FEATURE_COLS,
            "include_built_area": False,
        },
        MAIN_SPEC_ID: {
            "description": "Filtered decayed jobs plus transaction-year built area",
            "static_cols": FILTERED_JOB_FEATURE_COLS,
            "include_built_area": True,
        },
    }

    toy_model_artifacts = {
        spec_id: fit_toy_biogeme_model(
            spec_id,
            spec["static_cols"],
            choice_neighborhood_features,
            df_transactions_toy,
            toy_built_area_cols,
            include_built_area=spec["include_built_area"],
        )
        for spec_id, spec in toy_model_specs.items()
    }

    toy_model_comparison = (
        pd.DataFrame(
            [artifact["summary_row"] for artifact in toy_model_artifacts.values()]
        )
        .assign(
            description=lambda df: df["spec_id"].map(
                {
                    spec_id: spec["description"]
                    for spec_id, spec in toy_model_specs.items()
                }
            ),
            delta_aic_vs_best=lambda df: df["aic"] - df["aic"].min(),
        )
        .sort_values("aic")
        .reset_index(drop=True)
        .round(
            {
                "final_log_likelihood": 3,
                "null_log_likelihood": 3,
                "mcfadden_pseudo_r2": 4,
                "aic": 3,
                "bic": 3,
                "delta_aic_vs_best": 3,
            }
        )
    )
    toy_model_comparison
    return toy_model_artifacts, toy_model_specs


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Model Fit Reports

    Biogeme exposes optimizer and fit diagnostics on each estimation result. The tables below combine those reported values with the equal-share null likelihood used for this toy comparison.
    """)
    return


@app.cell
def _(cast, np, pd, toy_model_artifacts, toy_model_specs):
    toy_general_stat_rows = []
    toy_optimizer_rows = []
    toy_fit_rows = []

    for _spec_id, _artifact in toy_model_artifacts.items():
        _results = _artifact["results"]
        _summary = cast(dict[str, object], _artifact["summary_row"])
        _description = str(toy_model_specs[_spec_id]["description"])
        _general_stats = _results.get_general_statistics()
        _raw_results = getattr(_results, "raw_estimation_results", None)
        _optimization_messages = (
            getattr(_raw_results, "optimization_messages", {}) if _raw_results else {}
        ) or {}
        _gradient = getattr(_raw_results, "gradient", []) if _raw_results else []
        _gradient_norm = (
            float(np.linalg.norm(np.asarray(_gradient, dtype=float)))
            if len(_gradient)
            else np.nan
        )
        _optimization_time = _optimization_messages.get("Optimization time")
        _optimization_time_seconds = (
            float(_optimization_time.total_seconds())
            if hasattr(_optimization_time, "total_seconds")
            else np.nan
        )
        _initial_log_likelihood = float(
            getattr(_raw_results, "initial_log_likelihood", np.nan)
        )
        _final_log_likelihood = float(_summary["final_log_likelihood"])
        _equal_share_null_log_likelihood = float(_summary["null_log_likelihood"])
        _likelihood_gain_vs_init = _final_log_likelihood - _initial_log_likelihood
        _likelihood_gain_vs_equal_share = (
            _final_log_likelihood - _equal_share_null_log_likelihood
        )

        toy_general_stat_rows.extend(
            {
                "spec_id": _spec_id,
                "description": _description,
                "statistic": _statistic,
                "value": _value,
            }
            for _statistic, _value in _general_stats.items()
        )
        toy_optimizer_rows.append(
            {
                "spec_id": _spec_id,
                "description": _description,
                "algorithm_has_converged": bool(_summary["algorithm_has_converged"]),
                "cause_of_termination": _optimization_messages.get(
                    "Cause of termination", ""
                ),
                "algorithm": _optimization_messages.get("Algorithm", ""),
                "iterations": pd.to_numeric(
                    _optimization_messages.get("Number of iterations", np.nan),
                    errors="coerce",
                ),
                "function_evaluations": pd.to_numeric(
                    _optimization_messages.get(
                        "Number of function evaluations", np.nan
                    ),
                    errors="coerce",
                ),
                "gradient_evaluations": pd.to_numeric(
                    _optimization_messages.get(
                        "Number of gradient evaluations", np.nan
                    ),
                    errors="coerce",
                ),
                "hessian_evaluations": pd.to_numeric(
                    _optimization_messages.get("Number of hessian evaluations", np.nan),
                    errors="coerce",
                ),
                "relative_gradient": pd.to_numeric(
                    _optimization_messages.get("Relative gradient", np.nan),
                    errors="coerce",
                ),
                "final_gradient_norm": _gradient_norm,
                "optimization_time_seconds": _optimization_time_seconds,
            }
        )
        toy_fit_rows.append(
            {
                "spec_id": _spec_id,
                "description": _description,
                "parameters": _summary["parameters"],
                "sample_size": _summary["sample_size"],
                "alternatives": _summary["alternatives"],
                "initial_log_likelihood": _initial_log_likelihood,
                "equal_share_null_log_likelihood": _equal_share_null_log_likelihood,
                "final_log_likelihood": _final_log_likelihood,
                "likelihood_gain_vs_init": _likelihood_gain_vs_init,
                "likelihood_gain_vs_equal_share": _likelihood_gain_vs_equal_share,
                "lr_stat_vs_init": 2 * _likelihood_gain_vs_init,
                "lr_stat_vs_equal_share": 2 * _likelihood_gain_vs_equal_share,
                "rho_square_init": float(getattr(_results, "rho_square_init", np.nan)),
                "rho_bar_square_init": float(
                    getattr(_results, "rho_bar_square_init", np.nan)
                ),
                "mcfadden_pseudo_r2_equal_share": _summary["mcfadden_pseudo_r2"],
                "aic": _summary["aic"],
                "bic": _summary["bic"],
            }
        )

    toy_biogeme_general_statistics = pd.DataFrame(toy_general_stat_rows)
    toy_optimizer_summary = pd.DataFrame(toy_optimizer_rows).round(
        {
            "relative_gradient": 8,
            "final_gradient_norm": 6,
            "optimization_time_seconds": 3,
        }
    )
    toy_fit_statistics = (
        pd.DataFrame(toy_fit_rows)
        .sort_values("aic")
        .reset_index(drop=True)
        .round(
            {
                "initial_log_likelihood": 3,
                "equal_share_null_log_likelihood": 3,
                "final_log_likelihood": 3,
                "likelihood_gain_vs_init": 3,
                "likelihood_gain_vs_equal_share": 3,
                "lr_stat_vs_init": 3,
                "lr_stat_vs_equal_share": 3,
                "rho_square_init": 4,
                "rho_bar_square_init": 4,
                "mcfadden_pseudo_r2_equal_share": 4,
                "aic": 3,
                "bic": 3,
            }
        )
    )
    toy_fit_statistics
    return (
        toy_biogeme_general_statistics,
        toy_fit_statistics,
        toy_optimizer_summary,
    )


@app.cell
def _(toy_optimizer_summary):
    toy_optimizer_summary
    return


@app.cell
def _(toy_biogeme_general_statistics):
    toy_biogeme_general_statistics.pivot_table(
        index="statistic",
        columns="spec_id",
        values="value",
        aggfunc="first",
    ).reset_index()
    return


@app.cell
def _(toy_fit_statistics):
    toy_information_criteria_plot_data = toy_fit_statistics.set_index("spec_id").loc[
        :, ["aic", "bic"]
    ]
    toy_information_criteria_axis = toy_information_criteria_plot_data.plot.barh(
        figsize=(8, 4),
    )
    toy_information_criteria_axis.set_xlabel("Criterion value; lower is better")
    toy_information_criteria_axis.set_ylabel("")
    toy_information_criteria_axis.set_title("Toy model AIC and BIC")
    toy_information_criteria_axis
    return


@app.cell
def _(toy_fit_statistics):
    toy_likelihood_plot_data = toy_fit_statistics.set_index("spec_id").loc[
        :,
        [
            "initial_log_likelihood",
            "equal_share_null_log_likelihood",
            "final_log_likelihood",
        ],
    ]
    toy_likelihood_axis = toy_likelihood_plot_data.plot.barh(
        figsize=(8, 4.5),
    )
    toy_likelihood_axis.set_xlabel("Log likelihood; less negative is better")
    toy_likelihood_axis.set_ylabel("")
    toy_likelihood_axis.set_title("Initial, null, and final log likelihood")
    toy_likelihood_axis
    return


@app.cell
def _(toy_fit_statistics):
    toy_relative_fit_plot_data = toy_fit_statistics.set_index("spec_id").loc[
        :,
        [
            "rho_square_init",
            "rho_bar_square_init",
            "mcfadden_pseudo_r2_equal_share",
        ],
    ]
    toy_relative_fit_axis = toy_relative_fit_plot_data.plot.barh(
        figsize=(8, 4),
    )
    toy_relative_fit_axis.set_xlabel("Pseudo-R2")
    toy_relative_fit_axis.set_ylabel("")
    toy_relative_fit_axis.set_title("Relative fit statistics")
    toy_relative_fit_axis
    return


@app.cell
def _(toy_fit_statistics):
    toy_lr_plot_data = toy_fit_statistics.set_index("spec_id").loc[
        :, ["lr_stat_vs_init", "lr_stat_vs_equal_share"]
    ]
    toy_lr_axis = toy_lr_plot_data.plot.barh(
        figsize=(8, 4),
    )
    toy_lr_axis.set_xlabel("Likelihood-ratio statistic; higher is better")
    toy_lr_axis.set_ylabel("")
    toy_lr_axis.set_title("Likelihood improvement over baseline likelihoods")
    toy_lr_axis
    return


@app.cell
def _(MAIN_SPEC_ID, toy_model_artifacts):
    toy_main_artifact = toy_model_artifacts[MAIN_SPEC_ID]
    toy_main_estimated_parameters = toy_main_artifact["estimated_parameters"]
    toy_main_coefficient_summary = (
        toy_main_estimated_parameters.assign(
            value=lambda df: df["Value"].round(4),
            robust_se=lambda df: df["Robust std err."].round(4),
            robust_t=lambda df: df["Robust t-stat."].round(3),
            robust_p=lambda df: df["Robust p-value"].round(4),
        )
        .loc[:, ["feature", "value", "robust_se", "robust_t", "robust_p"]]
        .sort_values("feature")
    )
    toy_main_coefficient_summary
    return toy_main_artifact, toy_main_estimated_parameters


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Final Model Fit Statistics

    The final toy model is the filtered decayed jobs model with transaction-year built area. These tables isolate its fit and optimizer diagnostics from the two-spec comparison above.
    """)
    return


@app.cell
def _(
    FILTERED_JOB_FEATURE_COLS,
    MAIN_SPEC_ID,
    toy_fit_statistics,
    toy_optimizer_summary,
):
    toy_final_model_fit_summary = (
        toy_fit_statistics.loc[lambda df: df["spec_id"].eq(MAIN_SPEC_ID)]
        .merge(
            toy_optimizer_summary.loc[
                :,
                [
                    "spec_id",
                    "algorithm_has_converged",
                    "iterations",
                    "relative_gradient",
                    "final_gradient_norm",
                    "optimization_time_seconds",
                    "cause_of_termination",
                ],
            ],
            on="spec_id",
            how="left",
        )
        .assign(
            retained_job_features=", ".join(FILTERED_JOB_FEATURE_COLS),
            retained_job_feature_count=len(FILTERED_JOB_FEATURE_COLS),
        )
        .loc[
            :,
            [
                "spec_id",
                "description",
                "retained_job_feature_count",
                "retained_job_features",
                "parameters",
                "sample_size",
                "alternatives",
                "initial_log_likelihood",
                "equal_share_null_log_likelihood",
                "final_log_likelihood",
                "likelihood_gain_vs_equal_share",
                "lr_stat_vs_equal_share",
                "rho_square_init",
                "rho_bar_square_init",
                "mcfadden_pseudo_r2_equal_share",
                "aic",
                "bic",
                "algorithm_has_converged",
                "iterations",
                "relative_gradient",
                "final_gradient_norm",
                "optimization_time_seconds",
                "cause_of_termination",
            ],
        ]
    )
    toy_final_model_fit_summary.iloc[0]
    return (toy_final_model_fit_summary,)


@app.cell
def _(toy_final_model_fit_summary):
    toy_final_model_fit_long = (
        toy_final_model_fit_summary.iloc[0]
        .rename_axis("metric")
        .reset_index(name="value")
    )
    toy_final_model_fit_long
    return


@app.cell
def _(MAIN_SPEC_ID, toy_biogeme_general_statistics):
    toy_final_model_biogeme_general_statistics = toy_biogeme_general_statistics.loc[
        lambda df: df["spec_id"].eq(MAIN_SPEC_ID),
        ["statistic", "value"],
    ].reset_index(drop=True)
    toy_final_model_biogeme_general_statistics
    return


@app.cell
def _(toy_final_model_fit_summary):
    toy_final_model_likelihood_plot_data = (
        toy_final_model_fit_summary.loc[
            :,
            [
                "initial_log_likelihood",
                "equal_share_null_log_likelihood",
                "final_log_likelihood",
            ],
        ]
        .melt(var_name="likelihood_type", value_name="log_likelihood")
        .assign(
            likelihood_type=lambda df: df["likelihood_type"].str.replace(
                "_",
                " ",
            )
        )
    )
    toy_final_model_likelihood_axis = toy_final_model_likelihood_plot_data.plot.barh(
        x="likelihood_type",
        y="log_likelihood",
        legend=False,
        figsize=(8, 3.5),
    )
    toy_final_model_likelihood_axis.set_xlabel(
        "Log likelihood; less negative is better"
    )
    toy_final_model_likelihood_axis.set_ylabel("")
    toy_final_model_likelihood_axis.set_title("Final model log likelihood")
    toy_final_model_likelihood_axis
    return


@app.cell
def _(pd, toy_model_artifacts):
    toy_sensitivity_coefficient_summary = (
        pd.concat(
            [
                artifact["estimated_parameters"].loc[
                    :, ["spec_id", "feature", "Value", "Robust std err."]
                ]
                for artifact in toy_model_artifacts.values()
            ],
            ignore_index=True,
        )
        .assign(
            value=lambda df: df["Value"].round(4),
            robust_se=lambda df: df["Robust std err."].round(4),
        )
        .loc[:, ["spec_id", "feature", "value", "robust_se"]]
        .sort_values(["feature", "spec_id"])
    )
    toy_sensitivity_coefficient_summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Diagnostics
    """)
    return


@app.cell
def _(toy_main_artifact):
    toy_main_validation = toy_main_artifact["validation"]
    toy_main_validation
    return


@app.cell
def _(
    MAIN_SPEC_ID,
    build_feature_diagnostics_frame,
    choice_neighborhood_features,
    compute_feature_diagnostics,
    df_transactions_toy,
    toy_built_area_cols,
    toy_model_specs,
):
    toy_main_diagnostics_frame = build_feature_diagnostics_frame(
        choice_neighborhood_features,
        df_transactions_toy,
        toy_model_specs[MAIN_SPEC_ID]["static_cols"],
        toy_built_area_cols,
    )
    (
        _,
        toy_main_feature_correlation,
        toy_main_feature_vif,
        _toy_main_max_abs_correlation,
    ) = compute_feature_diagnostics(toy_main_diagnostics_frame)
    toy_main_feature_vif
    return (toy_main_feature_correlation,)


@app.cell
def _(plt, sns, toy_main_feature_correlation):
    toy_correlation_heatmap_figure, toy_correlation_heatmap_axis = plt.subplots(
        figsize=(7, 5)
    )
    sns.heatmap(
        toy_main_feature_correlation,
        annot=True,
        cmap="vlag",
        center=0,
        vmin=-1,
        vmax=1,
        ax=toy_correlation_heatmap_axis,
    )
    toy_correlation_heatmap_axis.set_title(
        "Filtered decayed toy model feature correlation"
    )
    toy_correlation_heatmap_figure.tight_layout()
    toy_correlation_heatmap_figure
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Prediction Checks
    """)
    return


@app.cell
def _(
    choice_neighborhood_features,
    df_transactions_toy,
    np,
    pd,
    predict_toy_choice_shares,
    toy_built_area_cols,
    toy_main_artifact,
):
    toy_main_choice_share_summary = predict_toy_choice_shares(
        toy_main_artifact,
        choice_neighborhood_features,
        df_transactions_toy,
        toy_built_area_cols,
    )
    toy_main_share_error_metrics = pd.DataFrame(
        [
            {
                "share_mae": toy_main_choice_share_summary["abs_share_error"].mean(),
                "share_rmse": np.sqrt(
                    np.square(toy_main_choice_share_summary["share_error"]).mean()
                ),
                "predicted_share_sum": toy_main_choice_share_summary[
                    "predicted_share"
                ].sum(),
                "observed_share_sum": toy_main_choice_share_summary[
                    "observed_share"
                ].sum(),
            }
        ]
    ).round(6)
    toy_main_share_error_metrics
    return (toy_main_choice_share_summary,)


@app.cell
def _(toy_main_choice_share_summary):
    toy_main_choice_share_summary.round(
        {
            "observed_share": 4,
            "predicted_share": 4,
            "share_error": 4,
            "abs_share_error": 4,
        }
    ).head(15)
    return


@app.cell
def _(toy_main_estimated_parameters):
    toy_coefficient_plot_data = toy_main_estimated_parameters.sort_values("Value")
    toy_coefficient_plot_axis = toy_coefficient_plot_data.plot.barh(
        x="feature",
        y="Value",
        xerr="Robust std err.",
        legend=False,
        figsize=(8, 4.5),
    )
    toy_coefficient_plot_axis.axvline(0, color="black", linewidth=0.8)
    toy_coefficient_plot_axis.set_xlabel("Coefficient estimate")
    toy_coefficient_plot_axis.set_ylabel("")
    toy_coefficient_plot_axis.set_title(
        "Filtered decayed jobs + built area coefficients"
    )
    toy_coefficient_plot_axis
    return


if __name__ == "__main__":
    app.run()
