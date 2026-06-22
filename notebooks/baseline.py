import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import os
    import warnings
    from pathlib import Path

    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    from housing_choice.modeling import (
        build_availability_choice_dataframe,
        build_structural_baseline_inputs,
        compute_scale_audit,
        fit_biogeme_availability_model,
        predict_availability_choice_shares,
        summarize_availability_by_transaction,
        validate_availability_choice_dataframe,
    )

    warnings.filterwarnings("ignore", category=FutureWarning)


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Structural Baseline Housing Choice Model

    This notebook builds the new baseline for neighborhood-level social-housing choice models. The baseline uses a discrete-choice model with active alternatives, supply/activity controls, and spatial controls, but intentionally excludes job-accessibility variables.
    """)
    return


@app.cell
def _():
    DATA_PATH = Path(os.environ["DATA_PATH"])
    GENERATED_PATH = DATA_PATH / "generated"
    NEIGHBORHOOD_FEATURES_PATH = GENERATED_PATH / "col_final.gpkg"
    TRANSACTIONS_PATH = GENERATED_PATH / "transactions_final.parquet"

    MODELING_YEAR_MIN = 2020
    MODELING_YEAR_MAX = 2025
    ACTIVE_WINDOW_DAYS = 365
    MIN_AVAILABLE_ALTERNATIVES = 2
    MISSING_VALUE_SENTINEL = 99999
    BIOGEME_MODEL_PREFIX = "baseline"

    BASELINE_STATIC_COLS = [
        "accessibility_services_scaled",
        "travel_time_city_center_scaled",
        "travel_time_nearest_crossing_scaled",
        "access_is_restricted",
        "centroid_east_km",
        "centroid_north_km",
    ]
    SUPPLY_FEATURE = "log_active_sales_12m"
    return (
        ACTIVE_WINDOW_DAYS,
        BASELINE_STATIC_COLS,
        BIOGEME_MODEL_PREFIX,
        DATA_PATH,
        MIN_AVAILABLE_ALTERNATIVES,
        MISSING_VALUE_SENTINEL,
        MODELING_YEAR_MAX,
        MODELING_YEAR_MIN,
        SUPPLY_FEATURE,
    )


@app.cell(hide_code=True)
def _(
    ACTIVE_WINDOW_DAYS,
    MIN_AVAILABLE_ALTERNATIVES,
    MODELING_YEAR_MAX,
    MODELING_YEAR_MIN,
):
    mo.md(f"""
    ## Baseline Contract

    The baseline keeps purchases from `{MODELING_YEAR_MIN}` through `{MODELING_YEAR_MAX}` and uses every neighborhood in `col_final.gpkg`. For each transaction, alternatives are neighborhoods with another observed purchase within `{ACTIVE_WINDOW_DAYS}` days of the focal purchase date; the chosen neighborhood is always available. Transactions with fewer than `{MIN_AVAILABLE_ALTERNATIVES}` available alternatives are dropped and reported.

    Utility includes services, travel-time controls, restricted access, transaction-year built area, a recent-activity supply proxy, and centroid coordinate controls. Job-accessibility variables are excluded by design so later job models have a clean measuring stick.
    """)
    return


@app.cell
def _(
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
    df_neighborhood_raw = baseline_inputs.df_neighborhood_raw
    df_transactions_raw = baseline_inputs.df_transactions_raw

    input_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhood_features",
                "path": str(baseline_inputs.neighborhood_features_path),
                "rows": len(df_neighborhood_raw),
                "columns": len(df_neighborhood_raw.columns),
            },
            {
                "artifact": "transactions",
                "path": str(baseline_inputs.transactions_path),
                "rows": len(df_transactions_raw),
                "columns": len(df_transactions_raw.columns),
            },
        ]
    )
    input_summary
    return (baseline_inputs,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Feature Preparation

    Feature cataloging and scaling reuse the project helpers. The only new baseline features are centroid coordinate controls, measured in kilometers from the median modeled neighborhood centroid.
    """)
    return


@app.cell
def _(BASELINE_STATIC_COLS, baseline_inputs):
    feature_catalog = baseline_inputs.feature_catalog
    prepared_neighborhood_features = baseline_inputs.prepared_neighborhood_features
    built_area_cols = baseline_inputs.built_area_cols

    baseline_feature_summary = pd.DataFrame(
        [
            {"feature_group": "static_controls", "features": len(BASELINE_STATIC_COLS)},
            {"feature_group": "supply_activity", "features": 1},
            {"feature_group": "transaction_year_built_area", "features": 1},
            {"feature_group": "job_accessibility", "features": 0},
        ]
    )
    baseline_feature_summary
    return (built_area_cols,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Active Choice Set

    This section replaces the all-neighborhoods-always-available assumption. The recent-activity count is a supply proxy, not a claim about true inventory: it measures observed activity around each transaction date and excludes the focal transaction itself.
    """)
    return


@app.cell
def _(baseline_inputs):
    df_transactions_baseline = baseline_inputs.df_transactions_baseline
    df_transactions_aligned = baseline_inputs.df_transactions_aligned
    choice_neighborhood_features = baseline_inputs.choice_neighborhood_features
    _name_to_idx_map = baseline_inputs.name_to_idx_map
    active_choice_set = baseline_inputs.active_choice_set
    dynamic_alt_features = dict(baseline_inputs.dynamic_alt_features)

    choice_set_summary = baseline_inputs.choice_set_summary.round(3)
    choice_set_summary
    return (
        active_choice_set,
        choice_neighborhood_features,
        dynamic_alt_features,
    )


@app.cell
def _(active_choice_set):
    availability_distribution = summarize_availability_by_transaction(
        active_choice_set.transactions,
    ).round(3)
    availability_distribution
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Baseline Diagnostics

    These checks make sure the active choice set and model-ready frame are coherent before estimation. They also show whether the baseline is still dominated by a small number of high-volume neighborhoods.
    """)
    return


@app.cell
def _(
    BASELINE_STATIC_COLS,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features,
    dynamic_alt_features,
):
    baseline_choice_frame, baseline_model_feature_cols = (
        build_availability_choice_dataframe(
            choice_neighborhood_features,
            active_choice_set.transactions,
            BASELINE_STATIC_COLS,
            built_area_cols,
            active_choice_set.availability,
            dynamic_alt_features,
        )
    )
    baseline_validation = validate_availability_choice_dataframe(
        baseline_choice_frame,
        baseline_model_feature_cols,
        len(choice_neighborhood_features),
        missing_value_sentinel=MISSING_VALUE_SENTINEL,
    )
    baseline_validation
    return baseline_choice_frame, baseline_model_feature_cols


@app.cell
def _(
    active_choice_set,
    baseline_choice_frame,
    baseline_model_feature_cols,
    choice_neighborhood_features,
):
    availability_mask = pd.DataFrame(
        active_choice_set.availability,
        columns=[f"available_{idx}" for idx in choice_neighborhood_features.index],
    ).astype(bool)
    scale_inputs = {}
    for feature in baseline_model_feature_cols:
        feature_values = baseline_choice_frame.loc[
            :,
            [f"{feature}_{idx}" for idx in choice_neighborhood_features.index],
        ]
        scale_inputs[feature] = (
            feature_values.where(availability_mask.to_numpy())
            .melt(value_name=feature)[feature]
            .dropna()
            .reset_index(drop=True)
        )

    baseline_scale_audit = compute_scale_audit(
        pd.DataFrame(scale_inputs),
        baseline_model_feature_cols,
    )
    baseline_scale_audit
    return


@app.cell
def _(active_choice_set):
    transaction_share_summary = (
        active_choice_set.transactions["neighborhood"]
        .value_counts(normalize=True)
        .rename_axis("neighborhood")
        .reset_index(name="observed_share")
        .assign(cumulative_share=lambda df: df["observed_share"].cumsum())
    )

    transaction_concentration = pd.DataFrame(
        [
            {
                "model_transactions": len(active_choice_set.transactions),
                "model_neighborhoods": active_choice_set.transactions[
                    "neighborhood"
                ].nunique(),
                "top_5_share": transaction_share_summary.head(5)[
                    "observed_share"
                ].sum(),
                "top_10_share": transaction_share_summary.head(10)[
                    "observed_share"
                ].sum(),
                "hhi": (transaction_share_summary["observed_share"] ** 2).sum(),
            }
        ]
    ).round(4)
    transaction_concentration
    return (transaction_share_summary,)


@app.cell
def _(transaction_share_summary):
    transaction_share_summary.head(15).round(4)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Baseline Estimation

    The estimator is an availability-aware multinomial logit fit with Biogeme. The first run can take several minutes because Biogeme compiles the larger 95-alternative likelihood.
    """)
    return


@app.cell
def _(
    BASELINE_STATIC_COLS,
    BIOGEME_MODEL_PREFIX,
    MISSING_VALUE_SENTINEL,
    active_choice_set,
    built_area_cols,
    choice_neighborhood_features,
    dynamic_alt_features,
):
    baseline_artifact = fit_biogeme_availability_model(
        "structural_baseline",
        BASELINE_STATIC_COLS,
        choice_neighborhood_features,
        active_choice_set.transactions,
        built_area_cols,
        active_choice_set.availability,
        dynamic_alt_features=dynamic_alt_features,
        model_prefix=BIOGEME_MODEL_PREFIX,
        missing_value_sentinel=MISSING_VALUE_SENTINEL,
        use_jit=False,
    )

    baseline_model_summary = pd.DataFrame([baseline_artifact["summary_row"]]).round(
        {
            "final_log_likelihood": 3,
            "aic": 3,
            "bic": 3,
        }
    )
    baseline_model_summary
    return (baseline_artifact,)


@app.cell
def _(baseline_artifact):
    baseline_coefficient_summary = (
        baseline_artifact["estimated_parameters"]
        .assign(
            value=lambda df: df["Value"].round(4),
            robust_se=lambda df: df["Robust std err."].round(4),
            robust_t=lambda df: df["Robust t-stat."].round(3),
            robust_p=lambda df: df["Robust p-value"].round(4),
        )
        .loc[:, ["feature", "value", "robust_se", "robust_t", "robust_p"]]
        .sort_values("feature")
    )
    baseline_coefficient_summary
    return (baseline_coefficient_summary,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Baseline Interpretation

    This baseline asks what explains observed social-housing purchases after restricting choices to plausibly active neighborhoods. The coefficients below are conditional associations inside that active choice set; they should not be read as direct household preferences.
    """)
    return


@app.cell
def _(baseline_coefficient_summary):
    _coefficient_roles = {
        "access_is_restricted": "access control",
        "accessibility_services_scaled": "service accessibility",
        "centroid_east_km": "spatial control",
        "centroid_north_km": "spatial control",
        "log_active_sales_12m": "supply/activity proxy",
        "log_built_area_ha": "development proxy",
        "travel_time_city_center_scaled": "centrality control",
        "travel_time_nearest_crossing_scaled": "border-access control",
    }
    _coefficient_readings = {
        "access_is_restricted": "Restricted access has little independent signal in this baseline.",
        "accessibility_services_scaled": "Higher service accessibility is negatively associated after supply and spatial controls.",
        "centroid_east_km": "More easterly neighborhoods are more likely to be chosen, conditional on the other controls.",
        "centroid_north_km": "North-south position has weak independent signal here.",
        "log_active_sales_12m": "Recently active neighborhoods are much more likely to be chosen, as expected for a supply/activity proxy.",
        "log_built_area_ha": "More built area in the purchase year is associated with higher choice probability.",
        "travel_time_city_center_scaled": "Farther neighborhoods are less likely to be chosen once active supply and coordinates are controlled.",
        "travel_time_nearest_crossing_scaled": "Nearest-crossing travel time has only marginal evidence in this baseline.",
    }
    _coefficient_cautions = {
        "access_is_restricted": "Could still be correlated with developer, product type, or project age.",
        "accessibility_services_scaled": "Do not interpret as buyers disliking services; this likely reflects residual geography or supply placement.",
        "centroid_east_km": "This is a broad spatial control, not an explanation of why the east matters.",
        "centroid_north_km": "Weak evidence; avoid substantive interpretation.",
        "log_active_sales_12m": "This is observed activity, not true inventory or available units.",
        "log_built_area_ha": "Built area is a broad development proxy, not unit availability.",
        "travel_time_city_center_scaled": "Direction differs from the old model because supply and spatial controls absorb peripheral concentration.",
        "travel_time_nearest_crossing_scaled": "Marginal evidence; treat as a control rather than a finding.",
    }

    coefficient_interpretation = (
        baseline_coefficient_summary.assign(
            role=lambda df: df["feature"].map(_coefficient_roles),
            direction=lambda df: np.select(
                [df["value"].gt(0), df["value"].lt(0)],
                ["positive", "negative"],
                default="zero",
            ),
            evidence=lambda df: np.select(
                [df["robust_p"].lt(0.05), df["robust_p"].lt(0.10)],
                ["clear", "marginal"],
                default="weak",
            ),
            reading=lambda df: df["feature"].map(_coefficient_readings),
            caution=lambda df: df["feature"].map(_coefficient_cautions),
        )
        .loc[:, ["feature", "role", "direction", "evidence", "reading", "caution"]]
        .sort_values(["role", "feature"])
    )
    coefficient_interpretation
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    The core baseline signal is supply and development: recent activity and built area both enter positively. The coordinate controls show remaining spatial structure, while service and travel-time coefficients should be treated as conditional controls rather than standalone preference statements. Employment-access variables are still excluded; they should be tested only as additions to this baseline.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Residual Fit Checks

    The share check compares observed neighborhood transaction shares with shares implied by the fitted active-choice model. Positive share errors are over-predictions; negative share errors are under-predictions. Large errors identify neighborhoods where baseline supply, geography, and service controls still do not explain observed choices.
    """)
    return


@app.cell
def _(baseline_artifact, choice_neighborhood_features):
    baseline_choice_share_summary = predict_availability_choice_shares(
        baseline_artifact,
        choice_neighborhood_features,
    ).round(
        {
            "observed_share": 4,
            "predicted_share": 4,
            "share_error": 4,
            "abs_share_error": 4,
        }
    )
    baseline_choice_share_summary.head(15)
    return (baseline_choice_share_summary,)


@app.cell
def _(baseline_choice_share_summary):
    _residual_columns = [
        "neighborhood",
        "observed_share",
        "predicted_share",
        "share_error",
        "abs_share_error",
    ]
    largest_overpredicted_neighborhoods = (
        baseline_choice_share_summary.sort_values(
            "share_error",
            ascending=False,
        )
        .loc[:, _residual_columns]
        .head(10)
    )
    largest_overpredicted_neighborhoods
    return


@app.cell
def _(baseline_choice_share_summary):
    largest_underpredicted_neighborhoods = (
        baseline_choice_share_summary.sort_values(
            "share_error",
        )
        .loc[
            :,
            [
                "neighborhood",
                "observed_share",
                "predicted_share",
                "share_error",
                "abs_share_error",
            ],
        ]
        .head(10)
    )
    largest_underpredicted_neighborhoods
    return


@app.cell
def _(baseline_choice_share_summary):
    choice_share_plot_data = baseline_choice_share_summary.head(15).sort_values(
        "observed_share",
    )
    choice_share_axis = choice_share_plot_data.plot.barh(
        x="neighborhood",
        y=["observed_share", "predicted_share"],
        figsize=(9, 7),
    )
    choice_share_axis.set_xlabel("Share")
    choice_share_axis.set_ylabel("")
    choice_share_axis.set_title("Structural baseline: observed vs predicted shares")
    plt.tight_layout()
    choice_share_axis
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Baseline Reading

    This notebook establishes the no-jobs structural baseline. Future employment-access models should be judged against this setup: they need to improve residual fit, keep coefficient signs interpretable, and add explanatory value after active alternatives, recent activity, built area, and spatial controls are already included.
    """)
    return


if __name__ == "__main__":
    app.run()
