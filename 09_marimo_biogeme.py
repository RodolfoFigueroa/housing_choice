import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import warnings

    import biogeme.database as db
    import geopandas as gpd
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from biogeme import models
    from biogeme.biogeme import BIOGEME
    from biogeme.expressions import Beta, Variable
    from biogeme.results_processing import get_pandas_estimated_parameters
    from pandas.errors import PerformanceWarning

    return (
        BIOGEME,
        Beta,
        PerformanceWarning,
        Variable,
        db,
        get_pandas_estimated_parameters,
        gpd,
        models,
        np,
        os,
        pd,
        sqlalchemy,
        warnings,
    )


@app.cell
def _(os, sqlalchemy):
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}",
    )
    return (engine,)


@app.cell
def _(engine, gpd):
    with engine.connect() as conn1:
        city_center = gpd.read_postgis(
            """
            SELECT geometry FROM centroids_historical
            WHERE cve_met = '02.2.03'
            """,
            conn1,
            geom_col="geometry",
        ).to_crs("EPSG:6372")


@app.cell
def _(engine, gpd):
    with engine.connect() as conn2:
        city_census_tracts = gpd.read_postgis(
            """
            SELECT census_2020_ageb.cvegeo AS census_tract_code, census_2020_ageb.geometry FROM census_2020_ageb
            INNER JOIN census_2020_mun
                ON census_2020_ageb.cve_mun = census_2020_mun.cvegeo
            INNER JOIN metropoli_2020
                ON census_2020_mun.cve_met = metropoli_2020.cve_met
            WHERE metropoli_2020.cve_met = '02.2.03'
            """,
            conn2,
            geom_col="geometry",
        )


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
    return (
        built_area_cols,
        model_feature_cols,
        static_feature_cols,
        wanted_cols,
    )


@app.cell
def _(gpd):
    gpd.read_file("./data/processed/col_final.gpkg")


@app.cell
def _(gpd, wanted_cols):
    df_neighborhood_features = (
        gpd.read_file("./data/processed/col_final.gpkg")
        .assign(
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
def _(df_neighborhood_features, pd):
    TRANSACTION_THRESH = 20

    _df_transactions_raw = (
        pd.read_parquet(
            "./data/processed/transactions_final.parquet",
        )
        .loc[:, ["address", "purchase_date"]]
        .rename(columns={"address": "neighborhood"})
        .assign(purchase_year=lambda df: pd.to_datetime(df["purchase_date"]).dt.year)
        .loc[lambda df: df["purchase_year"].between(2020, 2025)]
        .drop(columns=["purchase_date"])
    )

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


@app.cell
def _(
    df_neighborhood_features_trimmed,
    df_transactions,
    np,
    pd,
    static_feature_cols,
):
    _built_year_weights = df_transactions["purchase_year"].value_counts(normalize=True)

    df_feature_diagnostics = df_neighborhood_features_trimmed.loc[
        :, static_feature_cols
    ].copy()
    df_feature_diagnostics["log_built_area_ha"] = 0.0
    for _year, _weight in _built_year_weights.items():
        _built_area_col = f"built_area_{int(_year)}"
        df_feature_diagnostics["log_built_area_ha"] += (
            np.log1p(
                df_neighborhood_features_trimmed[_built_area_col]
                .astype(float)
                .div(10_000)
            )
            * _weight
        )

    feature_correlation = df_feature_diagnostics.corr().round(3)

    _feature_vif_rows = []
    _feature_frame = df_feature_diagnostics.astype(float)
    for _feature in _feature_frame.columns:
        _y = _feature_frame[_feature].to_numpy()
        _x = _feature_frame.drop(columns=[_feature]).to_numpy()
        _x = np.column_stack([np.ones(len(_x)), _x])
        _lstsq_result = np.linalg.lstsq(_x, _y, rcond=None)
        _beta = _lstsq_result[0]
        _pred = _x @ _beta
        _ss_res = ((_y - _pred) ** 2).sum()
        _ss_tot = ((_y - _y.mean()) ** 2).sum()
        _r2 = 1 - _ss_res / _ss_tot if _ss_tot else 0
        _feature_vif_rows.append(
            {
                "feature": _feature,
                "vif": 1 / (1 - _r2) if _r2 < 1 else float("inf"),
                "r2": _r2,
            }
        )

    feature_vif = (
        pd.DataFrame(_feature_vif_rows)
        .sort_values("vif", ascending=False)
        .reset_index(drop=True)
        .round({"vif": 3, "r2": 3})
    )

    _feature_corr_abs = feature_correlation.abs().where(
        ~np.eye(len(feature_correlation), dtype=bool)
    )
    max_abs_feature_correlation = round(float(_feature_corr_abs.max().max()), 3)

    feature_correlation, feature_vif


@app.cell
def _(
    PerformanceWarning,
    built_area_cols,
    df_neighborhood_features_trimmed,
    df_transactions,
    model_feature_cols,
    np,
    static_feature_cols,
    warnings,
):
    s = (
        df_neighborhood_features_trimmed.loc[:, static_feature_cols]
        .reset_index(names="index")
        .melt(id_vars="index")
        .assign(index=lambda df: df["index"].astype(str))
        .assign(variable=lambda df: df["variable"] + "_" + df["index"])
        .drop(columns=["index"])
        .set_index("variable")["value"]
        .to_dict()
    )

    _dynamic_feature_values = {}
    for _idx, _row in df_neighborhood_features_trimmed.loc[
        :, built_area_cols
    ].iterrows():
        _area_by_year = {
            int(_col.rsplit("_", maxsplit=1)[1]): float(_row[_col])
            for _col in built_area_cols
        }
        _dynamic_feature_values[f"log_built_area_ha_{_idx}"] = np.log1p(
            df_transactions["purchase_year"]
            .map(_area_by_year)
            .astype(float)
            .div(10_000)
        )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PerformanceWarning)
        df_transactions_augmented = df_transactions.assign(
            **s,
            **_dynamic_feature_values,
        )

    # Defragment DataFrame
    _df_model_columns = [
        "neighborhood",
        "purchase_year",
        *[
            f"{_feature}_{_idx}"
            for _idx in df_neighborhood_features_trimmed.index
            for _feature in model_feature_cols
        ],
    ]
    df_transactions_augmented = df_transactions_augmented.loc[
        :, _df_model_columns
    ].copy()
    return (df_transactions_augmented,)


@app.cell
def _(df_neighborhood_features_trimmed):
    df_neighborhood_features_trimmed


@app.cell
def _(
    BIOGEME,
    Beta,
    Variable,
    db,
    df_neighborhood_features_trimmed,
    df_transactions_augmented,
    model_feature_cols,
    models,
):
    database = db.Database("housing_choice_model", df_transactions_augmented)

    choice = Variable("neighborhood")

    betas = {
        f"beta_{col}": Beta(f"beta_{col}", 0, None, None, 0)
        for col in model_feature_cols
    }

    V = {}
    av = {}
    for i in range(len(df_neighborhood_features_trimmed)):
        var_map = {
            f"var_{col}_{i}": Variable(f"{col}_{i}") for col in model_feature_cols
        }
        V[i] = sum(
            betas[f"beta_{col}"] * var_map[f"var_{col}_{i}"]
            for col in model_feature_cols
        )
        av[i] = 1

    logprob = models.loglogit(V, av, choice)
    biogeme_model = BIOGEME(
        database,
        logprob,
        parameters="./params/test_1.yaml",
        generate_yaml=False,
        generate_html=True,
        save_iterations=False,
    )
    biogeme_model.model_name = "test_1"

    results = biogeme_model.estimate()
    return (results,)


@app.cell
def _(results):
    print(results.short_summary())


@app.cell
def _(get_pandas_estimated_parameters, results):
    estimated_parameters = get_pandas_estimated_parameters(estimation_results=results)
    estimated_parameters
    return (estimated_parameters,)


@app.cell
def _(estimated_parameters):
    estimated_parameters.set_index("Name")["Value"].plot.barh()


@app.cell
def _(built_area_cols, df_transactions, gpd, name_to_idx_map, np, pd):
    best_static_feature_cols = [
        "jobs_manufacture_20_2025",
        "accessibility_services",
        "travel_time_city_center_min",
        "travel_time_nearest_crossing_min",
        "access_is_restricted",
    ]
    best_model_feature_cols = [*best_static_feature_cols, "log_built_area_ha"]

    best_neighborhood_features = (
        gpd.read_file("./data/processed/col_final.gpkg")
        .assign(
            jobs_manufacture_20_2025=lambda df: df["jobs_manufacture_20_2025"].div(
                10_000
            ),
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

    best_neighborhood_names = best_neighborhood_features["name_detail"]
    best_neighborhood_features = best_neighborhood_features.loc[
        :, [*best_static_feature_cols, *built_area_cols]
    ]

    _best_year_weights = df_transactions["purchase_year"].value_counts(normalize=True)
    best_feature_diagnostics = best_neighborhood_features.loc[
        :, best_static_feature_cols
    ].copy()
    best_feature_diagnostics["log_built_area_ha"] = 0.0
    for _year, _weight in _best_year_weights.items():
        best_feature_diagnostics["log_built_area_ha"] += (
            np.log1p(
                best_neighborhood_features[f"built_area_{int(_year)}"]
                .astype(float)
                .div(10_000)
            )
            * _weight
        )

    best_feature_correlation = best_feature_diagnostics.corr().round(3)

    _best_vif_rows = []
    _best_vif_frame = best_feature_diagnostics.astype(float)
    for _feature in _best_vif_frame.columns:
        _y = _best_vif_frame[_feature].to_numpy()
        _x = _best_vif_frame.drop(columns=[_feature]).to_numpy()
        _x = np.column_stack([np.ones(len(_x)), _x])
        _beta = np.linalg.lstsq(_x, _y, rcond=None)[0]
        _pred = _x @ _beta
        _ss_res = ((_y - _pred) ** 2).sum()
        _ss_tot = ((_y - _y.mean()) ** 2).sum()
        _r2 = 1 - _ss_res / _ss_tot if _ss_tot else 0
        _best_vif_rows.append(
            {
                "feature": _feature,
                "vif": 1 / (1 - _r2) if _r2 < 1 else float("inf"),
                "r2": _r2,
            }
        )

    best_feature_vif = (
        pd.DataFrame(_best_vif_rows)
        .sort_values("vif", ascending=False)
        .reset_index(drop=True)
        .round({"vif": 3, "r2": 3})
    )

    _best_corr_abs = best_feature_correlation.abs().where(
        ~np.eye(len(best_feature_correlation), dtype=bool)
    )
    best_max_abs_feature_correlation = round(float(_best_corr_abs.max().max()), 3)

    return (
        best_feature_vif,
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
    np,
    pd,
):
    _best_static_series = (
        best_neighborhood_features.loc[:, best_static_feature_cols]
        .reset_index(names="index")
        .melt(id_vars="index")
        .assign(index=lambda df: df["index"].astype(str))
        .assign(variable=lambda df: df["variable"] + "_" + df["index"])
        .drop(columns=["index"])
        .set_index("variable")["value"]
    )

    _best_dynamic_features = {}
    for _idx, _row in best_neighborhood_features.loc[:, built_area_cols].iterrows():
        _area_by_year = {
            int(_col.rsplit("_", maxsplit=1)[1]): float(_row[_col])
            for _col in built_area_cols
        }
        _best_dynamic_features[f"log_built_area_ha_{_idx}"] = np.log1p(
            df_transactions["purchase_year"]
            .map(_area_by_year)
            .astype(float)
            .div(10_000)
        )

    best_transactions_augmented = pd.concat(
        [
            df_transactions.loc[:, ["neighborhood", "purchase_year"]].reset_index(
                drop=True
            ),
            pd.DataFrame(
                {key: value for key, value in _best_static_series.items()},
                index=df_transactions.index,
            ).reset_index(drop=True),
            pd.DataFrame(_best_dynamic_features).reset_index(drop=True),
        ],
        axis=1,
    )

    _best_model_columns = [
        "neighborhood",
        "purchase_year",
        *[
            f"{_feature}_{_idx}"
            for _idx in best_neighborhood_features.index
            for _feature in best_model_feature_cols
        ],
    ]
    best_transactions_augmented = best_transactions_augmented.loc[
        :, _best_model_columns
    ].copy()

    return (best_transactions_augmented,)


@app.cell
def _(
    BIOGEME,
    Beta,
    Variable,
    best_model_feature_cols,
    best_neighborhood_features,
    best_transactions_augmented,
    db,
    get_pandas_estimated_parameters,
    models,
):
    best_database = db.Database(
        "housing_choice_best_manufacture_20", best_transactions_augmented
    )
    best_choice = Variable("neighborhood")

    best_betas = {
        f"beta_{col}": Beta(f"best_beta_{col}", 0, None, None, 0)
        for col in best_model_feature_cols
    }

    best_V = {}
    best_av = {}
    for _idx in range(len(best_neighborhood_features)):
        _var_map = {
            f"var_{col}_{_idx}": Variable(f"{col}_{_idx}")
            for col in best_model_feature_cols
        }
        best_V[_idx] = sum(
            best_betas[f"beta_{col}"] * _var_map[f"var_{col}_{_idx}"]
            for col in best_model_feature_cols
        )
        best_av[_idx] = 1

    best_logprob = models.loglogit(best_V, best_av, best_choice)
    best_biogeme_model = BIOGEME(
        best_database,
        best_logprob,
        parameters="./params/test_1.yaml",
        generate_yaml=False,
        generate_html=False,
        save_iterations=False,
    )
    best_biogeme_model.model_name = "test_1_best_manufacture_20"

    best_results = best_biogeme_model.estimate()
    best_estimated_parameters = get_pandas_estimated_parameters(
        estimation_results=best_results
    )

    return best_estimated_parameters, best_results


@app.cell
def _(best_estimated_parameters, best_feature_vif, best_results, pd, results):
    best_model_comparison = pd.DataFrame(
        [
            {
                "model": "current_all_jobs_20min",
                "parameters": results.number_of_parameters,
                "sample_size": results.sample_size,
                "final_log_likelihood": results.final_log_likelihood,
                "aic": results.akaike_information_criterion,
                "bic": results.bayesian_information_criterion,
            },
            {
                "model": "best_manufacture_jobs_20min",
                "parameters": best_results.number_of_parameters,
                "sample_size": best_results.sample_size,
                "final_log_likelihood": best_results.final_log_likelihood,
                "aic": best_results.akaike_information_criterion,
                "bic": best_results.bayesian_information_criterion,
            },
        ]
    ).round(
        {
            "final_log_likelihood": 2,
            "aic": 2,
            "bic": 2,
        }
    )

    best_model_comparison["delta_aic_vs_current"] = (
        best_model_comparison["aic"]
        - best_model_comparison.loc[
            best_model_comparison["model"] == "current_all_jobs_20min", "aic"
        ].iloc[0]
    ).round(2)

    best_coefficient_summary = best_estimated_parameters.assign(
        feature=lambda df: df["Name"].str.replace("best_beta_", "", regex=False),
        value=lambda df: df["Value"].round(3),
        robust_se=lambda df: df["Robust std err."].round(3),
        robust_t=lambda df: df["Robust t-stat."].round(3),
        robust_p=lambda df: df["Robust p-value"].round(4),
    ).loc[:, ["feature", "value", "robust_se", "robust_t", "robust_p"]]

    best_model_comparison, best_coefficient_summary, best_feature_vif


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


@app.cell
def _(
    best_estimated_parameters,
    best_model_feature_cols,
    best_neighborhood_features,
    best_neighborhood_names,
    best_transactions_augmented,
    df_transactions,
    np,
    pd,
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


if __name__ == "__main__":
    app.run()
