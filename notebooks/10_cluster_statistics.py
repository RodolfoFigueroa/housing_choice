import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import itertools
    import os
    from pathlib import Path

    import geopandas as gpd
    import marimo as mo
    import pandas as pd
    import sqlalchemy

    from housing_choice.sector_clusters import (
        band_suffix,
        build_sector_cluster_analysis,
        build_semantic_sector_cluster_configs,
        export_sector_cluster_diagnostics,
    )


@app.cell(hide_code=True)
def md_overview():
    mo.md("""
    # Economic-Sector Cluster Statistics

    This notebook builds employment-cluster statistics from DENUE establishments for the semantic sector groups used by the job-accessibility features. It consumes the cleaned neighborhood artifact from `05_clean_neighborhoods.py` and exports a reusable neighborhood-level cluster feature table for `15_generate_neighborhood_features.py`.
    """)
    return


@app.cell(hide_code=True)
def md_setup():
    mo.md("""
    ## Runtime Setup

    Paths, the database engine, and sector-cluster configurations are defined up front. The sector list mirrors the job-accessibility semantic groups, excluding the aggregate `all` group.
    """)
    return


@app.cell
def paths():
    data_path = Path(os.environ["DATA_PATH"])
    generated_path = data_path / "generated"
    processed_path = data_path / "processed"

    neighborhoods_clean_path = generated_path / "neighborhoods_clean.gpkg"
    sector_cluster_feature_output_path = (
        generated_path / "sector_cluster_neighborhood_features.gpkg"
    )
    sector_cluster_config_summary_path = (
        generated_path / "sector_cluster_config_summary.parquet"
    )
    sector_cluster_point_summary_path = (
        generated_path / "sector_cluster_point_summary.parquet"
    )
    sector_cluster_grid_summary_path = (
        generated_path / "sector_cluster_grid_summary.parquet"
    )
    sector_cluster_spatial_stats_summary_path = (
        generated_path / "sector_cluster_spatial_stats_summary.parquet"
    )
    sector_cluster_summary_path = generated_path / "sector_cluster_summary.parquet"
    sector_cluster_neighborhood_feature_summary_path = (
        generated_path / "sector_cluster_neighborhood_feature_summary.parquet"
    )
    sector_cluster_threshold_audit_path = (
        generated_path / "sector_cluster_threshold_audit.parquet"
    )
    return (
        neighborhoods_clean_path,
        processed_path,
        sector_cluster_config_summary_path,
        sector_cluster_feature_output_path,
        sector_cluster_grid_summary_path,
        sector_cluster_neighborhood_feature_summary_path,
        sector_cluster_point_summary_path,
        sector_cluster_spatial_stats_summary_path,
        sector_cluster_summary_path,
        sector_cluster_threshold_audit_path,
    )


@app.cell
def database_engine():
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}",
    )
    return (engine,)


@app.cell
def clean_neighborhood_input(neighborhoods_clean_path):
    df_col = gpd.read_file(neighborhoods_clean_path)

    clean_neighborhood_input_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhoods_clean",
                "path": str(neighborhoods_clean_path),
                "rows": len(df_col),
                "unique_name_detail": df_col["name_detail"].nunique(),
                "crs": df_col.crs.to_string(),
            }
        ]
    )
    clean_neighborhood_input_summary
    return (df_col,)


@app.cell
def sector_cluster_configs(processed_path):
    sector_cluster_configs = build_semantic_sector_cluster_configs(processed_path)
    sector_cluster_config_summary = pd.DataFrame(
        [
            {
                "sector": config.sector_name,
                "output_prefix": config.output_prefix,
                "scian_prefixes": ",".join(config.scian_prefixes),
                "diagnostics_path": str(config.diagnostics_path),
                "denue_table": config.denue_table,
                "crs": config.crs,
                "buffer_m": config.buffer_m,
                "grid_size_m": config.grid_size_m,
                "hotspot_min_jobs": config.hotspot_min_jobs,
                "hotspot_min_businesses": config.hotspot_min_businesses,
                "spatial_permutations": config.spatial_permutations,
                "spatial_random_seed": config.spatial_random_seed,
                "distance_bands_km": ",".join(
                    f"{band:g}" for band in config.distance_bands_km
                ),
            }
            for config in sector_cluster_configs
        ]
    )
    sector_cluster_config_summary
    return sector_cluster_config_summary, sector_cluster_configs


@app.cell(hide_code=True)
def md_cluster_build():
    mo.md("""
    ## Cluster Build

    Each sector is processed with the same grid, hotspot, and exposure pipeline. The shared thresholds keep sectors comparable for the first pass; the audit tables below make it clear where sector-specific threshold tuning may be needed later.
    """)
    return


@app.cell
def sector_cluster_analysis(df_col, engine, sector_cluster_configs):
    sector_cluster_results = [
        build_sector_cluster_analysis(df_col, engine, config)
        for config in sector_cluster_configs
    ]

    sector_cluster_point_summary = pd.concat(
        [result.point_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_point_summary
    return sector_cluster_point_summary, sector_cluster_results


@app.cell
def sector_cluster_summaries(sector_cluster_results):
    sector_cluster_grid_summary = pd.concat(
        [result.grid_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_spatial_stats_summary = pd.concat(
        [result.spatial_stats_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_summary = pd.concat(
        [result.cluster_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_neighborhood_feature_summary = pd.concat(
        [result.neighborhood_feature_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_threshold_audit = pd.DataFrame(
        [
            {
                "sector": result.config.sector_name,
                "output_prefix": result.config.output_prefix,
                "hotspot_min_jobs": result.config.hotspot_min_jobs,
                "hotspot_min_businesses": result.config.hotspot_min_businesses,
                "hotspot_candidate_cells": len(result.hotspot_cells),
                "selected_clusters": len(result.clusters),
                "selected_cluster_jobs": float(result.clusters["num_jobs"].sum()),
                "selected_cluster_businesses": int(
                    result.clusters["num_businesses"].sum()
                ),
            }
            for result in sector_cluster_results
        ]
    )
    sector_cluster_threshold_audit
    return (
        sector_cluster_grid_summary,
        sector_cluster_neighborhood_feature_summary,
        sector_cluster_spatial_stats_summary,
        sector_cluster_summary,
        sector_cluster_threshold_audit,
    )


@app.cell(hide_code=True)
def md_feature_artifact():
    mo.md("""
    ## Neighborhood Feature Artifact

    The exported GeoPackage has one row per cleaned neighborhood and contains only identity columns, geometry, and sector-prefixed cluster exposure columns. Downstream notebooks should read this artifact instead of recomputing clusters.
    """)
    return


@app.cell
def sector_cluster_feature_export(
    df_col,
    sector_cluster_feature_output_path,
    sector_cluster_results,
):
    sector_cluster_neighborhood_feature_frame = pd.concat(
        [result.neighborhood_feature_frame for result in sector_cluster_results],
        axis=1,
    )
    sector_cluster_feature_cols = [
        column
        for result in sector_cluster_results
        for column in result.cluster_feature_cols
    ]
    sector_cluster_neighborhood_features_gdf = gpd.GeoDataFrame(
        df_col[["name", "name_detail", "geometry"]]
        .set_index("name_detail")
        .join(sector_cluster_neighborhood_feature_frame)
        .reset_index()
        .loc[:, ["name", "name_detail", *sector_cluster_feature_cols, "geometry"]],
        geometry="geometry",
        crs=df_col.crs,
    )

    if sector_cluster_feature_output_path.exists():
        sector_cluster_feature_output_path.unlink()
    sector_cluster_neighborhood_features_gdf.to_file(
        sector_cluster_feature_output_path,
        driver="GPKG",
    )

    pd.DataFrame(
        [
            {
                "artifact": "sector_cluster_neighborhood_features",
                "path": str(sector_cluster_feature_output_path),
                "rows": len(sector_cluster_neighborhood_features_gdf),
                "feature_columns": len(sector_cluster_feature_cols),
            }
        ]
    )
    return (sector_cluster_neighborhood_features_gdf,)


@app.cell
def diagnostics_export(sector_cluster_results):
    sector_cluster_diagnostics_paths = {
        result.config.output_prefix: export_sector_cluster_diagnostics(result)
        for result in sector_cluster_results
    }

    sector_cluster_diagnostics_summary = pd.DataFrame(
        [
            {
                "sector": result.config.sector_name,
                "output_prefix": result.config.output_prefix,
                "diagnostics_path": str(
                    sector_cluster_diagnostics_paths[result.config.output_prefix]
                ),
            }
            for result in sector_cluster_results
        ]
    )
    sector_cluster_diagnostics_summary
    return (sector_cluster_diagnostics_paths,)


@app.cell
def summary_artifact_export(
    sector_cluster_config_summary,
    sector_cluster_config_summary_path,
    sector_cluster_grid_summary,
    sector_cluster_grid_summary_path,
    sector_cluster_neighborhood_feature_summary,
    sector_cluster_neighborhood_feature_summary_path,
    sector_cluster_point_summary,
    sector_cluster_point_summary_path,
    sector_cluster_spatial_stats_summary,
    sector_cluster_spatial_stats_summary_path,
    sector_cluster_summary,
    sector_cluster_summary_path,
    sector_cluster_threshold_audit,
    sector_cluster_threshold_audit_path,
):
    sector_cluster_summary_paths = {
        "config": sector_cluster_config_summary_path,
        "points": sector_cluster_point_summary_path,
        "grid": sector_cluster_grid_summary_path,
        "spatial_stats": sector_cluster_spatial_stats_summary_path,
        "clusters": sector_cluster_summary_path,
        "neighborhood_features": sector_cluster_neighborhood_feature_summary_path,
        "threshold_audit": sector_cluster_threshold_audit_path,
    }

    sector_cluster_config_summary.to_parquet(sector_cluster_config_summary_path)
    sector_cluster_point_summary.to_parquet(sector_cluster_point_summary_path)
    sector_cluster_grid_summary.to_parquet(sector_cluster_grid_summary_path)
    sector_cluster_spatial_stats_summary.to_parquet(
        sector_cluster_spatial_stats_summary_path,
    )
    sector_cluster_summary.to_parquet(sector_cluster_summary_path)
    sector_cluster_neighborhood_feature_summary.to_parquet(
        sector_cluster_neighborhood_feature_summary_path,
    )
    sector_cluster_threshold_audit.to_parquet(sector_cluster_threshold_audit_path)

    pd.DataFrame(
        [
            {"artifact": artifact, "path": str(path), "written": path.exists()}
            for artifact, path in sector_cluster_summary_paths.items()
        ]
    )
    return (sector_cluster_summary_paths,)


@app.cell
def export_validation(
    df_col,
    sector_cluster_config_summary,
    sector_cluster_diagnostics_paths,
    sector_cluster_feature_output_path,
    sector_cluster_neighborhood_features_gdf,
    sector_cluster_summary_paths,
):
    expected_prefixes = sector_cluster_config_summary["output_prefix"].tolist()
    missing_prefix_core_columns = []
    for prefix in expected_prefixes:
        required_columns = [
            f"nearest_{prefix}_cluster_jobs",
            f"{prefix}_distance_nearest_cluster_km",
            f"log_{prefix}_jobs_within_2km",
            f"log_{prefix}_cluster_gravity_inv_sq",
        ]
        missing_prefix_core_columns.extend(
            column
            for column in required_columns
            if column not in sector_cluster_neighborhood_features_gdf.columns
        )

    validation_rows = [
        {
            "check": "feature_artifact_written",
            "passed": sector_cluster_feature_output_path.exists(),
            "value": str(sector_cluster_feature_output_path),
        },
        {
            "check": "feature_rows_match_clean_neighborhoods",
            "passed": len(sector_cluster_neighborhood_features_gdf) == len(df_col),
            "value": len(sector_cluster_neighborhood_features_gdf),
        },
        {
            "check": "name_detail_unique",
            "passed": sector_cluster_neighborhood_features_gdf["name_detail"].is_unique,
            "value": int(
                sector_cluster_neighborhood_features_gdf["name_detail"].nunique()
            ),
        },
        {
            "check": "all_expected_sector_core_columns_present",
            "passed": len(missing_prefix_core_columns) == 0,
            "value": ", ".join(missing_prefix_core_columns),
        },
        {
            "check": "diagnostics_paths_written",
            "passed": all(
                path.exists() for path in sector_cluster_diagnostics_paths.values()
            ),
            "value": len(sector_cluster_diagnostics_paths),
        },
        {
            "check": "summary_parquets_written",
            "passed": all(
                path.exists() for path in sector_cluster_summary_paths.values()
            ),
            "value": len(sector_cluster_summary_paths),
        },
    ]

    for prefix, bands_string in sector_cluster_config_summary[
        ["output_prefix", "distance_bands_km"]
    ].itertuples(index=False, name=None):
        distance_col = f"{prefix}_distance_nearest_cluster_km"
        distances = sector_cluster_neighborhood_features_gdf[distance_col].dropna()
        validation_rows.append(
            {
                "check": f"{prefix}_distances_nonnegative",
                "passed": distances.ge(0).all(),
                "value": float(distances.min()) if not distances.empty else None,
            }
        )
        bands = tuple(float(value) for value in bands_string.split(","))
        for lower_band, upper_band in itertools.pairwise(bands):
            lower_col = f"{prefix}_jobs_within_{band_suffix(lower_band)}"
            upper_col = f"{prefix}_jobs_within_{band_suffix(upper_band)}"
            validation_rows.append(
                {
                    "check": f"{prefix}_{upper_col}_ge_{lower_col}",
                    "passed": bool(
                        (
                            sector_cluster_neighborhood_features_gdf[upper_col]
                            >= sector_cluster_neighborhood_features_gdf[lower_col]
                        ).all()
                    ),
                    "value": bool(
                        (
                            sector_cluster_neighborhood_features_gdf[upper_col]
                            >= sector_cluster_neighborhood_features_gdf[lower_col]
                        ).all()
                    ),
                }
            )

    sector_cluster_export_validation = pd.DataFrame(validation_rows)
    sector_cluster_export_validation
    return


if __name__ == "__main__":
    app.run()
