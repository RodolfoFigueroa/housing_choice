import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import os
    import re
    from logging import INFO
    from pathlib import Path

    import ee
    import geemap
    import geopandas as gpd
    import marimo as mo
    import numpy as np
    import osmnx as ox
    import pandas as pd
    import sqlalchemy
    from lyra.api import LyraAPIClient

    from housing_choice.funcs import (
        calculate_accessibility_jobs,
        calculate_accessibility_services,
        load_parks,
    )
    from housing_choice.sector_clusters import (
        LOGISTICS_CLUSTER_CONFIG,
        MANUFACTURING_CLUSTER_CONFIG,
        band_suffix,
        build_sector_cluster_analysis,
        export_sector_cluster_diagnostics,
    )

    ee.Initialize()


@app.cell(hide_code=True)
def md_overview():
    mo.md("""
    # Neighborhood feature build

    This notebook assembles the canonical modeling feature table from upstream artifacts. Cleaned neighborhood geometries and cleaned transaction names now come from `07_clean_neighborhoods.py`; this notebook adds cluster, accessibility, travel-time, and built-area features before writing `col_final.gpkg` and `transactions_final.parquet`.
    """)
    return


@app.cell(hide_code=True)
def md_setup():
    mo.md("""
    ## Runtime setup

    Imports, environment-derived paths, service clients, and database connections are defined first. These cells should stay small and reusable because later feature sections depend on them.
    """)
    return


@app.cell
def _():
    data_path = Path(os.environ["DATA_PATH"])
    generated_path = data_path / "generated"
    return data_path, generated_path


@app.cell
def _():
    LYRA_HOST = os.environ["LYRA_HOST"]
    # LYRA_HOST = "localhost:5219"

    client = LyraAPIClient(
        host=LYRA_HOST,
        log_level=INFO,
        secure="localhost" not in LYRA_HOST,
        headers={
            "P-Access-Token-Id": os.environ["PANGOLIN_ACCESS_TOKEN_ID"],
            "P-Access-Token": os.environ["PANGOLIN_ACCESS_TOKEN"],
        },
    )
    return (client,)


@app.cell
def _():
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}",
    )
    return (engine,)


@app.cell(hide_code=True)
def md_neighborhood_geometry():
    mo.md("""
    ## Cleaned Neighborhood Inputs

    Neighborhood name normalization, manual geometry corrections, transaction address cleaning, and CRS projection are handled upstream in `07_clean_neighborhoods.py`. This section reads those stable artifacts so downstream feature calculations share the same neighborhood universe.
    """)
    return


@app.cell(hide_code=True)
def md_cleaning_artifacts():
    mo.md("""
    `07_clean_neighborhoods.py` exports `neighborhoods_clean.gpkg` and `transactions_clean.parquet` under the generated data directory. Re-run that notebook when raw neighborhood names, manual corrections, or transaction name cleaning rules change.
    """)
    return


@app.cell(hide_code=True)
def md_transactions_input():
    mo.md("""
    ## Clean Transaction Input

    The transaction artifact contains cleaned address names and original transaction attributes. The final export at the end of this notebook still filters purchases to neighborhoods retained in the feature table.
    """)
    return


@app.cell
def _(generated_path):
    transactions_clean_path = generated_path / "transactions_clean.parquet"
    df_transactions = pd.read_parquet(transactions_clean_path)

    transactions_input_summary = pd.DataFrame(
        [
            {
                "artifact": "transactions_clean",
                "path": str(transactions_clean_path),
                "rows": len(df_transactions),
                "unique_addresses": df_transactions["address"].nunique(),
                "min_purchase_date": df_transactions["purchase_date"].min(),
                "max_purchase_date": df_transactions["purchase_date"].max(),
            }
        ]
    )
    transactions_input_summary
    return (df_transactions,)


@app.cell(hide_code=True)
def md_neighborhood_artifact():
    mo.md("""
    The cleaned neighborhood artifact is projected to `EPSG:6372`, has one row per retained neighborhood, and preserves the `name`, `name_detail`, `access`, and `geometry` columns expected by the feature pipeline.
    """)
    return


@app.cell
def _(df_transactions, generated_path):
    neighborhoods_clean_path = generated_path / "neighborhoods_clean.gpkg"
    df_col = gpd.read_file(neighborhoods_clean_path)

    clean_neighborhood_input_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhoods_clean",
                "path": str(neighborhoods_clean_path),
                "rows": len(df_col),
                "unique_name_detail": df_col["name_detail"].nunique(),
                "crs": df_col.crs.to_string(),
                "all_names_in_transactions": bool(
                    df_col["name_detail"].isin(df_transactions["address"]).all()
                ),
            }
        ]
    )
    clean_neighborhood_input_summary
    return (df_col,)


@app.cell(hide_code=True)
def md_sector_cluster_features():
    mo.md("""
    ## Economic-sector cluster features

    This section derives neighborhood exposure to employment clusters from DENUE establishments. The shared sector-cluster pipeline is run for manufacturing and logistics, and the resulting neighborhood-level features are written directly into `col_final.gpkg`.
    """)
    return


@app.cell
def sector_cluster_analysis(df_col, engine):
    mfg_cluster_analysis = build_sector_cluster_analysis(
        df_col,
        engine,
        MANUFACTURING_CLUSTER_CONFIG,
    )
    logistics_cluster_analysis = build_sector_cluster_analysis(
        df_col,
        engine,
        LOGISTICS_CLUSTER_CONFIG,
    )
    sector_cluster_results = [mfg_cluster_analysis, logistics_cluster_analysis]

    sector_cluster_point_summary = pd.concat(
        [result.point_summary for result in sector_cluster_results],
        ignore_index=True,
    )
    sector_cluster_point_summary
    return (sector_cluster_results,)


@app.cell(hide_code=True)
def md_sector_cluster_outputs():
    mo.md("""
    ### Cluster features and diagnostics

    For each sector, hotspot cells are dissolved into ranked clusters and translated into neighborhood exposure measures: proximity, overlap, jobs within distance bands, and gravity-style exposure. Diagnostic layers are exported separately for map review and sanity checks.
    """)
    return


@app.cell
def sector_cluster_neighborhood_features(sector_cluster_results):
    sector_cluster_neighborhood_feature_frame = pd.concat(
        [result.neighborhood_feature_frame for result in sector_cluster_results],
        axis=1,
    )
    sector_cluster_feature_cols = [
        column
        for result in sector_cluster_results
        for column in result.cluster_feature_cols
    ]
    sector_cluster_neighborhood_features = {
        result.config.output_prefix: result.neighborhood_features
        for result in sector_cluster_results
    }
    sector_cluster_summary = pd.concat(
        [result.cluster_summary for result in sector_cluster_results],
        ignore_index=True,
    )

    def _tidy_neighborhood_feature_summary(result):
        prefix = result.config.output_prefix
        return (
            result.neighborhood_features[
                [
                    "name_detail",
                    f"nearest_{prefix}_cluster_rank",
                    f"nearest_{prefix}_cluster_jobs",
                    f"{prefix}_distance_nearest_cluster_km",
                    f"{prefix}_jobs_within_2km",
                    f"log_{prefix}_jobs_within_2km",
                    f"{prefix}_cluster_gravity_inv_sq",
                    f"log_{prefix}_cluster_gravity_inv_sq",
                    f"intersects_{prefix}_cluster",
                    f"within_1km_of_{prefix}_cluster",
                ]
            ]
            .rename(
                columns={
                    f"nearest_{prefix}_cluster_rank": "nearest_cluster_rank",
                    f"nearest_{prefix}_cluster_jobs": "nearest_cluster_jobs",
                    f"{prefix}_distance_nearest_cluster_km": "distance_nearest_cluster_km",
                    f"{prefix}_jobs_within_2km": "jobs_within_2km",
                    f"log_{prefix}_jobs_within_2km": "log_jobs_within_2km",
                    f"{prefix}_cluster_gravity_inv_sq": "cluster_gravity_inv_sq",
                    f"log_{prefix}_cluster_gravity_inv_sq": "log_cluster_gravity_inv_sq",
                    f"intersects_{prefix}_cluster": "intersects_cluster",
                    f"within_1km_of_{prefix}_cluster": "within_1km_of_cluster",
                }
            )
            .assign(sector=result.config.sector_name)
            .loc[
                :,
                [
                    "sector",
                    "name_detail",
                    "nearest_cluster_rank",
                    "nearest_cluster_jobs",
                    "distance_nearest_cluster_km",
                    "jobs_within_2km",
                    "log_jobs_within_2km",
                    "cluster_gravity_inv_sq",
                    "log_cluster_gravity_inv_sq",
                    "intersects_cluster",
                    "within_1km_of_cluster",
                ],
            ]
        )

    sector_cluster_feature_summary = pd.concat(
        [
            _tidy_neighborhood_feature_summary(result)
            for result in sector_cluster_results
        ],
        ignore_index=True,
    ).sort_values(["sector", "distance_nearest_cluster_km"])

    sector_cluster_summary
    return (
        sector_cluster_feature_cols,
        sector_cluster_neighborhood_feature_frame,
        sector_cluster_neighborhood_features,
    )


@app.cell
def sector_cluster_diagnostics_export(sector_cluster_results):
    sector_cluster_diagnostics_paths = {
        result.config.output_prefix: export_sector_cluster_diagnostics(result)
        for result in sector_cluster_results
    }

    pd.DataFrame(
        [
            {"sector": prefix, "diagnostics_path": str(path)}
            for prefix, path in sector_cluster_diagnostics_paths.items()
        ]
    )
    return (sector_cluster_diagnostics_paths,)


@app.cell(hide_code=True)
def md_accessibility():
    mo.md("""
    ## Accessibility features

    Accessibility features summarize access to jobs and parks/services using the Lyra routing service and project helper functions. Cached cells are intentionally used here because these service calls are expensive and deterministic for a fixed input geometry set.
    """)
    return


@app.cell
def _(client, df_col):
    with mo.persistent_cache("accessibility_jobs"):
        df_accessibility_jobs = calculate_accessibility_jobs(df_col, client).drop(
            columns=["year_2025"]
        )
    return (df_accessibility_jobs,)


@app.cell
def _(client, data_path, df_col):
    df_park = load_parks(data_path)

    accessibility_services = calculate_accessibility_services(
        df_col,
        df_park,
        client,
        network_type="drive",
        attraction_edge_weights="length",
        attraction_max_weight=1000,
        accessibility_edge_weights="length",
        accessibility_max_weight=1000,
    )["accessibility_all"]
    return (accessibility_services,)


@app.cell(hide_code=True)
def md_travel_times():
    mo.md("""
    ## Travel-time anchors

    These cells compute travel times from each neighborhood to major reference points: the metropolitan center and the east/west border crossings. The OSMnx road graph is built around the neighborhood extent, then shortest paths are evaluated from neighborhood centroids to each anchor.
    """)
    return


@app.cell
def _(engine):
    with engine.connect() as _conn:
        city_center = gpd.read_postgis(
            """
            SELECT geometry FROM centroids_historical
            WHERE cve_met = '02.2.03'
            """,
            _conn,
            geom_col="geometry",
        ).to_crs("EPSG:6372")
    return (city_center,)


@app.cell
def _(df_col):
    g = ox.graph_from_bbox(
        df_col.assign(geometry=lambda df: df["geometry"].buffer(5000))
        .to_crs("EPSG:4326")
        .total_bounds,
    )
    g = ox.add_edge_speeds(g)
    g = ox.add_edge_travel_times(g)
    return (g,)


@app.cell
def _(city_center, df_col, g):
    cent = df_col.centroid.to_crs("EPSG:4326")
    col_nodes = ox.nearest_nodes(g, cent.x, cent.y)

    city_center_node = ox.nearest_nodes(
        g,
        city_center.to_crs("EPSG:4326")["geometry"].x.iloc[0],
        city_center.to_crs("EPSG:4326")["geometry"].y.iloc[0],
    )

    crossing_coords = [
        (32.66487765887405, -115.49637151372004),
        (32.67263745662977, -115.38776736117538),
    ]

    nodes = [city_center_node] + [
        ox.nearest_nodes(g, lon, lat) for lat, lon in crossing_coords
    ]
    return cent, col_nodes, nodes


@app.cell
def _(cent, col_nodes, g, nodes):
    travel_times = []
    for name, node in zip(
        ["city_center", "crossing_west", "crossing_east"], nodes, strict=True
    ):
        print(f"{name}: {node}")
        shortest_paths = ox.shortest_path(
            g,
            col_nodes,
            [node] * len(col_nodes),
            weight="travel_time",
            cpus=8,
        )
        travel_times.append(
            [
                ox.routing.route_to_gdf(g, path, weight="travel_time")[
                    "travel_time"
                ].sum()
                if path is not None
                else np.nan
                for path in shortest_paths
            ]
        )

    df_travel_times = pd.DataFrame(
        zip(*travel_times, strict=True),
        columns=[
            "travel_time_city_center",
            "travel_time_crossing_west",
            "travel_time_crossing_east",
        ],
        index=cent.index,
    )
    return (df_travel_times,)


@app.cell(hide_code=True)
def md_built_area():
    mo.md("""
    ## Built-area history

    Google Dynamic World built-surface estimates are reduced over each neighborhood for 2020 through 2025. These columns capture recent physical development intensity and are joined into the final neighborhood feature table.
    """)
    return


@app.cell
def _(df_col):
    features = geemap.geopandas_to_ee(
        df_col.set_index("name_detail")[["geometry"]].to_crs("EPSG:4326")
    )

    bbox = ee.Geometry.Rectangle(
        coords=df_col.to_crs("EPSG:4326").total_bounds.tolist()
    )
    return bbox, features


@app.cell
def _(bbox, features):
    areas = []

    for year in range(2020, 2026):
        img: ee.Image = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterBounds(bbox)
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .select("built")
            .mean()
            .multiply(ee.Image.pixelArea())
        )

        res = ee.data.computeFeatures(
            {
                "expression": img.reduceRegions(
                    collection=features, reducer=ee.Reducer.sum(), scale=10, tileScale=4
                ),
                "fileFormat": "GEOPANDAS_GEODATAFRAME",
            }
        ).set_index("name_detail")["sum"]

        areas.append(pd.Series(res, name=f"built_area_{year}"))

    df_areas = pd.concat(areas, axis=1)
    return (df_areas,)


@app.cell(hide_code=True)
def md_final_export():
    mo.md("""
    ## Canonical neighborhood export

    The final neighborhood table combines cleaned geometries, accessibility features, travel times, built-area history, and sector-cluster exposure. This section writes `col_final.gpkg`, which should be the only source used by later notebooks for neighborhood-level features.
    """)
    return


@app.cell
def _(df_accessibility_jobs):
    unwanted_cols_jobs = [
        c for c in df_accessibility_jobs.columns if re.match(r"jobs_\d\d", c)
    ]
    df_accessibility_jobs_filtered = df_accessibility_jobs.drop(
        columns=unwanted_cols_jobs
    )
    return (df_accessibility_jobs_filtered,)


@app.cell
def _(
    accessibility_services,
    df_accessibility_jobs_filtered,
    df_areas,
    df_col,
    df_travel_times,
    generated_path,
    sector_cluster_neighborhood_feature_frame,
):
    df_final = (
        pd.concat([df_col, df_accessibility_jobs_filtered, df_travel_times], axis=1)
        .assign(accessibility_services=accessibility_services)
        .set_index("name_detail")
        .join(df_areas)
        .join(sector_cluster_neighborhood_feature_frame)
        .reset_index()
        .pipe(
            lambda frame: gpd.GeoDataFrame(frame, geometry="geometry", crs=df_col.crs)
        )
    )

    df_final.to_file(generated_path / "col_final.gpkg")
    return (df_final,)


@app.cell(hide_code=True)
def md_validation():
    mo.md("""
    ## Export validation

    The validation cell checks row counts, uniqueness, sector-cluster feature presence, distance sanity, monotonic distance-band totals, diagnostics output, and removal of the legacy manufacturing sidecar. It is meant to make the feature-export contract visible before downstream analysis notebooks consume the outputs.
    """)
    return


@app.cell
def sector_cluster_feature_validation(
    df_col,
    df_final,
    generated_path,
    sector_cluster_diagnostics_paths,
    sector_cluster_feature_cols,
    sector_cluster_neighborhood_features,
    sector_cluster_results,
):
    legacy_mfg_cluster_feature_output_path = Path(
        generated_path / "mfg_cluster_neighborhood_features.gpkg"
    )
    if legacy_mfg_cluster_feature_output_path.exists():
        legacy_mfg_cluster_feature_output_path.unlink()

    _missing_feature_cols = sorted(
        set(sector_cluster_feature_cols) - set(df_final.columns)
    )
    _validation_rows = [
        {
            "check": "df_final_count_matches_df_col",
            "passed": len(df_final) == len(df_col),
            "value": len(df_final),
        },
        {
            "check": "name_detail_unique",
            "passed": df_final["name_detail"].is_unique,
            "value": int(df_final["name_detail"].nunique()),
        },
        {
            "check": "sector_cluster_feature_columns_present",
            "passed": len(_missing_feature_cols) == 0,
            "value": ", ".join(_missing_feature_cols),
        },
        {
            "check": "col_final_written",
            "passed": (generated_path / "col_final.gpkg").exists(),
            "value": str(generated_path / "col_final.gpkg"),
        },
    ]

    for _result in sector_cluster_results:
        _prefix = _result.config.output_prefix
        _features = sector_cluster_neighborhood_features[_prefix]
        _distance_col = f"{_prefix}_distance_nearest_cluster_km"
        _distances = df_final[_distance_col].dropna()
        _validation_rows.extend(
            [
                {
                    "check": f"{_prefix}_neighborhood_count_matches_df_col",
                    "passed": len(_features) == len(df_col),
                    "value": len(_features),
                },
                {
                    "check": f"{_prefix}_distances_nonnegative",
                    "passed": _distances.ge(0).all(),
                    "value": float(_distances.min())
                    if not _distances.empty
                    else np.nan,
                },
                {
                    "check": f"{_prefix}_diagnostics_written",
                    "passed": sector_cluster_diagnostics_paths[_prefix].exists(),
                    "value": str(sector_cluster_diagnostics_paths[_prefix]),
                },
            ]
        )
        for _lower_band, _upper_band in zip(
            _result.config.distance_bands_km,
            _result.config.distance_bands_km[1:],
            strict=False,
        ):
            _lower_col = f"{_prefix}_jobs_within_{band_suffix(_lower_band)}"
            _upper_col = f"{_prefix}_jobs_within_{band_suffix(_upper_band)}"
            _validation_rows.append(
                {
                    "check": f"{_prefix}_{_upper_col}_ge_{_lower_col}",
                    "passed": bool(
                        (df_final[_upper_col] >= df_final[_lower_col]).all()
                    ),
                    "value": bool((df_final[_upper_col] >= df_final[_lower_col]).all()),
                }
            )

    _validation_rows.append(
        {
            "check": "legacy_sidecar_removed",
            "passed": not legacy_mfg_cluster_feature_output_path.exists(),
            "value": str(legacy_mfg_cluster_feature_output_path),
        }
    )

    sector_cluster_feature_export_validation = pd.DataFrame(_validation_rows)
    sector_cluster_feature_export_validation
    return


@app.cell(hide_code=True)
def md_transactions_export():
    mo.md("""
    ## Transaction export

    The final transaction artifact keeps only purchases whose address matches a retained neighborhood. This preserves alignment between purchase records and the canonical neighborhood feature table.
    """)
    return


@app.cell
def _(df_col, df_transactions, generated_path):
    df_transactions_final = df_transactions.loc[
        lambda df: df["address"].isin(df_col["name_detail"])
    ]

    df_transactions_final.to_parquet(generated_path / "transactions_final.parquet")
    return


if __name__ == "__main__":
    app.run()
