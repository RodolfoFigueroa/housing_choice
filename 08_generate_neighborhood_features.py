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
    import shapely
    import sqlalchemy
    from esda.getisord import G_Local
    from esda.moran import Moran, Moran_Local
    from libpysal.weights import W
    from lyra.api import LyraAPIClient
    from pyproj import CRS

    from housing_choice.funcs import (
        calculate_accessibility_jobs,
        calculate_accessibility_services,
        load_parks,
    )

    ee.Initialize()


@app.cell
def _():
    data_path = Path(os.environ["DATA_PATH"])
    return (data_path,)


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


@app.function
def clean_fracc_col(col: pd.Series) -> pd.Series:
    return (
        col.str.casefold()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(r"fracc(\.|ionamiento)?", "", regex=True)
        .str.replace("desarrollo urbano", "")
        .str.strip()
    )


@app.cell
def _(data_path):
    df_transactions: pd.DataFrame = (
        pd.read_excel(
            data_path
            / "processing"
            / "2"
            / "Analytics - RPPC - Interés Social - 2020 a 2025.xlsx",
            usecols=[
                "Fecha de operación",
                "Inmobiliaria",
                "Valor de operación",
                "Superficie",
                "Categoría",
                "Dirección",
                "Fraccionamiento",
            ],
        )
        .rename(
            columns={
                "Fecha de operación": "purchase_date",
                "Inmobiliaria": "agency",
                "Valor de operación": "price",
                "Superficie": "area_m2",
            }
        )
        .loc[
            lambda df: df["Categoría"].isin(
                ["Compraventa Exe", "Competencia inmobiliaria"]
            )
        ]
        .drop(columns=["Categoría"])
        .dropna(subset=["Dirección"])
        .rename(columns={"Dirección": "address"})
        .assign(
            address=lambda df: (
                clean_fracc_col(df["address"])
                .replace(
                    {
                        "angeles de puebla segunda seccion": "angeles de puebla",
                        "la condesa seccion oleaga ampliacion": "la condesa seccion oleaga",
                    }
                )
                .where(
                    lambda s: ~s.str.startswith("rincones de puebla"),
                    "rincones de puebla",
                )
                .where(
                    lambda s: ~s.str.startswith("mision de puebla"), "mision de puebla"
                )
            )
        )
    )
    return (df_transactions,)


@app.function
def merge_and_concat(
    df: gpd.GeoDataFrame | pd.DataFrame,
    mask: pd.Series,
    *,
    name: str,
    name_detail: str,
    access: str,
    crs: CRS | str,
) -> gpd.GeoDataFrame:
    df_sol = (
        pd.Series(
            {
                "name": name,
                "name_detail": name_detail,
                "geometry": df.loc[mask, "geometry"].union_all(),
                "access": access,
            }
        )
        .to_frame()
        .transpose()
    )
    return pd.concat(
        [
            df.loc[~mask],
            df_sol,
        ],
        ignore_index=True,
    ).pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))


@app.cell
def _(data_path, df_transactions: pd.DataFrame):
    df_col = (
        gpd.read_file(
            data_path / "initial" / "lim_cols_cp",
            columns=["COLONIAS", "Col_Secc", "ACCESO", "geometry"],
        )
        .dropna(subset=["COLONIAS"])
        .rename(
            columns={"COLONIAS": "name", "Col_Secc": "name_detail", "ACCESO": "access"}
        )
        .assign(
            name=lambda df: clean_fracc_col(df["name"]).replace(
                {"condominios villanova": "condominio villanova"}
            ),
            name_detail=lambda df: (
                clean_fracc_col(df["name_detail"])
                .fillna(df["name"])
                .replace({"condominios villanova": "condominio villanova"})
            ),
        )
    )

    crs = df_col.crs
    if crs is None:
        raise ValueError

    # == Parajes de puebla == #

    parajes_mask = df_col["name"] == "parajes de puebla"
    df_parajes = df_col.loc[parajes_mask]

    parajes_first = df_parajes.loc[
        lambda df: df["name_detail"] == "parajes de puebla"
    ].iloc[0]
    parajes_second = pd.Series(
        {
            "name": "parajes de puebla",
            "name_detail": "parajes de puebla segunda seccion",
            "geometry": df_parajes.loc[
                lambda df: df["name_detail"] != "parajes de puebla", "geometry"
            ].union_all(),
            "access": "LIBRE",
        }
    )

    df_parajes = pd.concat(
        [parajes_first, parajes_second], axis=1, ignore_index=True
    ).transpose()

    df_col = pd.concat(
        [
            df_col.loc[~parajes_mask],
            df_parajes,
        ],
        ignore_index=True,
    )

    # == Valle oriente == #
    df_col = merge_and_concat(
        df_col,
        df_col["name"] == "valle oriente",
        name="valle oriente",
        name_detail="valle oriente",
        access="LIBRE",
        crs=crs,
    )

    # == Sol de Puebla == #
    df_col = merge_and_concat(
        df_col,
        df_col["name_detail"] == "sol de puebla",
        name="sol de puebla",
        name_detail="sol de puebla",
        access="LIBRE",
        crs=crs,
    )

    # == Quinta granada == #
    df_col = merge_and_concat(
        df_col,
        df_col["name"] == "quinta granada",
        name="quinta granada",
        name_detail="quinta granada",
        access="RESTRINGIDO",
        crs=crs,
    )

    # == Villa Toledo == #
    df_col = merge_and_concat(
        df_col,
        df_col["name"] == "villa toledo",
        name="villa toledo",
        name_detail="villa toledo",
        access="RESTRINGIDO",
        crs=crs,
    )

    # == Valle de puebla == #
    valle_mask = df_col["name"].str.contains("valle de puebla")
    df_valle = df_col.loc[valle_mask].assign(
        name_detail=lambda df: df["name_detail"].str.replace("etapa", "seccion")
    )

    df_col = pd.concat(
        [
            df_col.loc[~valle_mask],
            df_valle,
        ],
        ignore_index=True,
    )

    df_col = merge_and_concat(
        df_col,
        df_col["name_detail"] == "valle de puebla sexta seccion",
        name="valle de puebla",
        name_detail="valle de puebla sexta seccion",
        access="LIBRE",
        crs=crs,
    )

    # Final

    df_col = (
        df_col.loc[lambda df: df["name_detail"].isin(df_transactions["address"])]
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))
        .to_crs("EPSG:6372")
    )
    return (df_col,)


@app.cell
def mfg_denue_points(df_col, engine):
    mfg_xmin, mfg_ymin, mfg_xmax, mfg_ymax = df_col.buffer(10_000).total_bounds

    MFG_PER_OCU_TO_NUM_WORKERS_MAP = {
        "0 a 5 personas": 3,
        "6 a 10 personas": 8,
        "11 a 30 personas": 20,
        "31 a 50 personas": 40,
        "51 a 100 personas": 75,
        "101 a 250 personas": 175,
        "251 y m?s personas": 500,
    }

    with engine.connect() as _conn:
        mfg_points = gpd.read_postgis(
            """
            SELECT codigo_act, per_ocu, geometry
            FROM denue_2025_05
            WHERE
                codigo_act LIKE ANY (ARRAY['31%%', '32%%', '33%%'])
                AND (geometry && ST_MakeEnvelope(%(xmin)s, %(ymin)s, %(xmax)s, %(ymax)s, 6372))
            """,
            _conn,
            geom_col="geometry",
            params={
                "xmin": int(mfg_xmin),
                "ymin": int(mfg_ymin),
                "xmax": int(mfg_xmax),
                "ymax": int(mfg_ymax),
            },
        )

    mfg_points = mfg_points.assign(
        num_jobs=lambda df: df["per_ocu"].map(MFG_PER_OCU_TO_NUM_WORKERS_MAP)
    ).drop(columns=["per_ocu"])

    mfg_points_summary = pd.DataFrame(
        [
            {
                "businesses": len(mfg_points),
                "jobs": float(mfg_points["num_jobs"].sum()),
                "min_x": mfg_xmin,
                "min_y": mfg_ymin,
                "max_x": mfg_xmax,
                "max_y": mfg_ymax,
            }
        ]
    )

    mfg_points_summary
    return mfg_points, mfg_xmax, mfg_xmin, mfg_ymax, mfg_ymin


@app.cell
def mfg_grid_aggregation(mfg_points, mfg_xmax, mfg_xmin, mfg_ymax, mfg_ymin):
    mfg_grid_size = 250
    mfg_grid_n_cols = int((mfg_xmax - mfg_xmin) / mfg_grid_size) + 1
    mfg_grid_n_rows = int((mfg_ymax - mfg_ymin) / mfg_grid_size) + 1

    _mfg_boxes = [
        shapely.box(
            mfg_xmin + _col * mfg_grid_size,
            mfg_ymin + _row * mfg_grid_size,
            mfg_xmin + (_col + 1) * mfg_grid_size,
            mfg_ymin + (_row + 1) * mfg_grid_size,
        )
        for _col in range(mfg_grid_n_cols)
        for _row in range(mfg_grid_n_rows)
    ]

    _mfg_grid_cells = gpd.GeoDataFrame(
        geometry=_mfg_boxes, crs="EPSG:6372"
    ).reset_index(names="grid_idx")
    _mfg_grid_cells["grid_col"] = _mfg_grid_cells["grid_idx"] // mfg_grid_n_rows
    _mfg_grid_cells["grid_row"] = _mfg_grid_cells["grid_idx"] % mfg_grid_n_rows

    _mfg_joined_points = _mfg_grid_cells.sjoin(
        mfg_points[["num_jobs", "geometry"]],
        how="left",
        predicate="contains",
    )
    _mfg_cell_stats = _mfg_joined_points.groupby("grid_idx", as_index=False).agg(
        num_jobs=("num_jobs", "sum"), num_businesses=("num_jobs", "count")
    )

    mfg_grid = _mfg_grid_cells.merge(_mfg_cell_stats, on="grid_idx", how="left")
    mfg_grid["num_jobs"] = mfg_grid["num_jobs"].fillna(0.0)
    mfg_grid["num_businesses"] = mfg_grid["num_businesses"].fillna(0).astype(int)
    mfg_grid["cell_area_km2"] = mfg_grid.geometry.area / 1_000_000
    mfg_grid["jobs_per_km2"] = mfg_grid["num_jobs"] / mfg_grid["cell_area_km2"]
    mfg_grid["log_jobs"] = np.log1p(mfg_grid["num_jobs"])

    mfg_grid_summary = pd.DataFrame(
        [
            {
                "grid_cells": len(mfg_grid),
                "cells_with_businesses": int(mfg_grid["num_businesses"].gt(0).sum()),
                "businesses": int(mfg_grid["num_businesses"].sum()),
                "jobs": float(mfg_grid["num_jobs"].sum()),
            }
        ]
    )

    mfg_grid_summary
    return mfg_grid, mfg_grid_n_cols, mfg_grid_n_rows


@app.cell
def mfg_spatial_weights(mfg_grid, mfg_grid_n_cols, mfg_grid_n_rows):
    mfg_spatial_permutations = 199
    mfg_spatial_random_seed = 42
    mfg_hotspot_min_jobs = 500
    mfg_hotspot_min_businesses = 2

    _mfg_neighbor_offsets = [
        (_dc, _dr)
        for _dc in (-1, 0, 1)
        for _dr in (-1, 0, 1)
        if not (_dc == 0 and _dr == 0)
    ]

    mfg_weight_neighbors = {}
    for _idx, _col, _row in mfg_grid[["grid_idx", "grid_col", "grid_row"]].itertuples(
        index=False, name=None
    ):
        _neighbor_ids = []
        for _dc, _dr in _mfg_neighbor_offsets:
            _neighbor_col = int(_col) + _dc
            _neighbor_row = int(_row) + _dr
            if (
                0 <= _neighbor_col < mfg_grid_n_cols
                and 0 <= _neighbor_row < mfg_grid_n_rows
            ):
                _neighbor_ids.append(
                    int(_neighbor_col * mfg_grid_n_rows + _neighbor_row)
                )
        mfg_weight_neighbors[int(_idx)] = _neighbor_ids

    mfg_weights = W(mfg_weight_neighbors, silence_warnings=True)
    mfg_weights.transform = "R"

    mfg_weights_binary = W(mfg_weight_neighbors, silence_warnings=True)
    mfg_weights_binary.transform = "B"
    return (
        mfg_hotspot_min_businesses,
        mfg_hotspot_min_jobs,
        mfg_spatial_permutations,
        mfg_spatial_random_seed,
        mfg_weight_neighbors,
        mfg_weights,
        mfg_weights_binary,
    )


@app.cell
def mfg_hotspot_stats(
    mfg_grid,
    mfg_spatial_permutations,
    mfg_spatial_random_seed,
    mfg_weights,
    mfg_weights_binary,
):
    mfg_values = mfg_grid["num_jobs"].to_numpy(dtype=float)

    mfg_moran_jobs = Moran(
        mfg_values, mfg_weights, permutations=mfg_spatial_permutations
    )
    mfg_local_moran_jobs = Moran_Local(
        mfg_values,
        mfg_weights,
        permutations=mfg_spatial_permutations,
        seed=mfg_spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )
    mfg_getis_ord_jobs = G_Local(
        mfg_values,
        mfg_weights_binary,
        transform="B",
        permutations=mfg_spatial_permutations,
        star=True,
        seed=mfg_spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )

    mfg_hotspot_grid = mfg_grid.copy()
    mfg_hotspot_grid["local_moran_i"] = mfg_local_moran_jobs.Is
    mfg_hotspot_grid["local_moran_q"] = mfg_local_moran_jobs.q
    mfg_hotspot_grid["local_moran_p"] = mfg_local_moran_jobs.p_sim
    mfg_hotspot_grid["getis_ord_g"] = mfg_getis_ord_jobs.Gs
    mfg_hotspot_grid["getis_ord_z"] = mfg_getis_ord_jobs.Zs
    mfg_hotspot_grid["getis_ord_p"] = mfg_getis_ord_jobs.p_sim
    mfg_hotspot_grid["is_local_high_high"] = mfg_hotspot_grid["local_moran_q"].eq(
        1
    ) & mfg_hotspot_grid["local_moran_p"].lt(0.05)
    mfg_hotspot_grid["is_gi_hotspot"] = mfg_hotspot_grid["getis_ord_z"].gt(
        1.96
    ) & mfg_hotspot_grid["getis_ord_p"].lt(0.05)
    mfg_hotspot_grid["is_hotspot_candidate"] = mfg_hotspot_grid["is_gi_hotspot"] & (
        mfg_hotspot_grid["num_jobs"].gt(0) | mfg_hotspot_grid["is_local_high_high"]
    )

    mfg_spatial_stats_summary = pd.DataFrame(
        [
            {
                "statistic": "Global Moran's I",
                "value": mfg_moran_jobs.I,
                "expected_value": mfg_moran_jobs.EI,
                "p_sim": mfg_moran_jobs.p_sim,
                "permutations": mfg_spatial_permutations,
            },
            {
                "statistic": "Local Moran high-high cells",
                "value": int(mfg_hotspot_grid["is_local_high_high"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": mfg_spatial_permutations,
            },
            {
                "statistic": "Getis-Ord Gi* hotspot cells",
                "value": int(mfg_hotspot_grid["is_gi_hotspot"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": mfg_spatial_permutations,
            },
            {
                "statistic": "Selected hotspot candidate cells",
                "value": int(mfg_hotspot_grid["is_hotspot_candidate"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": mfg_spatial_permutations,
            },
        ]
    )

    mfg_spatial_stats_summary
    return (mfg_hotspot_grid,)


@app.cell
def mfg_hotspot_clusters(
    mfg_grid,
    mfg_hotspot_grid,
    mfg_hotspot_min_businesses,
    mfg_hotspot_min_jobs,
    mfg_weight_neighbors,
):
    def _assign_connected_components(selected_ids, neighbors):
        component_by_cell = {}
        component_id = 0
        for start in sorted(selected_ids):
            if start in component_by_cell:
                continue
            component_id += 1
            stack = [start]
            component_by_cell[start] = component_id
            while stack:
                cell = stack.pop()
                for neighbor in neighbors[cell]:
                    if neighbor in selected_ids and neighbor not in component_by_cell:
                        component_by_cell[neighbor] = component_id
                        stack.append(neighbor)
        return component_by_cell

    _selected_hotspot_ids = set(
        mfg_hotspot_grid.loc[
            mfg_hotspot_grid["is_hotspot_candidate"], "grid_idx"
        ].astype(int)
    )

    mfg_hotspot_cells = mfg_hotspot_grid.loc[
        mfg_hotspot_grid["is_hotspot_candidate"]
    ].copy()
    if mfg_hotspot_cells.empty:
        mfg_hotspot_cells["cluster_id"] = pd.Series(dtype="Int64")
        mfg_clusters_all = gpd.GeoDataFrame(
            columns=[
                "cluster_id",
                "num_cells",
                "num_jobs",
                "num_businesses",
                "max_getis_ord_z",
                "min_getis_ord_p",
                "max_local_moran_i",
                "area_km2",
                "jobs_per_km2",
                "centroid_x",
                "centroid_y",
                "passes_cluster_threshold",
                "geometry",
            ],
            geometry="geometry",
            crs=mfg_grid.crs,
        )
    else:
        _component_by_cell = _assign_connected_components(
            _selected_hotspot_ids, mfg_weight_neighbors
        )
        mfg_hotspot_cells["cluster_id"] = (
            mfg_hotspot_cells["grid_idx"].astype(int).map(_component_by_cell)
        )
        mfg_clusters_all = (
            mfg_hotspot_cells.dissolve(
                by="cluster_id",
                aggfunc={
                    "grid_idx": "count",
                    "num_jobs": "sum",
                    "num_businesses": "sum",
                    "getis_ord_z": "max",
                    "getis_ord_p": "min",
                    "local_moran_i": "max",
                },
            )
            .rename(
                columns={
                    "grid_idx": "num_cells",
                    "getis_ord_z": "max_getis_ord_z",
                    "getis_ord_p": "min_getis_ord_p",
                    "local_moran_i": "max_local_moran_i",
                }
            )
            .reset_index()
        )
        mfg_clusters_all["area_km2"] = mfg_clusters_all.geometry.area / 1_000_000
        mfg_clusters_all["jobs_per_km2"] = (
            mfg_clusters_all["num_jobs"] / mfg_clusters_all["area_km2"]
        )
        mfg_clusters_all["centroid_x"] = mfg_clusters_all.geometry.centroid.x
        mfg_clusters_all["centroid_y"] = mfg_clusters_all.geometry.centroid.y
        mfg_clusters_all["passes_cluster_threshold"] = mfg_clusters_all["num_jobs"].ge(
            mfg_hotspot_min_jobs
        ) & mfg_clusters_all["num_businesses"].ge(mfg_hotspot_min_businesses)

    mfg_clusters_all = mfg_clusters_all.sort_values(
        ["num_jobs", "area_km2"], ascending=[False, False]
    ).reset_index(drop=True)
    mfg_clusters_all["cluster_rank_all"] = np.arange(1, len(mfg_clusters_all) + 1)

    mfg_clusters = mfg_clusters_all.loc[
        mfg_clusters_all["passes_cluster_threshold"]
    ].copy()
    mfg_clusters = mfg_clusters.sort_values(
        ["num_jobs", "area_km2"], ascending=[False, False]
    ).reset_index(drop=True)
    mfg_clusters["cluster_rank"] = np.arange(1, len(mfg_clusters) + 1)

    mfg_cluster_summary = mfg_clusters[
        [
            "cluster_rank",
            "cluster_id",
            "num_cells",
            "num_jobs",
            "num_businesses",
            "area_km2",
            "jobs_per_km2",
            "max_getis_ord_z",
            "min_getis_ord_p",
            "centroid_x",
            "centroid_y",
        ]
    ].copy()

    mfg_cluster_summary
    return mfg_clusters, mfg_hotspot_cells


@app.cell
def mfg_neighborhood_features(df_col, mfg_clusters):
    _mfg_neighborhood_base = (
        df_col[["name", "name_detail", "geometry"]]
        .copy()
        .reset_index(names="neighborhood_idx")
    )

    mfg_distance_bands_km = [0.5, 1.0, 2.0, 5.0]
    mfg_cluster_model_candidate_cols = [
        "mfg_distance_nearest_cluster_km",
        "log_mfg_jobs_within_2km",
        "log_mfg_cluster_gravity_inv_sq",
    ]

    def _band_suffix(distance_km):
        if float(distance_km).is_integer():
            return f"{int(distance_km)}km"
        return f"{int(distance_km * 1000)}m"

    def _empty_cluster_exposure_record(neighborhood_idx, bands_km):
        record = {
            "neighborhood_idx": neighborhood_idx,
            "nearest_mfg_cluster_id": np.nan,
            "nearest_mfg_cluster_rank": np.nan,
            "nearest_mfg_cluster_jobs": np.nan,
            "nearest_mfg_cluster_businesses": np.nan,
            "nearest_mfg_cluster_jobs_per_km2": np.nan,
            "nearest_mfg_cluster_area_km2": np.nan,
            "distance_to_mfg_cluster_boundary_m": np.nan,
            "distance_to_mfg_cluster_centroid_m": np.nan,
            "mfg_distance_nearest_cluster_km": np.nan,
            "mfg_distance_nearest_cluster_centroid_km": np.nan,
            "intersects_mfg_cluster": False,
            "within_500m_of_mfg_cluster": False,
            "within_1km_of_mfg_cluster": False,
            "within_2km_of_mfg_cluster": False,
            "mfg_cluster_gravity_inv_sq": 0.0,
            "mfg_cluster_gravity_exp_2km": 0.0,
            "log_mfg_cluster_gravity_inv_sq": 0.0,
            "log_mfg_cluster_gravity_exp_2km": 0.0,
        }
        for band_km in bands_km:
            suffix = _band_suffix(band_km)
            record[f"mfg_clusters_within_{suffix}"] = 0
            record[f"mfg_jobs_within_{suffix}"] = 0.0
            record[f"log_mfg_jobs_within_{suffix}"] = 0.0
            record[f"mfg_largest_cluster_jobs_within_{suffix}"] = 0.0
        return record

    def _cluster_exposure_records(neighborhoods, clusters, bands_km):
        if clusters.empty:
            return [
                _empty_cluster_exposure_record(row.neighborhood_idx, bands_km)
                for row in neighborhoods.itertuples()
            ]

        cluster_table = clusters[
            [
                "cluster_id",
                "cluster_rank",
                "num_jobs",
                "num_businesses",
                "jobs_per_km2",
                "area_km2",
                "geometry",
            ]
        ].copy()
        cluster_centroids = cluster_table.geometry.centroid
        cluster_jobs = cluster_table["num_jobs"].to_numpy(dtype=float)

        records = []
        for row in neighborhoods.itertuples():
            boundary_distances_m = cluster_table.geometry.distance(
                row.geometry
            ).to_numpy(dtype=float)
            point = row.geometry.representative_point()
            centroid_distances_m = cluster_centroids.distance(point).to_numpy(
                dtype=float
            )
            nearest_position = int(boundary_distances_m.argmin())
            nearest_cluster = cluster_table.iloc[nearest_position]
            distance_km = boundary_distances_m / 1000

            record = {
                "neighborhood_idx": row.neighborhood_idx,
                "nearest_mfg_cluster_id": nearest_cluster["cluster_id"],
                "nearest_mfg_cluster_rank": nearest_cluster["cluster_rank"],
                "nearest_mfg_cluster_jobs": float(nearest_cluster["num_jobs"]),
                "nearest_mfg_cluster_businesses": int(
                    nearest_cluster["num_businesses"]
                ),
                "nearest_mfg_cluster_jobs_per_km2": float(
                    nearest_cluster["jobs_per_km2"]
                ),
                "nearest_mfg_cluster_area_km2": float(nearest_cluster["area_km2"]),
                "distance_to_mfg_cluster_boundary_m": float(
                    boundary_distances_m[nearest_position]
                ),
                "distance_to_mfg_cluster_centroid_m": float(
                    centroid_distances_m[nearest_position]
                ),
                "mfg_distance_nearest_cluster_km": float(
                    boundary_distances_m[nearest_position] / 1000
                ),
                "mfg_distance_nearest_cluster_centroid_km": float(
                    centroid_distances_m[nearest_position] / 1000
                ),
                "intersects_mfg_cluster": bool(
                    boundary_distances_m[nearest_position] <= 0
                ),
                "within_500m_of_mfg_cluster": bool(
                    boundary_distances_m[nearest_position] <= 500
                ),
                "within_1km_of_mfg_cluster": bool(
                    boundary_distances_m[nearest_position] <= 1_000
                ),
                "within_2km_of_mfg_cluster": bool(
                    boundary_distances_m[nearest_position] <= 2_000
                ),
                "mfg_cluster_gravity_inv_sq": float(
                    (cluster_jobs / np.power(distance_km + 0.25, 2)).sum()
                ),
                "mfg_cluster_gravity_exp_2km": float(
                    (cluster_jobs * np.exp(-distance_km / 2)).sum()
                ),
            }
            record["log_mfg_cluster_gravity_inv_sq"] = float(
                np.log1p(record["mfg_cluster_gravity_inv_sq"])
            )
            record["log_mfg_cluster_gravity_exp_2km"] = float(
                np.log1p(record["mfg_cluster_gravity_exp_2km"])
            )

            for band_km in bands_km:
                suffix = _band_suffix(band_km)
                in_band = distance_km <= band_km
                band_jobs = cluster_jobs[in_band]
                record[f"mfg_clusters_within_{suffix}"] = int(in_band.sum())
                record[f"mfg_jobs_within_{suffix}"] = float(band_jobs.sum())
                record[f"log_mfg_jobs_within_{suffix}"] = float(
                    np.log1p(band_jobs.sum())
                )
                record[f"mfg_largest_cluster_jobs_within_{suffix}"] = (
                    float(band_jobs.max()) if len(band_jobs) else 0.0
                )

            records.append(record)
        return records

    _mfg_exposure_records = _cluster_exposure_records(
        _mfg_neighborhood_base, mfg_clusters, mfg_distance_bands_km
    )
    _mfg_exposure_frame = pd.DataFrame(_mfg_exposure_records)

    mfg_neighborhood_features = _mfg_neighborhood_base.merge(
        _mfg_exposure_frame,
        on="neighborhood_idx",
        how="left",
    )

    mfg_cluster_feature_cols = [
        col
        for col in mfg_neighborhood_features.columns
        if col not in {"neighborhood_idx", "name", "name_detail", "geometry"}
    ]

    mfg_neighborhood_feature_frame = mfg_neighborhood_features.set_index("name_detail")[
        mfg_cluster_feature_cols
    ]

    mfg_neighborhood_feature_summary = mfg_neighborhood_features[
        [
            "name_detail",
            "nearest_mfg_cluster_rank",
            "nearest_mfg_cluster_jobs",
            "mfg_distance_nearest_cluster_km",
            "mfg_jobs_within_2km",
            "log_mfg_jobs_within_2km",
            "mfg_cluster_gravity_inv_sq",
            "log_mfg_cluster_gravity_inv_sq",
            "intersects_mfg_cluster",
            "within_1km_of_mfg_cluster",
        ]
    ].sort_values("mfg_distance_nearest_cluster_km")

    mfg_neighborhood_feature_summary.head(20)
    return (
        mfg_cluster_feature_cols,
        mfg_neighborhood_feature_frame,
        mfg_neighborhood_features,
    )


@app.cell
def mfg_diagnostics_export(mfg_clusters, mfg_hotspot_cells, mfg_hotspot_grid):
    mfg_spatial_diagnostics_output_path = Path(
        "./data/processed/mfg_spatial_diagnostics.gpkg"
    )
    if mfg_spatial_diagnostics_output_path.exists():
        mfg_spatial_diagnostics_output_path.unlink()

    mfg_hotspot_grid.to_file(
        mfg_spatial_diagnostics_output_path,
        layer="mfg_hotspot_grid",
        driver="GPKG",
    )
    mfg_hotspot_cells.to_file(
        mfg_spatial_diagnostics_output_path,
        layer="mfg_hotspot_cells",
        driver="GPKG",
    )
    mfg_clusters.to_file(
        mfg_spatial_diagnostics_output_path,
        layer="mfg_clusters",
        driver="GPKG",
    )

    mfg_spatial_diagnostics_layers = [
        "mfg_hotspot_grid",
        "mfg_hotspot_cells",
        "mfg_clusters",
    ]

    mfg_spatial_diagnostics_output_path
    return (mfg_spatial_diagnostics_output_path,)


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
    mfg_neighborhood_feature_frame,
):
    df_final = (
        pd.concat([df_col, df_accessibility_jobs_filtered, df_travel_times], axis=1)
        .assign(accessibility_services=accessibility_services)
        .set_index("name_detail")
        .join(df_areas)
        .join(mfg_neighborhood_feature_frame)
        .reset_index()
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=df_col.crs))
    )

    df_final.to_file("./data/processed/col_final.gpkg")
    return (df_final,)


@app.cell
def mfg_feature_validation(
    df_col,
    df_final,
    mfg_cluster_feature_cols,
    mfg_neighborhood_features,
    mfg_spatial_diagnostics_output_path,
):
    legacy_mfg_cluster_feature_output_path = Path(
        "./data/processed/mfg_cluster_neighborhood_features.gpkg"
    )
    if legacy_mfg_cluster_feature_output_path.exists():
        legacy_mfg_cluster_feature_output_path.unlink()

    _mfg_monotonic_band_checks = {
        "jobs_1km_ge_500m": (
            df_final["mfg_jobs_within_1km"] >= df_final["mfg_jobs_within_500m"]
        ).all(),
        "jobs_2km_ge_1km": (
            df_final["mfg_jobs_within_2km"] >= df_final["mfg_jobs_within_1km"]
        ).all(),
        "jobs_5km_ge_2km": (
            df_final["mfg_jobs_within_5km"] >= df_final["mfg_jobs_within_2km"]
        ).all(),
    }
    _mfg_missing_feature_cols = sorted(
        set(mfg_cluster_feature_cols) - set(df_final.columns)
    )

    mfg_feature_export_validation = pd.DataFrame(
        [
            {
                "check": "neighborhood_count_matches_df_col",
                "passed": len(mfg_neighborhood_features) == len(df_col),
                "value": len(mfg_neighborhood_features),
            },
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
                "check": "mfg_feature_columns_present",
                "passed": len(_mfg_missing_feature_cols) == 0,
                "value": ", ".join(_mfg_missing_feature_cols),
            },
            {
                "check": "distances_nonnegative",
                "passed": df_final["mfg_distance_nearest_cluster_km"]
                .dropna()
                .ge(0)
                .all(),
                "value": float(df_final["mfg_distance_nearest_cluster_km"].min()),
            },
            *[
                {"check": check, "passed": bool(passed), "value": bool(passed)}
                for check, passed in _mfg_monotonic_band_checks.items()
            ],
            {
                "check": "col_final_written",
                "passed": Path("./data/processed/col_final.gpkg").exists(),
                "value": "./data/processed/col_final.gpkg",
            },
            {
                "check": "diagnostics_written",
                "passed": mfg_spatial_diagnostics_output_path.exists(),
                "value": str(mfg_spatial_diagnostics_output_path),
            },
            {
                "check": "legacy_sidecar_removed",
                "passed": not legacy_mfg_cluster_feature_output_path.exists(),
                "value": str(legacy_mfg_cluster_feature_output_path),
            },
        ]
    )

    mfg_feature_export_validation
    return


@app.cell
def _(df_col, df_transactions: pd.DataFrame):
    df_transactions_final = df_transactions.loc[
        lambda df: df["address"].isin(df_col["name_detail"])
    ]

    df_transactions_final.to_parquet("./data/processed/transactions_final.parquet")
    return


if __name__ == "__main__":
    app.run()
