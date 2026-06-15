import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from pathlib import Path

    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import shapely
    import sqlalchemy
    from esda.getisord import G_Local
    from esda.moran import Moran, Moran_Local
    from libpysal.weights import W

    return (
        G_Local,
        Moran,
        Moran_Local,
        Path,
        W,
        gpd,
        np,
        os,
        pd,
        plt,
        shapely,
        sqlalchemy,
    )


@app.cell
def _(os, sqlalchemy):
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return (engine,)


@app.cell
def _(Path, os):
    data_path = Path(os.environ["DATA_PATH"])


@app.cell
def _(gpd):
    df_col = gpd.read_file("./data/processed/col_final.gpkg").to_crs("EPSG:6372")
    xmin, ymin, xmax, ymax = df_col.buffer(10_000).total_bounds
    return df_col, xmax, xmin, ymax, ymin


@app.cell
def _(engine, gpd, xmax, xmin, ymax, ymin):
    PER_OCU_TO_NUM_WORKERS_MAP = {
        "0 a 5 personas": 3,
        "6 a 10 personas": 8,
        "11 a 30 personas": 20,
        "31 a 50 personas": 40,
        "51 a 100 personas": 75,
        "101 a 250 personas": 175,
        "251 y más personas": 500,
    }

    with engine.connect() as conn:
        df_points = gpd.read_postgis(
            """
            SELECT codigo_act, per_ocu, geometry
            FROM denue_2025_05
            WHERE 
                codigo_act LIKE ANY (ARRAY['31%%', '32%%', '33%%'])
                AND (geometry && ST_MakeEnvelope(%(xmin)s, %(ymin)s, %(xmax)s, %(ymax)s, 6372))
            """,
            conn,
            geom_col="geometry",
            params={
                "xmin": int(xmin),
                "ymin": int(ymin),
                "xmax": int(xmax),
                "ymax": int(ymax),
            },
        )

    df_points = df_points.assign(
        num_jobs=lambda df: df["per_ocu"].map(PER_OCU_TO_NUM_WORKERS_MAP)
    ).drop(columns=["per_ocu"])
    return (df_points,)


@app.cell
def _(gpd, shapely, xmax, xmin, ymax, ymin):
    grid_size = 250
    boxes = [
        shapely.box(
            xmin + i * grid_size,
            ymin + j * grid_size,
            xmin + (i + 1) * grid_size,
            ymin + (j + 1) * grid_size,
        )
        for i in range(int((xmax - xmin) / grid_size) + 1)
        for j in range(int((ymax - ymin) / grid_size) + 1)
    ]

    df_grid = gpd.GeoDataFrame(geometry=boxes, crs="EPSG:6372")
    return df_grid, grid_size


@app.cell
def _(df_points):
    df_points.plot(column="num_jobs", scheme="quantiles", s=2)


@app.cell
def _(df_grid, df_points, grid_size, np, xmax, xmin, ymax, ymin):
    _grid_cells = df_grid[["geometry"]].reset_index(names="grid_idx").copy()
    grid_n_cols = int((xmax - xmin) / grid_size) + 1
    grid_n_rows = int((ymax - ymin) / grid_size) + 1
    _grid_cells["grid_col"] = _grid_cells["grid_idx"] // grid_n_rows
    _grid_cells["grid_row"] = _grid_cells["grid_idx"] % grid_n_rows

    _joined_points = _grid_cells.sjoin(
        df_points[["num_jobs", "geometry"]],
        how="left",
        predicate="contains",
    )
    _cell_stats = _joined_points.groupby("grid_idx", as_index=False).agg(
        num_jobs=("num_jobs", "sum"), num_businesses=("num_jobs", "count")
    )

    mfg_grid = _grid_cells.merge(_cell_stats, on="grid_idx", how="left")
    mfg_grid["num_jobs"] = mfg_grid["num_jobs"].fillna(0.0)
    mfg_grid["num_businesses"] = mfg_grid["num_businesses"].fillna(0).astype(int)
    mfg_grid["cell_area_km2"] = mfg_grid.geometry.area / 1_000_000
    mfg_grid["jobs_per_km2"] = mfg_grid["num_jobs"] / mfg_grid["cell_area_km2"]
    mfg_grid["log_jobs"] = np.log1p(mfg_grid["num_jobs"])

    joined = mfg_grid
    return grid_n_cols, grid_n_rows, mfg_grid


@app.cell
def _(df_col, mfg_grid, np, plt):
    _plot_grid = mfg_grid.copy()
    _plot_grid["plot_log_jobs"] = _plot_grid["log_jobs"].where(
        _plot_grid["num_jobs"].gt(0), np.nan
    )

    mfg_grid_jobs_fig, _ax = plt.subplots(figsize=(8, 8))
    _plot_grid.plot(
        column="plot_log_jobs",
        cmap="magma",
        legend=True,
        ax=_ax,
        missing_kwds={"color": "#f2f2f2", "label": "0 jobs"},
    )
    df_col.boundary.plot(ax=_ax, color="white", linewidth=0.4)
    _ax.set_title("Manufacturing jobs by 250 m grid cell")
    _ax.set_axis_off()
    mfg_grid_jobs_fig


@app.cell
def _(W, grid_n_cols, grid_n_rows, mfg_grid):
    spatial_permutations = 199
    spatial_random_seed = 42
    hotspot_min_jobs = 500
    hotspot_min_businesses = 2

    _offsets = [
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
        for _dc, _dr in _offsets:
            _neighbor_col = int(_col) + _dc
            _neighbor_row = int(_row) + _dr
            if 0 <= _neighbor_col < grid_n_cols and 0 <= _neighbor_row < grid_n_rows:
                _neighbor_ids.append(int(_neighbor_col * grid_n_rows + _neighbor_row))
        mfg_weight_neighbors[int(_idx)] = _neighbor_ids

    mfg_weights = W(mfg_weight_neighbors, silence_warnings=True)
    mfg_weights.transform = "R"

    mfg_weights_binary = W(mfg_weight_neighbors, silence_warnings=True)
    mfg_weights_binary.transform = "B"
    return (
        hotspot_min_businesses,
        hotspot_min_jobs,
        mfg_weight_neighbors,
        mfg_weights,
        mfg_weights_binary,
        spatial_permutations,
        spatial_random_seed,
    )


@app.cell
def _(
    G_Local,
    Moran,
    Moran_Local,
    mfg_grid,
    mfg_weights,
    mfg_weights_binary,
    np,
    pd,
    spatial_permutations,
    spatial_random_seed,
):
    mfg_values = mfg_grid["num_jobs"].to_numpy(dtype=float)

    moran_jobs = Moran(mfg_values, mfg_weights, permutations=spatial_permutations)
    local_moran_jobs = Moran_Local(
        mfg_values,
        mfg_weights,
        permutations=spatial_permutations,
        seed=spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )
    getis_ord_jobs = G_Local(
        mfg_values,
        mfg_weights_binary,
        transform="B",
        permutations=spatial_permutations,
        star=True,
        seed=spatial_random_seed,
        n_jobs=1,
        keep_simulations=False,
    )

    mfg_hotspot_grid = mfg_grid.copy()
    mfg_hotspot_grid["local_moran_i"] = local_moran_jobs.Is
    mfg_hotspot_grid["local_moran_q"] = local_moran_jobs.q
    mfg_hotspot_grid["local_moran_p"] = local_moran_jobs.p_sim
    mfg_hotspot_grid["getis_ord_g"] = getis_ord_jobs.Gs
    mfg_hotspot_grid["getis_ord_z"] = getis_ord_jobs.Zs
    mfg_hotspot_grid["getis_ord_p"] = getis_ord_jobs.p_sim
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
                "value": moran_jobs.I,
                "expected_value": moran_jobs.EI,
                "p_sim": moran_jobs.p_sim,
                "permutations": spatial_permutations,
            },
            {
                "statistic": "Local Moran high-high cells",
                "value": int(mfg_hotspot_grid["is_local_high_high"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": spatial_permutations,
            },
            {
                "statistic": "Getis-Ord Gi* hotspot cells",
                "value": int(mfg_hotspot_grid["is_gi_hotspot"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": spatial_permutations,
            },
            {
                "statistic": "Selected hotspot candidate cells",
                "value": int(mfg_hotspot_grid["is_hotspot_candidate"].sum()),
                "expected_value": np.nan,
                "p_sim": np.nan,
                "permutations": spatial_permutations,
            },
        ]
    )

    mfg_spatial_stats_summary
    return (mfg_hotspot_grid,)


@app.cell
def _(
    gpd,
    hotspot_min_businesses,
    hotspot_min_jobs,
    mfg_grid,
    mfg_hotspot_grid,
    mfg_weight_neighbors,
    np,
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
            hotspot_min_jobs
        ) & mfg_clusters_all["num_businesses"].ge(hotspot_min_businesses)

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
def _(df_col, mfg_clusters, np, pd):
    _neighborhood_base = (
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

    _exposure_records = _cluster_exposure_records(
        _neighborhood_base, mfg_clusters, mfg_distance_bands_km
    )
    _exposure_frame = pd.DataFrame(_exposure_records)

    mfg_neighborhood_features = _neighborhood_base.merge(
        _exposure_frame,
        on="neighborhood_idx",
        how="left",
    )

    mfg_cluster_feature_cols = [
        col
        for col in mfg_neighborhood_features.columns
        if col not in {"neighborhood_idx", "name", "name_detail", "geometry"}
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
    return (mfg_neighborhood_features,)


@app.cell
def _(df_col, mfg_hotspot_cells, mfg_hotspot_grid, plt):
    mfg_getis_ord_fig, _axes = plt.subplots(1, 2, figsize=(14, 7))

    mfg_hotspot_grid.plot(
        column="getis_ord_z",
        cmap="coolwarm",
        legend=True,
        ax=_axes[0],
    )
    df_col.boundary.plot(ax=_axes[0], color="black", linewidth=0.3)
    _axes[0].set_title("Getis-Ord Gi* z-score")
    _axes[0].set_axis_off()

    mfg_hotspot_grid.plot(color="#f2f2f2", ax=_axes[1], linewidth=0)
    mfg_hotspot_grid.loc[mfg_hotspot_grid["is_gi_hotspot"]].plot(
        ax=_axes[1], color="#fdae61", linewidth=0
    )
    mfg_hotspot_cells.plot(ax=_axes[1], color="#d7191c", linewidth=0)
    df_col.boundary.plot(ax=_axes[1], color="black", linewidth=0.3)
    _axes[1].set_title("Gi* hotspots and selected candidate cells")
    _axes[1].set_axis_off()

    mfg_getis_ord_fig.tight_layout()
    mfg_getis_ord_fig


@app.cell
def _(
    df_col,
    mfg_clusters,
    mfg_grid,
    mfg_hotspot_cells,
    mfg_hotspot_grid,
    plt,
):
    mfg_cluster_fig, _ax = plt.subplots(figsize=(9, 9))

    mfg_hotspot_grid.plot(color="#f4f4f4", ax=_ax, linewidth=0)
    mfg_grid.loc[mfg_grid["num_jobs"].gt(0)].plot(
        column="log_jobs",
        cmap="Greys",
        ax=_ax,
        alpha=0.55,
        linewidth=0,
    )
    if not mfg_hotspot_cells.empty:
        mfg_hotspot_cells.boundary.plot(ax=_ax, color="#fdae61", linewidth=0.35)
    if not mfg_clusters.empty:
        mfg_clusters.plot(
            column="cluster_rank",
            cmap="tab20",
            ax=_ax,
            alpha=0.75,
            edgecolor="black",
            linewidth=0.8,
        )
        for _cluster in mfg_clusters.itertuples():
            _point = _cluster.geometry.representative_point()
            _ax.text(
                _point.x,
                _point.y,
                str(int(_cluster.cluster_rank)),
                ha="center",
                va="center",
                fontsize=8,
                weight="bold",
            )
    df_col.boundary.plot(ax=_ax, color="black", linewidth=0.35)
    _ax.set_title("Final manufacturing clusters")
    _ax.set_axis_off()
    mfg_cluster_fig


@app.cell
def _(
    Path,
    df_col,
    mfg_clusters,
    mfg_hotspot_cells,
    mfg_neighborhood_features,
    pd,
):
    mfg_cluster_feature_output_path = Path(
        "./data/processed/mfg_cluster_neighborhood_features.gpkg"
    )
    if mfg_cluster_feature_output_path.exists():
        mfg_cluster_feature_output_path.unlink()

    mfg_neighborhood_features.to_file(
        mfg_cluster_feature_output_path,
        layer="neighborhood_features",
        driver="GPKG",
    )
    mfg_clusters.to_file(
        mfg_cluster_feature_output_path,
        layer="mfg_clusters",
        driver="GPKG",
    )
    mfg_hotspot_cells.to_file(
        mfg_cluster_feature_output_path,
        layer="mfg_hotspot_cells",
        driver="GPKG",
    )

    _monotonic_band_checks = {
        "jobs_1km_ge_500m": (
            mfg_neighborhood_features["mfg_jobs_within_1km"]
            >= mfg_neighborhood_features["mfg_jobs_within_500m"]
        ).all(),
        "jobs_2km_ge_1km": (
            mfg_neighborhood_features["mfg_jobs_within_2km"]
            >= mfg_neighborhood_features["mfg_jobs_within_1km"]
        ).all(),
        "jobs_5km_ge_2km": (
            mfg_neighborhood_features["mfg_jobs_within_5km"]
            >= mfg_neighborhood_features["mfg_jobs_within_2km"]
        ).all(),
    }

    mfg_cluster_export_validation = pd.DataFrame(
        [
            {
                "check": "neighborhood_count_matches_df_col",
                "passed": len(mfg_neighborhood_features) == len(df_col),
                "value": len(mfg_neighborhood_features),
            },
            {
                "check": "name_detail_unique",
                "passed": mfg_neighborhood_features["name_detail"].is_unique,
                "value": int(mfg_neighborhood_features["name_detail"].nunique()),
            },
            {
                "check": "distances_nonnegative",
                "passed": mfg_neighborhood_features["mfg_distance_nearest_cluster_km"]
                .dropna()
                .ge(0)
                .all(),
                "value": float(
                    mfg_neighborhood_features["mfg_distance_nearest_cluster_km"].min()
                ),
            },
            *[
                {"check": check, "passed": bool(passed), "value": bool(passed)}
                for check, passed in _monotonic_band_checks.items()
            ],
            {
                "check": "output_file_written",
                "passed": mfg_cluster_feature_output_path.exists(),
                "value": str(mfg_cluster_feature_output_path),
            },
        ]
    )

    mfg_cluster_export_validation


if __name__ == "__main__":
    app.run()
