import marimo

__generated_with = "0.23.9"
app = marimo.App()


@app.cell
def _():
    import os
    import re
    from logging import INFO
    from pathlib import Path

    import ee
    import geemap
    import geopandas as gpd
    import numpy as np
    import osmnx as ox
    import pandas as pd
    import sqlalchemy
    from lyra.api import LyraAPIClient
    from pyproj import CRS

    from housing_choice.funcs import (
        calculate_accessibility_jobs,
        calculate_accessibility_services,
        load_parks,
    )

    return (
        CRS,
        INFO,
        LyraAPIClient,
        Path,
        calculate_accessibility_jobs,
        calculate_accessibility_services,
        ee,
        geemap,
        gpd,
        load_parks,
        np,
        os,
        ox,
        pd,
        re,
        sqlalchemy,
    )


@app.cell
def _(ee):
    ee.Initialize()


@app.cell
def _(Path, os):
    data_path = Path(os.environ["DATA_PATH"])
    return (data_path,)


@app.cell
def _(INFO, LyraAPIClient, os):
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
def _(os, sqlalchemy):
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}",
    )
    return (engine,)


@app.cell
def _(pd):
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

    return (clean_fracc_col,)


@app.cell
def _(clean_fracc_col, data_path, pd):
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


@app.cell
def _(CRS, gpd, pd):
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

    return (merge_and_concat,)


@app.cell
def _(
    clean_fracc_col,
    data_path,
    df_transactions: "pd.DataFrame",
    gpd,
    merge_and_concat,
    pd,
):
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
def _(calculate_accessibility_jobs, client, df_col):
    df_accessibility_jobs = calculate_accessibility_jobs(df_col, client).drop(
        columns=["year_2025"]
    )
    return (df_accessibility_jobs,)


@app.cell
def _(calculate_accessibility_services, client, data_path, df_col, load_parks):
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
def _(engine, gpd):
    with engine.connect() as conn:
        city_center = gpd.read_postgis(
            """
            SELECT geometry FROM centroids_historical
            WHERE cve_met = '02.2.03'
            """,
            conn,
            geom_col="geometry",
        ).to_crs("EPSG:6372")
    return (city_center,)


@app.cell
def _(df_col, ox):
    g = ox.graph_from_bbox(
        df_col.assign(geometry=lambda df: df["geometry"].buffer(5000))
        .to_crs("EPSG:4326")
        .total_bounds,
    )
    g = ox.add_edge_speeds(g)
    g = ox.add_edge_travel_times(g)
    return (g,)


@app.cell
def _(city_center, df_col, g, ox):
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
def _(cent, col_nodes, g, nodes, np, ox, pd):
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
def _(df_col, ee, geemap):
    features = geemap.geopandas_to_ee(
        df_col.set_index("name_detail")[["geometry"]].to_crs("EPSG:4326")
    )

    bbox = ee.Geometry.Rectangle(
        coords=df_col.to_crs("EPSG:4326").total_bounds.tolist()
    )
    return bbox, features


@app.cell
def _(bbox, ee, features, pd):
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
def _(df_final):
    df_final


@app.cell
def _(df_accessibility_jobs, re):
    unwanted_cols_jobs = [
        c for c in df_accessibility_jobs.columns if re.match(r"jobs_\d\d", c)
    ]
    df_accessibility_jobs_filtered = df_accessibility_jobs.drop(
        columns=unwanted_cols_jobs
    )
    return (df_accessibility_jobs_filtered,)


@app.cell
def _(df_accessibility_jobs_filtered):
    df_accessibility_jobs_filtered


@app.cell
def _(
    accessibility_services,
    df_accessibility_jobs_filtered,
    df_areas,
    df_col,
    df_travel_times,
    gpd,
    pd,
):
    df_final = (
        pd.concat([df_col, df_accessibility_jobs_filtered, df_travel_times], axis=1)
        .assign(accessibility_services=accessibility_services)
        .set_index("name_detail")
        .join(df_areas)
        .reset_index()
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=df_col.crs))
    )

    df_final.to_file("./data/processed/col_final.gpkg")
    return (df_final,)


@app.cell
def _(df_col, df_transactions: "pd.DataFrame"):
    df_transactions_final = df_transactions.loc[
        lambda df: df["address"].isin(df_col["name_detail"])
    ]

    df_transactions_final.to_parquet("./data/processed/transactions_final.parquet")


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
