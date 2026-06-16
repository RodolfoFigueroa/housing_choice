import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def md_overview(mo):
    mo.md("""
    # Clean Neighborhoods

    This notebook prepares the shared neighborhood geometry and transaction-name artifacts used by downstream feature and cluster notebooks. It owns name normalization, manual geometry corrections, transaction address cleaning, and the residential-neighborhood universe definition.
    """)
    return


@app.cell
def _():
    import os
    from pathlib import Path

    import geopandas as gpd
    import marimo as mo
    import pandas as pd
    from pyproj import CRS

    data_path = Path(os.environ["DATA_PATH"])
    generated_path = data_path / "generated"
    neighborhoods_clean_path = generated_path / "neighborhoods_clean.gpkg"
    transactions_clean_path = generated_path / "transactions_clean.parquet"
    return (
        CRS,
        data_path,
        gpd,
        mo,
        neighborhoods_clean_path,
        pd,
        transactions_clean_path,
    )


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


@app.cell(hide_code=True)
def md_transactions(mo):
    mo.md("""
    ## Transaction Names

    The transaction workbook defines the residential naming universe. Addresses are normalized with the same text-cleaning rules used for neighborhood names, plus known replacements for recurring transaction-name variants.
    """)
    return


@app.cell
def _(clean_fracc_col, data_path, pd):
    df_transactions_raw = pd.read_excel(
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

    df_transactions_clean = (
        df_transactions_raw.rename(
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
                    lambda s: ~s.str.startswith("mision de puebla"),
                    "mision de puebla",
                )
            )
        )
    )

    transaction_cleaning_summary = pd.DataFrame(
        [
            {
                "raw_rows": len(df_transactions_raw),
                "clean_rows": len(df_transactions_clean),
                "unique_clean_addresses": df_transactions_clean["address"].nunique(),
                "min_purchase_date": df_transactions_clean["purchase_date"].min(),
                "max_purchase_date": df_transactions_clean["purchase_date"].max(),
            }
        ]
    )
    transaction_cleaning_summary
    return (df_transactions_clean,)


@app.cell(hide_code=True)
def md_neighborhoods(mo):
    mo.md("""
    ## Neighborhood Geometry Normalization

    The source neighborhood layer is normalized to the cleaned transaction naming convention, corrected for known geometry/name issues, filtered to neighborhoods observed in transactions, and projected to `EPSG:6372` for metric spatial analysis.
    """)
    return


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
        ).pipe(lambda frame: gpd.GeoDataFrame(frame, geometry="geometry", crs=crs))

    return (merge_and_concat,)


@app.cell
def _(
    clean_fracc_col,
    data_path,
    df_transactions_clean,
    gpd,
    merge_and_concat,
    pd,
):
    df_neighborhoods_raw = (
        gpd.read_file(
            data_path / "initial" / "lim_cols_cp",
            columns=["COLONIAS", "Col_Secc", "ACCESO", "geometry"],
        )
        .dropna(subset=["COLONIAS"])
        .rename(
            columns={"COLONIAS": "name", "Col_Secc": "name_detail", "ACCESO": "access"}
        )
    )

    _df_col = df_neighborhoods_raw.assign(
        name=lambda df: clean_fracc_col(df["name"]).replace(
            {"condominios villanova": "condominio villanova"}
        ),
        name_detail=lambda df: (
            clean_fracc_col(df["name_detail"])
            .fillna(df["name"])
            .replace({"condominios villanova": "condominio villanova"})
        ),
    )

    source_crs = _df_col.crs
    if source_crs is None:
        raise ValueError("Neighborhood source CRS is missing")

    # == Parajes de puebla == #
    parajes_mask = _df_col["name"] == "parajes de puebla"
    df_parajes = _df_col.loc[parajes_mask]
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
    _df_col = pd.concat([_df_col.loc[~parajes_mask], df_parajes], ignore_index=True)

    # == Valle oriente == #
    _df_col = merge_and_concat(
        _df_col,
        _df_col["name"] == "valle oriente",
        name="valle oriente",
        name_detail="valle oriente",
        access="LIBRE",
        crs=source_crs,
    )

    # == Sol de Puebla == #
    _df_col = merge_and_concat(
        _df_col,
        _df_col["name_detail"] == "sol de puebla",
        name="sol de puebla",
        name_detail="sol de puebla",
        access="LIBRE",
        crs=source_crs,
    )

    # == Quinta granada == #
    _df_col = merge_and_concat(
        _df_col,
        _df_col["name"] == "quinta granada",
        name="quinta granada",
        name_detail="quinta granada",
        access="RESTRINGIDO",
        crs=source_crs,
    )

    # == Villa Toledo == #
    _df_col = merge_and_concat(
        _df_col,
        _df_col["name"] == "villa toledo",
        name="villa toledo",
        name_detail="villa toledo",
        access="RESTRINGIDO",
        crs=source_crs,
    )

    # == Valle de puebla == #
    valle_mask = _df_col["name"].str.contains("valle de puebla")
    df_valle = _df_col.loc[valle_mask].assign(
        name_detail=lambda df: df["name_detail"].str.replace("etapa", "seccion")
    )
    _df_col = pd.concat([_df_col.loc[~valle_mask], df_valle], ignore_index=True)
    _df_col = merge_and_concat(
        _df_col,
        _df_col["name_detail"] == "valle de puebla sexta seccion",
        name="valle de puebla",
        name_detail="valle de puebla sexta seccion",
        access="LIBRE",
        crs=source_crs,
    )

    available_neighborhood_names = set(_df_col["name_detail"])
    unmatched_transaction_addresses = sorted(
        set(df_transactions_clean["address"]) - available_neighborhood_names
    )

    df_neighborhoods_clean = (
        _df_col.loc[lambda df: df["name_detail"].isin(df_transactions_clean["address"])]
        .pipe(
            lambda frame: gpd.GeoDataFrame(frame, geometry="geometry", crs=source_crs)
        )
        .to_crs("EPSG:6372")
        .reset_index(drop=True)
    )

    neighborhood_cleaning_summary = pd.DataFrame(
        [
            {
                "raw_neighborhood_rows": len(df_neighborhoods_raw),
                "normalized_rows_before_filter": len(_df_col),
                "clean_neighborhood_rows": len(df_neighborhoods_clean),
                "unique_clean_names": df_neighborhoods_clean["name_detail"].nunique(),
                "unmatched_transaction_addresses": len(unmatched_transaction_addresses),
            }
        ]
    )
    neighborhood_cleaning_summary
    return df_neighborhoods_clean, unmatched_transaction_addresses


@app.cell(hide_code=True)
def md_validation(mo):
    mo.md("""
    ## Validation And Export

    The checks below make the cleaned-neighborhood contract explicit before downstream notebooks consume these artifacts.
    """)
    return


@app.cell
def _(
    df_neighborhoods_clean,
    df_transactions_clean,
    pd,
    unmatched_transaction_addresses,
):
    neighborhood_validation = pd.DataFrame(
        [
            {
                "check": "name_detail_unique",
                "passed": bool(df_neighborhoods_clean["name_detail"].is_unique),
                "value": int(df_neighborhoods_clean["name_detail"].nunique()),
            },
            {
                "check": "projected_crs_epsg_6372",
                "passed": bool(df_neighborhoods_clean.crs.to_string() == "EPSG:6372"),
                "value": df_neighborhoods_clean.crs.to_string(),
            },
            {
                "check": "non_empty_geometries",
                "passed": bool(
                    df_neighborhoods_clean.geometry.notna().all()
                    and (~df_neighborhoods_clean.geometry.is_empty).all()
                ),
                "value": int((~df_neighborhoods_clean.geometry.is_empty).sum()),
            },
            {
                "check": "all_neighborhoods_have_transactions",
                "passed": bool(
                    df_neighborhoods_clean["name_detail"]
                    .isin(df_transactions_clean["address"])
                    .all()
                ),
                "value": int(
                    df_neighborhoods_clean["name_detail"]
                    .isin(df_transactions_clean["address"])
                    .sum()
                ),
            },
            {
                "check": "unmatched_transaction_addresses_visible",
                "passed": True,
                "value": len(unmatched_transaction_addresses),
            },
        ]
    )
    neighborhood_validation
    return


@app.cell
def _(
    df_neighborhoods_clean,
    df_transactions_clean,
    neighborhoods_clean_path,
    pd,
    transactions_clean_path,
):
    if neighborhoods_clean_path.exists():
        neighborhoods_clean_path.unlink()

    df_neighborhoods_clean.to_file(neighborhoods_clean_path)
    df_transactions_clean.to_parquet(transactions_clean_path)

    clean_artifact_summary = pd.DataFrame(
        [
            {
                "artifact": "neighborhoods_clean",
                "path": str(neighborhoods_clean_path),
                "rows": len(df_neighborhoods_clean),
                "exists": neighborhoods_clean_path.exists(),
            },
            {
                "artifact": "transactions_clean",
                "path": str(transactions_clean_path),
                "rows": len(df_transactions_clean),
                "exists": transactions_clean_path.exists(),
            },
        ]
    )
    clean_artifact_summary
    return


if __name__ == "__main__":
    app.run()
