from pathlib import Path

import pandas as pd

import dagster as dg
from housing_choice.defs.resources import PathResource


def _clean_fracc_col(column: pd.Series) -> pd.Series:
    return (
        column.str.casefold()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(r"fracc(\.|ionamiento)?", "", regex=True)
        .str.replace("desarrollo urbano", "")
        .str.strip()
    )


@dg.asset(
    key=["transactions_clean"],
    io_manager_key="dataframe_manager",
    group_name="transactions",
)
def transactions_clean(path_resource: PathResource) -> pd.DataFrame:
    in_path = Path(path_resource.in_path)
    raw_transactions_path = (
        in_path / "Analytics - RPPC - Interés Social - 2020 a 2025.xlsx"
    )
    raw_transactions = pd.read_excel(
        raw_transactions_path,
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

    return (
        raw_transactions.rename(
            columns={
                "Fecha de operación": "purchase_date",
                "Inmobiliaria": "agency",
                "Valor de operación": "price",
                "Superficie": "area_m2",
            },
        )
        .loc[
            lambda frame: frame["Categoría"].isin(
                ("Compraventa Exe", "Competencia inmobiliaria")
            )
        ]
        .drop(columns=["Categoría"])
        .dropna(subset=["Dirección"])
        .rename(columns={"Dirección": "address"})
        .assign(
            address=lambda frame: (
                _clean_fracc_col(frame["address"])
                .replace(
                    {
                        "angeles de puebla segunda seccion": "angeles de puebla",
                        "la condesa seccion oleaga ampliacion": (
                            "la condesa seccion oleaga"
                        ),
                    },
                )
                .where(
                    lambda series: ~series.str.startswith("rincones de puebla"),
                    "rincones de puebla",
                )
                .where(
                    lambda series: ~series.str.startswith("mision de puebla"),
                    "mision de puebla",
                )
            ),
        )
    )
