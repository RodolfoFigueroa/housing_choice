import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
from lyra.api import LyraAPIClient


def load_parks(data_path: Path) -> gpd.GeoDataFrame:
    return (
        gpd.read_file(data_path / "initial" / "esp_pub", columns=["TIPO", "Sup_M2"])
        .loc[
            lambda df: df["TIPO"].isin(
                [
                    "JARDIN VECINAL",
                    "PARQUE DE BARRIO",
                    "JARDINES",
                    "JARDINES (VIVERO MPAL)",
                    "PARQUE URBANO",
                ]
            )
        ]
        .assign(
            geometry=lambda df: df["geometry"].force_2d().centroid,
            amenity="Parques recreativos",
        )
        .rename(columns={"Sup_M2": "area"})
        .drop(columns=["TIPO"])
        .to_crs("EPSG:6372")
    )


def calculate_accessibility_jobs(
    df: gpd.GeoDataFrame, client: LyraAPIClient, years: Sequence[int] = [2025]
) -> pd.DataFrame:
    group_patterns = {
        "all": r"^\d{6}",
        "manufacture": r"^(31|32|33)\d{4}",
        "construction": r"^23\d{4}",
        "logistics": r"^(48|49)\d{4}",
        "commerce": r"^(43|46)\d{4}",
        "business_services": r"^(51|52|53|54|55|56)\d{4}",
        "care_education_health": r"^(61|62)\d{4}",
        "local_services": r"^(71|72|81)\d{4}",
        "public_admin": r"^92\d{4}",
    }

    prefixes = [re.findall(r"\d{2}", val) for val in group_patterns.values()]
    prefixes_flat = [item for sublist in prefixes for item in sublist]

    for prefix in prefixes_flat:
        group_patterns[prefix] = rf"^{prefix}\d{{4}}"

    out = []
    for year in years:
        month = 5 if year == 2025 else 11

        response = client.process(
            "accessibility_jobs",
            payload={
                "data": {
                    "data_type": "geojson",
                    "value": json.loads(df[["geometry"]].to_json()),
                },
                "items": {
                    f"{key}_{thresh}": {
                        "pattern": pattern,
                        "edge_weights": "travel_time",
                        "max_weight": thresh * 60,
                        "network_type": "drive",
                    }
                    for key, pattern in group_patterns.items()
                    for thresh in [10, 20]
                },
                "year": year,
                "month": month,
            },
        )
        temp = pd.DataFrame(response["result"]).transpose().assign(year=year)
        temp.columns = [f"{col}_{year}" for col in temp.columns]
        out.append(temp)
    return (
        pd.concat(out, axis=1)
        .reset_index(names="index")
        .assign(index=lambda df: df["index"].astype(int))
        .set_index("index")
    )


def calculate_accessibility_services(
    df: gpd.GeoDataFrame,
    df_park: gpd.GeoDataFrame,
    client: LyraAPIClient,
    *,
    network_type: Literal["walk", "drive"],
    attraction_edge_weights: Literal["length", "travel_time"],
    attraction_max_weight: float,
    accessibility_edge_weights: Literal["length", "travel_time"],
    accessibility_max_weight: float,
) -> pd.DataFrame:
    response = client.process(
        "accessibility_services",
        payload={
            "data": {
                "data_type": "geojson",
                "value": json.loads(df[["geometry"]].to_json()),
            },
            "data_public": json.loads(df_park.to_json()),
            "items": {
                "all": {
                    "network_type": network_type,
                    "attraction_edge_weights": attraction_edge_weights,
                    "attraction_max_weight": attraction_max_weight,
                    "accessibility_edge_weights": accessibility_edge_weights,
                    "accessibility_max_weight": accessibility_max_weight,
                },
            },
        },
    )
    return (
        pd.DataFrame(response["result"])
        .transpose()
        .reset_index(names="index")
        .assign(index=lambda df: df["index"].astype(int))
        .set_index("index")
    )
