# Data Lineage

## Clean Neighborhoods And Transactions

`notebooks/05_clean_neighborhoods.py` creates the shared neighborhood and
transaction artifacts.

Input transaction records come from the social-housing workbook loaded by
`notebooks/05_clean_neighborhoods.py` under `DATA_PATH/processing/2`. The
notebook keeps purchase categories `Compraventa Exe` and
`Competencia inmobiliaria`, normalizes address strings, and exports
`DATA_PATH/generated/transactions_clean.parquet`.

Input neighborhood geometries come from `DATA_PATH/initial/lim_cols_cp`. The
notebook normalizes names, applies known manual geometry/name corrections,
filters to neighborhoods observed in the cleaned transaction addresses,
projects to `EPSG:6372`, and exports
`DATA_PATH/generated/neighborhoods_clean.gpkg`.

This filtering is a core modeling caveat: the retained neighborhood universe is
defined by observed social-housing transactions, not by every possible
residential neighborhood in the city.

## Sector Cluster Features

`notebooks/10_cluster_statistics.py` builds sector-cluster exposure artifacts
from DENUE establishments. It reads `neighborhoods_clean.gpkg`, queries the
`denue_2025_05` table, groups establishments by semantic SCIAN prefixes, maps
`per_ocu` employment ranges to approximate workers, and aggregates jobs onto a
250 meter grid.

The cluster pipeline detects spatial concentration using global Moran's I,
local Moran high-high cells, and Getis-Ord Gi* hotspot cells. Adjacent hotspot
cells become clusters, and clusters must pass the configured minimum job and
business thresholds. The exported neighborhood-level artifact is
`DATA_PATH/generated/sector_cluster_neighborhood_features.gpkg`; companion
parquet files store configuration, point, grid, spatial-statistic, cluster,
neighborhood-feature, and threshold-audit summaries.

## Neighborhood Feature Export

`notebooks/15_generate_neighborhood_features.py` joins the canonical feature
table.

The notebook starts from `neighborhoods_clean.gpkg` and
`transactions_clean.parquet`, then adds:

- Lyra job-accessibility features.
- Lyra service-accessibility features.
- OSMnx travel times to the metropolitan center and two border crossings.
- Google Dynamic World built-area history for 2020 through 2025.
- Precomputed sector-cluster exposure features.

The final neighborhood artifact is `DATA_PATH/generated/col_final.gpkg`. The
final transaction artifact is `DATA_PATH/generated/transactions_final.parquet`,
filtered to purchases whose cleaned address is in the retained neighborhood
table.

## Feature Catalog

The helper `housing_choice.modeling.build_feature_catalog` provides source
metadata that the generated documentation projects into feature families,
prepared column names, derivation notes, scale denominators, and feature
descriptions.

The generated raw catalog is available at
`docs/generated/feature-catalog.md`. Regenerate it after rebuilding
`col_final.gpkg` or changing the feature catalog helper.

## External Provenance

Lyra is an external service. This project pins the Lyra API and SDK client in
`uv.lock` to commit `57a6810e19d7dded33ee06a9a2c2996e472bcddf`, but the metric
logic lives in the separate `lyra-plugins` repository.

This documentation uses `lyra-plugins` commit
`2995f4605a6f1ce123517a260ed1ad018944152f` as the source for the
`accessibility_jobs` and `accessibility_services` metric definitions. Future
feature exports should record the deployed plugin commit used by `LYRA_HOST` so
the generated data can be tied to an exact metric implementation.
