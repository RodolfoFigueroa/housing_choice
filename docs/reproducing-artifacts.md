# Reproducing Artifacts

This document describes the environment, source data, external services, and
execution order needed to rebuild the canonical generated artifacts. It is
intended for someone setting up the repository for the first time.

It does not describe or endorse any specific discrete choice model
specification. Downstream modeling notebooks should document their own feature
selection and interpretation decisions.

## Environment

Install the Python environment with `uv` from the repository root:

```bash
uv sync
```

The notebooks are marimo Python files and should be executed with marimo rather
than edited as plain scripts.

Copy `.env.example` to `.env`, fill in the values, and use `uv run --env-file
.env ...` when executing the workflow. If you prefer not to use `.env`, export
the same variables in your shell.

- `DATA_PATH`: root directory for source and generated project data.
- `LYRA_HOST`: host for the Lyra API service used by accessibility metrics.
- `PANGOLIN_ACCESS_TOKEN_ID`: Pangolin token ID sent to Lyra.
- `PANGOLIN_ACCESS_TOKEN`: Pangolin token sent to Lyra.
- `POSTGRES_USER`: PostgreSQL user.
- `POSTGRES_PASSWORD`: PostgreSQL password.
- `POSTGRES_HOST`: PostgreSQL host.
- `POSTGRES_PORT`: PostgreSQL port.
- `POSTGRES_DB`: PostgreSQL database name.

The workflow also requires:

- A running Lyra service reachable at `LYRA_HOST`.
- Lyra metric plugins that include `accessibility_jobs` and
  `accessibility_services`.
- An authenticated Earth Engine environment for `ee.Initialize()`.
- Network access for OSMnx road-network downloads.
- PostgreSQL/PostGIS tables with the contracts described below.

The Lyra metric implementation is external to this repository. The project
documentation currently uses `lyra-plugins` commit
`2995f4605a6f1ce123517a260ed1ad018944152f` as the inspected source for the
accessibility metrics. Record the deployed plugin commit when regenerating
artifacts against a different Lyra deployment.

## Data Layout

`DATA_PATH` is expected to contain source inputs and will receive generated
outputs. Paths below are relative to `DATA_PATH`.

| Path | Type | Required by | Contract |
| --- | --- | --- | --- |
| `processing/2/Analytics - RPPC - Interés Social - 2020 a 2025.xlsx` | Excel workbook | `notebooks/05_clean_neighborhoods.py` | Must contain `Fecha de operación`, `Inmobiliaria`, `Valor de operación`, `Superficie`, `Categoría`, `Dirección`, and `Fraccionamiento`. |
| `initial/lim_cols_cp` | Geospatial layer | `notebooks/05_clean_neighborhoods.py` | Must contain `COLONIAS`, `Col_Secc`, `ACCESO`, and `geometry`. The source CRS must be present. |
| `initial/esp_pub` | Geospatial layer | `notebooks/15_generate_neighborhood_features.py` | Must contain `TIPO`, `Sup_M2`, and `geometry`. Park polygons are reduced to centroids before calling Lyra. |
| `generated/` | Directory | All canonical notebooks | Receives intermediate and final artifacts. |
| `processed/` | Directory | `notebooks/10_cluster_statistics.py` | Receives sector-cluster diagnostic GeoPackages. |

The social-housing workbook is filtered to the `Compraventa Exe` and
`Competencia inmobiliaria` categories. The retained neighborhood universe is
therefore defined by the neighborhoods observed in the cleaned transaction
records, not by every residential neighborhood in Mexicali.

## Database Contracts

The canonical workflow uses one PostgreSQL/PostGIS connection, built from the
`POSTGRES_*` environment variables.

| Table | Required by | Contract |
| --- | --- | --- |
| `denue_2025_05` | `notebooks/10_cluster_statistics.py` | Must contain `codigo_act`, `per_ocu`, and `geometry`. Geometries are read as `EPSG:6372`; sector filters use SCIAN prefixes. |
| `centroids_historical` | `notebooks/15_generate_neighborhood_features.py` | Must contain `cve_met` and `geometry`. The workflow reads the row where `cve_met = '02.2.03'` as the metropolitan-center anchor. |

## Regeneration Order

Run the notebooks from the repository root in this order. The marimo UI is
acceptable for manual execution. For batch-style execution, use
`marimo export session`; add `--force-overwrite` when you need to force a fresh
session execution.

```bash
uv run --env-file .env marimo export session --no-continue-on-error notebooks/05_clean_neighborhoods.py
uv run --env-file .env marimo export session --no-continue-on-error notebooks/10_cluster_statistics.py
uv run --env-file .env marimo export session --no-continue-on-error notebooks/15_generate_neighborhood_features.py
uv run --env-file .env python scripts/generate_doc_tables.py
```

`notebooks/05_clean_neighborhoods.py` writes:

- `generated/neighborhoods_clean.gpkg`
- `generated/transactions_clean.parquet`

`notebooks/10_cluster_statistics.py` reads
`generated/neighborhoods_clean.gpkg` and writes:

- `generated/sector_cluster_neighborhood_features.gpkg`
- `generated/sector_cluster_config_summary.parquet`
- `generated/sector_cluster_point_summary.parquet`
- `generated/sector_cluster_grid_summary.parquet`
- `generated/sector_cluster_spatial_stats_summary.parquet`
- `generated/sector_cluster_summary.parquet`
- `generated/sector_cluster_neighborhood_feature_summary.parquet`
- `generated/sector_cluster_threshold_audit.parquet`
- sector diagnostic GeoPackages under `processed/`

`notebooks/15_generate_neighborhood_features.py` reads the cleaned artifacts and
sector-cluster artifacts, then writes:

- `generated/col_final.gpkg`
- `generated/transactions_final.parquet`

`scripts/generate_doc_tables.py` reads `generated/col_final.gpkg` and writes:

- `docs/generated/feature-catalog.md`

## Troubleshooting

If `DATA_PATH` is missing or points at the wrong directory, the notebooks fail
before reading source inputs.

If Lyra calls fail, verify that `LYRA_HOST` is reachable, the Pangolin
credentials are valid, and the deployed Lyra engine has registered the required
metric plugins.

If Earth Engine initialization fails, authenticate the local environment before
running `notebooks/15_generate_neighborhood_features.py`.

If OSMnx fails, check network access and whether OpenStreetMap data can be
downloaded for the neighborhood bounding box.

If sector-cluster notebooks fail at database reads, verify the PostGIS tables,
column names, geometry CRS, and the `POSTGRES_*` variables.
