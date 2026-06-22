# Reproducing Artifacts

This document describes the environment, source data, external services, and
execution order needed to rebuild the canonical generated artifacts. It is
intended for someone setting up the repository for the first time.

It separates canonical data-artifact regeneration from model estimation. The
current discrete-choice baseline and grouped job-extension notebooks are
documented in [model methodology](model-methodology.md), but they do not write
canonical generated data artifacts.

See the [glossary](glossary.md) for project-specific definitions of external
services, source data, spatial formats, and feature-method terms used below.

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

## Lyra Service Data Requirements

The canonical workflow does not rebuild Lyra's backend data. If `LYRA_HOST`
points to an existing managed service, treat this section as the contract that
the service must already satisfy. If you operate the Lyra service yourself, the
engine must register the `accessibility_jobs` and `accessibility_services`
processors and have access to the data below.

| Lyra-side input | Used by | Contract |
| --- | --- | --- |
| DENUE establishments for the requested year and month | `accessibility_jobs`, `accessibility_services` | Must include establishment geometry, SCIAN activity code, and `per_ocu` employment-size range. Job accessibility filters these establishments by requested sector regex and maps `per_ocu` to approximate worker counts. |
| Level-9 mesh cells covering the buffered request extent | `accessibility_jobs`, `accessibility_services` | Must provide geometries that can be assigned to network nodes and averaged back to the requested neighborhood geometries. |
| 2020 AGEB census population fields | `accessibility_services` | Must support overlaying population onto level-9 mesh cells so service attraction can be discounted by nearby population pressure. |
| OSM road-network access or cache | `accessibility_jobs`, `accessibility_services` | Must support the configured network types and impedance fields used by the metric requests, including drive-network travel time for job accessibility and distance-based accessibility for services. |
| Client-supplied park/public-space geometries | `accessibility_services` | Supplied by this repository from `DATA_PATH/initial/esp_pub` in the Lyra request payload, not read from the Lyra database. |

When regenerating artifacts against a new Lyra deployment, record both the
deployed plugin commit and the backend data vintages. Otherwise identical
repository inputs can produce different accessibility features.

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

## Modeling Notebooks

After the canonical artifacts exist, the current modeling notebooks can be run
from the repository root:

```bash
uv run --env-file .env marimo export session --no-continue-on-error notebooks/baseline.py
uv run --env-file .env marimo export session --no-continue-on-error notebooks/job_extensions.py
```

`notebooks/baseline.py` uses
`housing_choice.modeling.build_structural_baseline_inputs` to assemble the
shared structural baseline and fit the availability-aware Biogeme model.

`notebooks/job_extensions.py` uses the same shared baseline inputs, adds
grouped job-accessibility features, runs a fast availability-aware screen, and
fits Biogeme for the structural baseline plus the top two grouped job
extensions.

These notebooks are analysis outputs, not data-lineage inputs. Rerun them after
changing the model helpers, generated feature data, or the candidate-extension
logic.

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
