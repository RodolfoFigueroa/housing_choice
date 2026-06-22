# Glossary

This glossary defines the terms as they are used in this repository. It is not
intended as a general reference; the definitions focus on how each term affects
the neighborhood-level housing-choice data pipeline.

## Data Sources And Institutions

**AGEB**

Mexican census basic geostatistical area. The Lyra service-accessibility metric
uses 2020 AGEB population fields, overlays them onto mesh cells, and uses them
to discount service attraction by nearby population pressure.

**Compraventa Exe**

One of the transaction categories retained from the social-housing workbook.
Together with `Competencia inmobiliaria`, it defines the transaction sample
used to build the retained neighborhood universe.

**Competencia inmobiliaria**

One of the transaction categories retained from the social-housing workbook.
Together with `Compraventa Exe`, it defines the transaction sample used to
build the retained neighborhood universe.

**DENUE**

INEGI's establishment directory. This project uses DENUE establishments for
employment counts, service locations, SCIAN sector classification, and sector
cluster detection.

**Exe**

Project shorthand that appears in the retained social-housing transaction
category `Compraventa Exe`. Treat it as part of the source category label,
not as a separate feature or method.

**RPPC**

Transaction source referenced by the social-housing workbook filename. In this
project, it is the provenance of the raw purchase records that are cleaned into
`transactions_clean.parquet` and `transactions_final.parquet`.

**SCIAN**

North American Industry Classification System used by Mexico. DENUE
establishments carry SCIAN activity codes, and the project uses SCIAN prefixes
or regex patterns to define job sectors and service categories.

## Spatial Concepts

**Centroid**

A representative point derived from a geometry. This project uses centroids
when matching neighborhoods, establishments, parks, and mesh cells to nearest
network nodes or target locations.

**CRS**

Coordinate reference system. It defines how coordinates map to real-world
locations and units. CRS consistency is important because distances, buffers,
and overlays are only meaningful when geometries are in compatible systems.

**EPSG:6372**

Projected CRS used by the canonical workflow for Mexicali-area spatial
processing. Distances and buffers in the documented pipelines assume geometries
have been projected to this CRS where stated.

**GeoDataFrame**

A pandas-like table with a geometry column and CRS metadata, normally provided
by GeoPandas. The notebooks use GeoDataFrames for neighborhood, establishment,
mesh, cluster, and output feature layers.

**GeoPackage**

Spatial file format commonly written with the `.gpkg` extension. The workflow
uses GeoPackages for cleaned neighborhoods, final neighborhood features, and
sector-cluster diagnostic outputs.

**Geometry**

The spatial shape associated with a row, such as a point, polygon, or
multi-polygon. In this project, geometry determines neighborhood membership,
network matching, overlays, distances, and aggregation.

**Mesh cell**

Regular spatial cell used as an intermediate aggregation unit. Lyra assigns
mesh cells to network nodes, computes accessibility at the mesh level, and then
averages intersecting mesh cells back to neighborhoods.

## External Tools And Services

**Earth Engine**

Google Earth Engine. The built-area pipeline uses it to access Google Dynamic
World imagery and compute yearly built-area summaries over neighborhoods.

**Google Dynamic World**

Earth Engine land-cover dataset. This project uses the `built` band from
`GOOGLE/DYNAMICWORLD/V1` as a broad built-area proxy for each neighborhood and
year.

**Lyra**

External metric service called from this repository to compute accessibility
features. The core Lyra engine registers metrics from plugins; the job and
service accessibility logic documented here lives in the `lyra-plugins`
repository, not in this repository.

**Lyra metric plugin**

A processor registered by Lyra that implements one metric. Single-item metrics
expose a `calculate` function. Batched metrics, including the accessibility
metrics used here, split work into prepare, per-item, and aggregate functions.

**marimo**

Notebook framework used by this repository. The notebooks are Python files, but
they should be executed and edited with marimo-aware tooling rather than treated
as ordinary scripts.

**OSM**

OpenStreetMap data. In this project, OSM roads provide the network geometry
used by OSMnx, Pandana, travel-time features, and Lyra accessibility metrics.

**OpenStreetMap**

Community-maintained geographic data source, abbreviated as OSM. The workflow
uses OpenStreetMap road data through OSMnx and Lyra's network preparation.

**OSMnx**

Python package used to download and process OpenStreetMap road networks. This
project uses OSMnx to compute travel times from neighborhoods to the
metropolitan center and border crossings.

**Pandana**

Network accessibility library used inside Lyra. The job and service
accessibility metrics use Pandana to aggregate opportunities over network
impedance thresholds with distance decay or service-specific weighting.

**Pangolin**

Authentication layer used when this project calls Lyra. The `PANGOLIN_*`
environment variables provide the credentials sent to the Lyra service.

**PostGIS**

Spatial extension for PostgreSQL. The workflow reads DENUE establishments and
historical centroid anchors from PostgreSQL/PostGIS tables.

## Feature And Method Terms

**Accessibility score**

Network-based opportunity measure assigned to a neighborhood. It is not a raw
count inside the neighborhood polygon and should not be interpreted as a direct
household preference measure.

**Active choice set**

Transaction-specific set of available neighborhood alternatives used by the
current structural baseline. A neighborhood is available for a purchase if it
has another observed purchase within the configured time window around the
focal transaction date; the chosen neighborhood is always available.

**Availability-aware multinomial logit**

Discrete-choice model where each transaction can have a different availability
mask over neighborhood alternatives. The current baseline and grouped
job-extension notebooks use this structure.

**Built area**

Estimated neighborhood area classified as built land cover by Google Dynamic
World. It is a broad development-intensity proxy, not a count of homes,
available inventory, or housing quality.

**Cluster exposure**

Feature family that measures proximity or exposure to dense sectoral
employment clusters. It is different from accessibility because it focuses on
detected concentrations of jobs rather than all reachable jobs in a sector.

**Continuation flag**

Notebook diagnostic used when deciding whether a model extension is worth
carrying forward. In the grouped job-extension notebook, a candidate passes
only if it improves Biogeme AIC by at least 2, has a positive job coefficient,
and has robust p-value below 0.10.

**Fast availability screen**

Lightweight multinomial-logit estimator used to rank many candidate extensions
before running slower Biogeme fits. It uses the same active choice set and
dynamic alternative features as the availability-aware Biogeme model, but it is
only a screening device.

**Exponential decay**

Weighting rule where farther opportunities contribute less than nearer
opportunities. Lyra job accessibility uses exponential decay when aggregating
workers over the road network.

**Getis-Ord Gi***

Local spatial-statistics measure used by the sector-cluster pipeline to detect
hotspot grid cells. In this project, adjacent hotspot cells are dissolved into
employment clusters before cluster thresholds are applied.

**Global Moran's I**

Spatial autocorrelation statistic used as a diagnostic in the sector-cluster
pipeline. It summarizes whether grid-level job counts are spatially clustered,
dispersed, or close to random at the global scale.

**Gravity exposure**

Cluster-exposure measure that discounts cluster jobs by distance. The project
exports inverse-square and exponential-decay variants for sector clusters.

**Grouped job accessibility**

Modeling feature that combines related scaled job-accessibility columns into a
more interpretable candidate, such as industrial jobs or services jobs. Grouped
features are built in memory for model comparison and are not written back to
the canonical neighborhood feature artifact.

**Job accessibility**

Lyra metric that estimates decayed access to DENUE workers by sector over the
road network. Values are in worker-opportunity units after network aggregation,
not observed commute choices or jobs physically located inside the
neighborhood.

**Local Moran high-high**

Local spatial-statistics classification for grid cells with high values near
other high-value cells. The cluster pipeline uses it as one diagnostic of
sectoral employment concentration.

**Network impedance**

Cost used to traverse a network, such as travel time or distance. The
accessibility metrics aggregate opportunities that are reachable within
configured impedance thresholds.

**per_ocu**

DENUE employment-size range field. The project maps these ranges to approximate
worker counts before calculating job accessibility and sector-cluster features.

**Service accessibility**

Lyra metric that combines DENUE amenities, supplied park locations, service
capacity assumptions, nearby population pressure, network distance, and mesh
overlap into a 0-1 neighborhood score.

## Project Concepts

**Neighborhood-level association**

The intended interpretation level for downstream modeling with these artifacts.
Features describe aggregate neighborhood conditions associated with observed
purchases; they do not directly recover individual household preferences.

**Retained neighborhood universe**

Set of neighborhoods kept after matching cleaned social-housing transactions to
cleaned neighborhood geometries. It is not the full set of residential
neighborhoods in Mexicali.

**Social-housing transaction sample**

Purchase records retained from the source workbook for the social-housing
market segment. The sample describes observed purchases and may reflect both
buyer demand and where social-housing supply was available.

**South-southeast peripheral clustering**

Documented caveat for Mexicali: social housing tends to be clustered in the
south-southeast peripheral region. Many neighborhood features may vary along
this same spatial gradient, so feature associations can contain spatial noise.

**Structural baseline**

Current measuring-stick discrete-choice model in `notebooks/baseline.py`. It
uses an active choice set, supply/activity proxy, transaction-year built area,
service and travel-time controls, restricted access, and centroid spatial
controls, while excluding job-accessibility variables.

## Maintenance

Update this glossary when adding a new external data source, feature family,
metric service, spatial method, or project-specific term that a first-time
reader would not be able to decode from the surrounding text.
