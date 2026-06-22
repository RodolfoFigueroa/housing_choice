# Neighborhood Feature Dictionary

See the [glossary](glossary.md) for project-specific definitions of data
sources, spatial terms, external services, accessibility metrics, and
cluster-statistics terminology used in this document.

## Identifiers And Access

`name`, `name_detail`, and `geometry` identify the neighborhood alternative.
`access` is represented as `access_is_restricted`, where `LIBRE` maps to 0 and
`RESTRINGIDO` maps to 1.

`access_is_restricted` controls for gated or restricted-access developments. It
should not be interpreted as a pure amenity: restricted access may also be
correlated with developer, product type, price, project age, or neighborhood
selection.

## Job Accessibility

Source in this project:
`housing_choice.funcs.calculate_accessibility_jobs`, called from
`notebooks/15_generate_neighborhood_features.py`.

External metric source:
`lyra-plugins/src/lyra_plugins/processors/accessibility_jobs.py` at commit
`2995f4605a6f1ce123517a260ed1ad018944152f`.

The project sends cleaned neighborhood geometries to Lyra and requests
sector-threshold items for 2025, month 5. Each item uses:

- `network_type="drive"`
- `edge_weights="travel_time"`
- `max_weight=10 * 60` or `20 * 60`
- a SCIAN regex pattern for the requested sector

Lyra converts input geometries to `EPSG:6372`, buffers their combined extent by
10 km, builds drive and walk accessibility networks from OSM roads, and loads
DENUE establishments from the requested year/month. DENUE `per_ocu` employment
ranges are mapped to approximate worker counts. Establishments and mesh cells
are assigned to nearest network nodes using geometry centroids.

For each requested item, Lyra filters DENUE establishments by the sector regex,
sums workers by network node, and uses Pandana to aggregate workers over the
requested network impedance with exponential decay. The result is joined to
mesh cells and averaged over mesh cells intersecting each neighborhood.

The resulting `jobs_*_2025` columns are accessibility scores in worker units,
but they are not raw counts inside a polygon and not exact point-to-point
commute times. They are network-reachable, exponentially decayed worker
opportunities within the requested threshold. The generated feature catalog
shows which job accessibility columns have prepared scaled versions and the
denominator used for each scale.

Sector groups requested by this project:

| Group | SCIAN regex |
| --- | --- |
| all | `^\d{6}` |
| manufacture | `^(31\|32\|33)\d{4}` |
| construction | `^23\d{4}` |
| logistics | `^(48\|49)\d{4}` |
| commerce | `^(43\|46)\d{4}` |
| business_services | `^(51\|52\|53\|54\|55\|56)\d{4}` |
| care_education_health | `^(61\|62)\d{4}` |
| local_services | `^(71\|72\|81)\d{4}` |
| public_admin | `^92\d{4}` |

Interpretation: higher values mean that a neighborhood overlaps mesh cells with
greater decayed access to workers in that sector by the OSM drive network. The
feature is still spatially aggregated, so it can be correlated with peripheral
development patterns, industrial land uses, road corridors, or where
social-housing projects are supplied.

## Service Accessibility

Source in this project:
`housing_choice.funcs.calculate_accessibility_services`, called from
`notebooks/15_generate_neighborhood_features.py`.

External metric source:
`lyra-plugins/src/lyra_plugins/processors/accessibility_services.py` and
`lyra_plugins.constants` at commit
`2995f4605a6f1ce123517a260ed1ad018944152f`.

The project sends cleaned neighborhood geometries and a public-space layer from
`DATA_PATH/initial/esp_pub`. `load_parks` keeps neighborhood gardens, barrio
parks, gardens, municipal nursery gardens, and urban parks; converts geometries
to centroids; renames `Sup_M2` to `area`; and labels them as recreational
parks.

The requested item is `all` services with:

- `network_type="drive"`
- `attraction_edge_weights="length"`
- `attraction_max_weight=1000`
- `accessibility_edge_weights="length"`
- `accessibility_max_weight=1000`

Lyra combines DENUE amenities and supplied public-space parks. DENUE amenities
are classified by SCIAN regex into health, recreation, and education service
types. Each amenity type has a service-specific attraction formula. Examples:
hospital attraction is workers times 20 daily patients, pharmacies are workers
times prescriptions per hour times hours, and park attraction is area divided by
30 square meters per visitor times two daily turnover cycles.

Lyra loads 2020 census AGEB population fields, overlays them onto level-9 mesh
cells, assigns mesh cells to network nodes, and uses network aggregation to
estimate the relevant population reachable from each amenity. Attraction is
discounted by that reached population. The final neighborhood score aggregates
adjusted attraction back to mesh cells over the configured network impedance,
applies `log(accessibility + 1) * 12.5`, clips to 0 through 100, divides by 100,
and averages intersecting mesh cells for each neighborhood.

Interpretation: `accessibility_services` is a 0-1 service accessibility score,
not a count of amenities. It mixes service capacity, population pressure,
network distance, and neighborhood mesh overlap. The generated feature catalog
records the prepared scaled version.

## Travel Times

Source: `notebooks/15_generate_neighborhood_features.py`.

The notebook builds an OSMnx road graph around the cleaned-neighborhood extent,
adds OSMnx edge speeds and travel times, maps neighborhood centroids to nearest
graph nodes, and computes shortest paths to:

- the metropolitan center from `centroids_historical` for `cve_met='02.2.03'`
- the west border crossing coordinate
- the east border crossing coordinate

Raw travel-time columns are in seconds. The prepared feature columns include
`travel_time_city_center_scaled` and
`travel_time_nearest_crossing_scaled`, where nearest crossing is the minimum of
east and west crossing times.

Interpretation: higher values mean longer road-network travel time. These
features are centrality and border-access controls, but they can also proxy for
urban edge, land price, infrastructure quality, or development timing.

## Built-Area History

Source: `notebooks/15_generate_neighborhood_features.py`.

The notebook uses Google Dynamic World `GOOGLE/DYNAMICWORLD/V1`, selects the
`built` band, averages each year from 2020 through 2025, multiplies by pixel
area, and reduces the result over each neighborhood. The exported columns are
`built_area_2020` through `built_area_2025` in square meters.

For transaction-year-aware analysis, the prepared representation maps each
purchase to the built-area column matching the purchase year and derives
`log_built_area_ha` as `log1p(area_m2 / 10000)`.

Interpretation: this is a broad development-intensity and supply proxy. It is
not a direct measure of available units, project inventory, housing quality, or
the number of homes for sale at the purchase date.

## Sector Cluster Exposure

Source:
`notebooks/10_cluster_statistics.py` and `src/housing_choice/sector_clusters.py`.

The cluster pipeline reads DENUE `denue_2025_05`, filters establishments by
semantic SCIAN sector prefixes, maps `per_ocu` to approximate workers, and
aggregates jobs to a 250 meter grid around the cleaned neighborhoods with a
10 km buffer.

It computes spatial statistics on grid job counts, including global Moran's I,
local Moran high-high cells, and Getis-Ord Gi* hotspot cells. Hotspot cells are
dissolved into connected clusters before the cluster-level thresholds are
applied. Clusters must have at least 500 approximate jobs and at least 2
businesses under the configured thresholds.

For each neighborhood and sector, the pipeline exports nearest-cluster
attributes, distance to nearest cluster boundary and centroid, booleans for
cluster proximity, jobs within configured distance bands, and gravity-style
cluster exposure metrics. The generated feature catalog records which raw
columns have prepared representations.

Interpretation: cluster features measure exposure to dense sectoral employment
clusters, not general accessibility to every job in the sector. Distance
features have the opposite direction from accessibility features: larger
distance values mean farther from clusters.

## Generated Catalog

The raw source-to-model mapping is generated in
`docs/generated/feature-catalog.md`. Regenerate it when `col_final.gpkg` or
`housing_choice.modeling.features` changes.
