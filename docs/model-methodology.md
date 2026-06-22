# Model Methodology

## Objective

The project studies which neighborhood attributes are available to explain
observed social-housing purchases. The analysis unit is the neighborhood, not
the individual home or household. Feature values therefore describe aggregate
neighborhood conditions associated with observed purchases.

Any downstream choice model should be read as a neighborhood-level association
model. It should not be read as direct household-level preference recovery
unless the analysis later adds the missing information needed for that
interpretation, such as the full set of house-level alternatives, household
budgets, household workplaces, developer inventory, financing eligibility, and
exact supply available at purchase time.

## Analysis Inputs

The stable inputs for downstream modeling are:

- `DATA_PATH/generated/col_final.gpkg`
- `DATA_PATH/generated/transactions_final.parquet`

These artifacts contain the retained neighborhood geometries, neighborhood
features, and cleaned transaction records. Current modeling notebooks should
reuse `housing_choice.modeling.build_structural_baseline_inputs` so they share
the same filtering rules, choice-set definition, supply/activity proxy, and
prepared covariate columns.

## Current Discrete-Choice Baseline

The current baseline lives in `notebooks/baseline.py`. It replaces the legacy
all-neighborhoods-always-available setup with an availability-aware multinomial
logit estimated with Biogeme.

The shared baseline builder:

- keeps purchases from 2020 through 2025 whose cleaned address matches a
  retained neighborhood;
- uses every retained neighborhood from `col_final.gpkg` as the candidate
  alternative universe;
- defines transaction-specific available alternatives as neighborhoods with
  another observed purchase within a 365-day window around the focal purchase
  date, while always keeping the chosen neighborhood available;
- drops transactions only if fewer than two alternatives are available;
- adds `log_active_sales_12m`, derived from the transaction-specific active
  sales matrix, as a supply/activity proxy;
- maps transaction year to `log_built_area_ha` from the matching
  `built_area_YYYY` column;
- includes service accessibility, city-center travel time, nearest-crossing
  travel time, restricted access, and centroid east/north controls;
- intentionally excludes employment-access variables.

The baseline should be treated as the measuring stick for later model
extensions. It is not meant to be permanent; it is meant to be stable,
compact, reproducible, and explicit enough that a new extension can be compared
against it without reinterpreting the data assembly.

## Grouped Job Extensions

The first model extension lives in `notebooks/job_extensions.py`. It tests
whether job accessibility adds explanatory value after holding the baseline
choice set, supply/activity proxy, built-area term, and controls fixed.

The extension deliberately avoids single-sector fishing by adding one grouped
job feature at a time. The current grouped candidates are:

- all jobs at 10 and 20 minutes;
- industrial jobs at 10 and 20 minutes, computed as the mean of manufacturing,
  logistics, and construction accessibility;
- services jobs at 10 and 20 minutes, computed as the mean of business
  services, care/education/health, and local services accessibility;
- commerce jobs at 10 and 20 minutes.

Public administration accessibility is excluded from these grouped candidates
because the current prepared feature has no useful variation.

The job-extension notebook first uses
`housing_choice.modeling.fit_fast_availability_mnl_screen` to rank all grouped
job candidates with the same availability mask and dynamic supply feature used
by the Biogeme model. It then fits Biogeme only for the structural baseline and
the top two non-baseline screen candidates. A grouped job candidate is marked
for continuation only if it improves Biogeme AIC by at least 2, has a positive
job coefficient, and has robust p-value below 0.10.

In the current generated data, the two industrial grouped candidates are the
Biogeme finalists selected by the fast screen. This should be read as a
model-building signal, not as a final claim that buyers prefer industrial jobs:
the next modeling step should test whether that signal survives stronger
spatial controls.

## Interpretation Caveats

The transaction data are a social-housing purchase sample. This is the right
scope for studying that market segment, but it means the analysis describes
where social-housing purchases occurred rather than unconstrained citywide
housing preferences.

The sample also mixes demand and supply. If developers built social housing in
specific corridors because land was cheaper, permits were easier, or parcels
were available, estimated associations can partly reflect project placement and
inventory rather than buyer preferences.

In Mexicali, social housing tends to be clustered in the south-southeast
peripheral region. That geography can create spatial noise because many
features vary together along the same urban-peripheral gradient. A future model
may therefore pick up the geography of social-housing supply as much as the
standalone effect of any one neighborhood feature.

Features are measured at the neighborhood level. A job-accessibility,
service-accessibility, travel-time, built-area, or cluster-exposure feature does
not mean every household in a neighborhood experiences that value. It is an
aggregate exposure attached to the neighborhood.

Spatial structure is likely important. Feature estimates can reflect omitted
spatial factors such as industrial disamenities, truck corridors, cheaper edge
land, distance from amenities, project phasing, neighborhood age, or unobserved
developer characteristics.

## Before Modeling

Before estimating or revising a model, maintainers should verify feature
provenance, units, transformations, aggregation level, and choice-set
construction. New model notebooks should either call the shared structural
baseline builder or document why they intentionally depart from it. Extension
notebooks should keep their candidate set compact, record model-comparison
criteria, and avoid promoting a feature into a new baseline until spatial and
supply confounding have been checked.
