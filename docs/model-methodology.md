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
features, and cleaned transaction records. Any future model specification should
document its own filtering rules, choice-set definition, covariate selection,
and estimation method separately from this provenance note.

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
construction. The documents in this folder are intended to make that
verification explicit without encoding any particular model specification or
result.
