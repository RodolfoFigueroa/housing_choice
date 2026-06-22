# Housing Choice Documentation

This folder documents the neighborhood-level housing-choice analysis inputs and
assumptions. It is meant for future developers that need
to understand the data, feature provenance, and caveats before changing the
analysis workflow.

Read the documents in this order:

1. [Model methodology](model-methodology.md) explains the research objective,
   analysis unit, and caveats.
2. [Reproducing artifacts](reproducing-artifacts.md) explains environment
   variables, source-data contracts, external services, and regeneration order.
3. [Glossary](glossary.md) defines project-specific data, spatial, metric, and
   method terms used across the documentation.
4. [Data lineage](data-lineage.md) traces the artifacts from raw inputs to the
   final analysis tables.
5. [Neighborhood feature dictionary](neighborhood-feature-dictionary.md)
   explains each feature family, how it is built, and how it should be
   interpreted.
6. [Generated feature catalog](generated/feature-catalog.md) is generated from
   the current `col_final.gpkg` artifact and the project feature catalog helper.

Regenerate the raw catalog tables with:

```bash
uv run --env-file .env python scripts/generate_doc_tables.py
```

The command requires `DATA_PATH` to point at the project data directory.
