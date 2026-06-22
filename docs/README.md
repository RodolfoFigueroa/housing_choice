# Housing Choice Documentation

This folder documents the neighborhood-level housing-choice analysis inputs and
assumptions. It is meant for future developers that need
to understand the data, feature provenance, and caveats before changing the
analysis workflow.

Read the documents in this order:

1. [Model methodology](model-methodology.md) explains the research objective,
   analysis unit, and caveats.
2. [Data lineage](data-lineage.md) traces the artifacts from raw inputs to the
   final analysis tables.
3. [Neighborhood feature dictionary](neighborhood-feature-dictionary.md)
   explains each feature family, how it is built, and how it should be
   interpreted.
4. [Generated feature catalog](generated/feature-catalog.md) is generated from
   the current `col_final.gpkg` artifact and the modeling feature catalog code.

Regenerate the raw catalog tables with:

```bash
uv run python scripts/generate_doc_tables.py
```

The command requires `DATA_PATH` to point at the project data directory.
