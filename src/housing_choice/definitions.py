from pathlib import Path

from dagster_components.managers import DataFrameFileManager, GeoDataFrameFileManager

import dagster as dg
from housing_choice.defs.resources import PathResource


@dg.definitions
def definitions() -> dg.Definitions:
    project_defs = dg.load_from_defs_folder(
        path_within_project=Path(__file__).parent / "defs"
    )

    data_dir = Path(__file__).parents[2] / "data"
    path_resource = PathResource(
        in_path=(data_dir / "initial").as_posix(),
        out_path=(data_dir / "generated").as_posix(),
    )
    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "dataframe_manager": DataFrameFileManager(
                path_resource=path_resource, extension=".parquet"
            ),
            "geodataframe_manager": GeoDataFrameFileManager(
                path_resource=path_resource, extension=".geoparquet"
            ),
        }
    )
    return dg.Definitions.merge(project_defs, extra_defs)
