import dagster as dg


class PathResource(dg.ConfigurableResource):
    in_path: str
    out_path: str
