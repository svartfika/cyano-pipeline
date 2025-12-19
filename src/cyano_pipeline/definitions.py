from dagster import Definitions, load_assets_from_modules
from cyano_pipeline.defs import assets
from cyano_pipeline.defs.resources import dlt_res, duckdb_res

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dlt": dlt_res,
        "duckdb": duckdb_res,
    },
)
