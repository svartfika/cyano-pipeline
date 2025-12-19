from collections.abc import Sequence

from dagster import Definitions, load_assets_from_modules
from dagster._core.definitions.module_loaders.load_assets_from_modules import AssetLoaderTypes

from cyano_pipeline.defs import assets
from cyano_pipeline.defs.resources import dlt_res, duckdb_res

all_assets: Sequence[AssetLoaderTypes] = load_assets_from_modules(modules=[assets])

defs: Definitions = Definitions(
    assets=all_assets,
    resources={
        "dlt": dlt_res,
        "duckdb": duckdb_res,
    },
)
