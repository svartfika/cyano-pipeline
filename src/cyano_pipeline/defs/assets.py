from dagster._core.definitions.assets.definition.assets_definition import AssetsDefinition

from cyano_pipeline.defs.dlt.factory import build_dlt_duckdb_asset
from cyano_pipeline.defs.dlt.sources.havochvatten import profiles, samples
from cyano_pipeline.defs.resources import dlt_res, duckdb_res

SCHEMA_NAME = "havochvatten_raw"

extract_havochvatten_profiles: AssetsDefinition = build_dlt_duckdb_asset(
    source=profiles,
    pipeline_name=SCHEMA_NAME,
    dataset_name="havochvatten_profiles",
    duckdb_resource=duckdb_res,
    dlt_resource=dlt_res,
)

extract_havochvatten_samples: AssetsDefinition = build_dlt_duckdb_asset(
    source=samples,
    pipeline_name=SCHEMA_NAME,
    dataset_name="havochvatten_samples",
    duckdb_resource=duckdb_res,
    dlt_resource=dlt_res,
)
