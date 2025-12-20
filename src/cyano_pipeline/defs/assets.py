from collections.abc import Iterator

import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

from cyano_pipeline.defs.dlt.sources.havochvatten import havochvatten_source
from cyano_pipeline.defs.resources import DUCKDB_PATH

DUCKDB_CREDENTIALS: DuckDbCredentials = DuckDbCredentials(
    conn_or_path=str(DUCKDB_PATH),
)

DATASET_NAME = "raw"

_DLT_DUCKDB_POOL = "duckdb_write"


@dlt_assets(
    dlt_source=havochvatten_source().with_resources("profiles", "results"),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="dlt_havochvatten",
        destination=dlt.destinations.duckdb(credentials=DUCKDB_CREDENTIALS),
        dataset_name=DATASET_NAME,
        progress="log",
    ),
    group_name="dlt_havochvatten",
    pool=_DLT_DUCKDB_POOL,
)
def havochvatten_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource) -> Iterator[DltEventType]:
    """Loads profiles (SCD2) and results (incremental) from Havochvatten API."""
    yield from dlt.run(context=context)
