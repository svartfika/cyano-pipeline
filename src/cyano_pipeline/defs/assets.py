from collections.abc import Iterator
from typing import Any

import dagster as dg
import dlt
from dagster._core.definitions.result import MaterializeResult
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType
from dagster_duckdb import DuckDBResource
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

from cyano_pipeline.defs.dlt.sources.havochvatten import havochvatten_source
from cyano_pipeline.defs.resources import DUCKDB_PATH
from cyano_pipeline.defs.utils import build_table_schema, count_table_rows

SCHEMA_RAW_HAVOCHVATTEN = "raw_havochvatten"
SCHEMA_CORE = "core"

DUCKDB_CREDENTIALS: DuckDbCredentials = DuckDbCredentials(
    conn_or_path=str(DUCKDB_PATH),
)

_DLT_DUCKDB_POOL = "duckdb_write"


@dlt_assets(
    dlt_source=havochvatten_source().with_resources("profiles", "results"),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="dlt_havochvatten",
        destination=dlt.destinations.duckdb(credentials=DUCKDB_CREDENTIALS),
        dataset_name=SCHEMA_RAW_HAVOCHVATTEN,
        progress="log",
    ),
    group_name="dlt_havochvatten",
    pool=_DLT_DUCKDB_POOL,
)
def havochvatten_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource) -> Iterator[DltEventType]:
    """Loads profiles (SCD2) and results (incremental) from Havochvatten API."""
    yield from dlt.run(context=context)


@dg.asset(
    deps=["dlt_havochvatten_source_profiles"],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def dim_bathing_waters(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "dim_bathing_waters"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
CREATE SCHEMA IF NOT EXISTS {schema_name};

CREATE OR REPLACE TABLE {fq_table_name} AS
SELECT
    _waters_id AS id,
    bathing_water__name AS name

FROM {SCHEMA_RAW_HAVOCHVATTEN}.profiles
WHERE _dlt_valid_to IS NULL;
    """
    with duckdb.get_connection() as conn:
        _ = conn.execute(query=query)
        row_count = count_table_rows(conn, schema_name, table_name)
        col_count, table_schema = build_table_schema(conn, schema_name, table_name)

        return dg.MaterializeResult(
            metadata={"row_count": row_count, "table": fq_table_name, "columns": col_count, "schema": table_schema},
        )


@dg.asset(
    deps=["dlt_havochvatten_source_results"],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def fact_water_samples(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "fact_water_samples"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
CREATE SCHEMA IF NOT EXISTS {schema_name};

CREATE OR REPLACE TABLE {fq_table_name} AS
SELECT
    _waters_id AS id,
    taken_at

FROM {SCHEMA_RAW_HAVOCHVATTEN}.results;
    """
    with duckdb.get_connection() as conn:
        _ = conn.execute(query=query)
        row_count = count_table_rows(conn, schema_name, table_name)
        col_count, table_schema = build_table_schema(conn, schema_name, table_name)

        return dg.MaterializeResult(
            metadata={"row_count": row_count, "table": fq_table_name, "columns": col_count, "schema": table_schema},
        )
