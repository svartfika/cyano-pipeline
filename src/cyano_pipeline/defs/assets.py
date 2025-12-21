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


# --- Extract ---


@dlt_assets(
    dlt_source=havochvatten_source().with_resources("waters", "profiles", "results"),
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


# --- Core layer ---


@dg.asset(
    deps=["dlt_havochvatten_source_profiles"],
    group_name="core_lookup",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_lookup_algal_id(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_algal_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
CREATE SCHEMA IF NOT EXISTS {schema_name};

CREATE OR REPLACE TABLE {fq_table_name} (
    algal_id INT PRIMARY KEY,
    status_code VARCHAR NOT NULL UNIQUE,
    sort_order INT
);

INSERT INTO {fq_table_name} VALUES 
    (3, 'bloom', 1),
    (4, 'no_bloom', 2),
    (5, 'no_data', 3)
;
    """
    with duckdb.get_connection() as conn:
        _ = conn.execute(query=query)
        row_count = count_table_rows(conn, schema_name, table_name)
        col_count, table_schema = build_table_schema(conn, schema_name, table_name)

        return dg.MaterializeResult(
            metadata={"row_count": row_count, "table": fq_table_name, "columns": col_count, "schema": table_schema},
        )


@dg.asset(
    deps=["dlt_havochvatten_source_profiles"],
    group_name="core_lookup",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_lookup_water_type_id(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_water_type_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
CREATE SCHEMA IF NOT EXISTS {schema_name};

CREATE OR REPLACE TABLE {fq_table_name} (
    water_type_id INT PRIMARY KEY,
    status_code VARCHAR NOT NULL UNIQUE,
    sort_order INT
);

INSERT INTO {fq_table_name} VALUES 
    (1, 'sea', 1),
    (2, 'river', 2),
    (3, 'lake', 3),
    (4, 'delta', 4)
;
    """
    with duckdb.get_connection() as conn:
        _ = conn.execute(query=query)
        row_count = count_table_rows(conn, schema_name, table_name)
        col_count, table_schema = build_table_schema(conn, schema_name, table_name)

        return dg.MaterializeResult(
            metadata={"row_count": row_count, "table": fq_table_name, "columns": col_count, "schema": table_schema},
        )


@dg.asset(
    deps=[
        "dlt_havochvatten_source_profiles",
        "ref_lookup_water_type_id",
    ],
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
        p._waters_id AS id,
        p.bathing_water__name AS name,

        l.water_type_id,
        l.status_code AS water_type_status_code,

        p.algae AS algae_bloom_risk,
        p.cyano AS cyanobacteria_risk,

        p.bathing_season__starts_at AS season_start,
        p.bathing_season__ends_at AS season_end,

        TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE) AS sampling_point_lon,
        TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE) AS sampling_point_lat,
        
        ST_Point(
                TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE),
                TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE)
        ) AS sampling_point_geom,

        p._dlt_load_id,
        p._dlt_id

    FROM {SCHEMA_RAW_HAVOCHVATTEN}.profiles p

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_water_type_id l
        ON p.bathing_water__water_type_id = l.water_type_id

    WHERE _dlt_valid_to IS NULL
    ;
    """
    with duckdb.get_connection() as conn:
        conn.install_extension(extension="spatial")
        conn.load_extension(extension="spatial")

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


# --- Jobs ---


profiles_job = dg.define_asset_job(
    name="profiles_refresh",
    selection=dg.AssetSelection.assets(
        "dlt_havochvatten_source_profiles",
        "dim_bathing_waters",
    ),
)

samples_job = dg.define_asset_job(
    name="samples_refresh",
    selection=dg.AssetSelection.assets(
        "dlt_havochvatten_source_results",
        "fact_water_samples",
    ),
)


# --- Schedules ---


weekly_profiles_schedule = dg.ScheduleDefinition(
    job=profiles_job,
    cron_schedule="0 3 * * 0",
)

daily_samples_schedule = dg.ScheduleDefinition(
    job=samples_job,
    cron_schedule="0 2 * * *",
)
