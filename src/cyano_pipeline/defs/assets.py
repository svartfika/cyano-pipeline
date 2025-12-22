from collections.abc import Iterator
from pathlib import Path
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
from cyano_pipeline.defs.utils import build_materialize_result, ensure_schema

PATH_SEED: Path = Path("seed")

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

# --- Seed ---


@dg.asset(
    group_name="core_seed",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_municipalities(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_municipalities"
    fq_table_name = f"{schema_name}.{table_name}"

    path_csv: Path = PATH_SEED / "municipality_master_2025.csv"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    SELECT
        municipality,
        county,
        county_name,
        land,
        nuts1_name,
        nuts2_name
    FROM read_csv_auto('{path_csv}', header=true)
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


@dg.asset(
    deps=["ref_municipalities"],
    group_name="core_seed",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_municipalities_aliases(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_municipalities_aliases"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} (
        source_name VARCHAR PRIMARY KEY,
        canonical_name VARCHAR NOT NULL
    );

    INSERT INTO {fq_table_name} VALUES 
        ('Malung', 'Malung-Sälen'),
        ('Upplands-Väsby', 'Upplands Väsby')
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Lookup ---


@dg.asset(
    group_name="core_lookup",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_lookup_algal_id(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_algal_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
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
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


@dg.asset(
    group_name="core_lookup",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_lookup_water_type_id(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_water_type_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
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
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Ref ---


@dg.asset(
    deps=[
        "fact_water_samples",
        "dim_bathing_waters",
    ],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_effective_season_bounds(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "ref_effective_season_bounds"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    WITH sample_percentiles AS (
        SELECT
            d.nuts2_name,
            d.water_type_status_code,

            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY f.sample_week)::INTEGER AS p10_week,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.sample_week)::INTEGER AS p90_week,

            COUNT(*) AS n_samples

        FROM {SCHEMA_CORE}.fact_water_samples f

        INNER JOIN {SCHEMA_CORE}.dim_bathing_waters d
            ON f.id = d.id

        WHERE f.has_algae_data

        GROUP BY d.nuts2_name, d.water_type_status_code
    ),

    bloom_extent AS (
        SELECT
            d.nuts2_name,
            d.water_type_status_code,

            MAX(f.sample_week) FILTER (WHERE f.is_bloom) AS latest_bloom_week

        FROM {SCHEMA_CORE}.fact_water_samples f

        INNER JOIN {SCHEMA_CORE}.dim_bathing_waters d
            ON f.id = d.id

        WHERE f.has_algae_data

        GROUP BY d.nuts2_name, d.water_type_status_code
    )

    SELECT
        sp.nuts2_name,
        sp.water_type_status_code,
        sp.p10_week AS effective_start_week,

        GREATEST(
            sp.p90_week,
            COALESCE(be.latest_bloom_week, sp.p90_week)
            ) + 1 AS effective_end_week,

        sp.n_samples

    FROM sample_percentiles sp

    LEFT JOIN bloom_extent be 
        ON sp.nuts2_name = be.nuts2_name 
        AND sp.water_type_status_code = be.water_type_status_code
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Dim ---


@dg.asset(
    deps=[
        "dlt_havochvatten_source_profiles",
        "ref_lookup_water_type_id",
        "ref_municipalities_aliases",
        "ref_municipalities",
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
    CREATE OR REPLACE TABLE {fq_table_name} AS

    WITH perimeter_points AS (
        SELECT 
            _dlt_parent_id,
            
            LIST(
                ST_Point(
                    TRY_CAST(longitude AS DOUBLE), 
                    TRY_CAST(latitude AS DOUBLE)
                )
                ORDER BY _dlt_list_idx
            ) AS points

        FROM {SCHEMA_RAW_HAVOCHVATTEN}.waters__bathing_water__perimeter_coordinates

        WHERE TRY_CAST(longitude AS DOUBLE) IS NOT NULL 
            AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL

        GROUP BY _dlt_parent_id
    ),

    perimeter_polygons AS (
        SELECT 
            _dlt_parent_id,

            ST_MakePolygon(
                ST_MakeLine(
                    list_concat(points, [points[1]])
                )
            ) AS perimeter_geom

        FROM perimeter_points

        WHERE len(points) >= 3
    )

    SELECT
        p._waters_id AS id,
        p.bathing_water__name AS name,

        p.bathing_water__eu_type AS is_eu_designated,

        l.water_type_id,
        l.status_code AS water_type_status_code,

        p.algae AS algae_bloom_risk,
        p.cyano AS cyanobacteria_risk,

        p.bathing_season__starts_at AS season_start,
        p.bathing_season__ends_at AS season_end,

        EXTRACT(WEEK FROM p.bathing_season__starts_at)::INTEGER AS season_start_week,
        EXTRACT(WEEK FROM p.bathing_season__ends_at)::INTEGER AS season_end_week,

        DATEDIFF('day',
            p.bathing_season__starts_at,
            p.bathing_season__ends_at
        ) + 1 AS season_duration_days,

        TRIM(p.bathing_water__municipality__name) AS municipality_raw,
        m.municipality,
        m.county,
        m.county_name,
        m.land,
        m.nuts1_name,
        m.nuts2_name,

        TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE) AS sampling_point_lon,
        TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE) AS sampling_point_lat,
        
        CASE 
            WHEN TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE) IS NOT NULL
                AND TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE) IS NOT NULL
            
            THEN ST_Point(
                TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE),
                TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE)
            )

            ELSE NULL
        END AS sampling_point_geom,

        pp.perimeter_geom,

        p._dlt_load_id,
        p._dlt_id

    FROM {SCHEMA_RAW_HAVOCHVATTEN}.profiles p

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_water_type_id l
        ON p.bathing_water__water_type_id = l.water_type_id

    LEFT JOIN {SCHEMA_CORE}.ref_municipalities_aliases a
        ON TRIM(p.bathing_water__municipality__name) = a.source_name

    LEFT JOIN {SCHEMA_CORE}.ref_municipalities m
        ON COALESCE(a.canonical_name, TRIM(p.bathing_water__municipality__name)) = m.municipality

    LEFT JOIN perimeter_polygons pp
        ON p._dlt_id = pp._dlt_parent_id

    WHERE _dlt_valid_to IS NULL
    ;
    """
    with duckdb.get_connection() as conn:
        conn.install_extension(extension="spatial")
        conn.load_extension(extension="spatial")

        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Fact ---


@dg.asset(
    deps=[
        "dlt_havochvatten_source_results",
        "ref_lookup_algal_id",
    ],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def fact_water_samples(duckdb: DuckDBResource) -> MaterializeResult[Any]:
    schema_name = SCHEMA_CORE
    table_name = "fact_water_samples"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    SELECT
        r._waters_id AS id,

        r.taken_at,

        r.taken_at::DATE AS sample_date,
        EXTRACT(YEAR FROM r.taken_at)::INTEGER AS sample_year,
        EXTRACT(MONTH FROM r.taken_at)::INTEGER AS sample_month,
        EXTRACT(WEEK FROM r.taken_at)::INTEGER AS sample_week,

        l.algal_id,
        l.status_code AS algal_status_code,

        (r.algal_id IS NOT NULL AND r.algal_id IN (3, 4)) AS has_algae_data,
        (r.algal_id = 3) AS is_bloom,

        TRY_CAST(r.water_temp AS DOUBLE) AS water_temp,

        r._dlt_load_id,
        r._dlt_id

    FROM {SCHEMA_RAW_HAVOCHVATTEN}.results r

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_algal_id l
        ON r.algal_id = l.algal_id
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


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
