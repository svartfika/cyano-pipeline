from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
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
SCHEMA_MART = "mart"

DUCKDB_CREDENTIALS: DuckDbCredentials = DuckDbCredentials(
    conn_or_path=str(DUCKDB_PATH),
)

_DLT_DUCKDB_POOL = "duckdb_write"


# === Extract ===


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


# === Core layer ===

# --- Seed ---


@dg.asset(
    group_name="core_seed",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def ref_municipalities(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Loads Swedish municipality-to-region mapping from seed CSV."""
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
def ref_municipalities_aliases(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Maps municipality name variants to canonical names."""
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
def ref_lookup_algal_id(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Lookup table for algal bloom status codes."""
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_algal_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} (
        algal_id INT PRIMARY KEY,
        status_code VARCHAR NOT NULL UNIQUE,
        status_label_sv VARCHAR NOT NULL,
        sort_order INT
    );

    INSERT INTO {fq_table_name} VALUES 
        (3, 'bloom', 'Blomning', 1),
        (4, 'no_bloom', 'Ingen blomning', 2),
        (5, 'no_data', 'Ingen uppgift', 3)
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
def ref_lookup_bacteria_assess_id(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Lookup table for bacteria assessment status codes."""
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_bacteria_assess_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} (
        assess_id INT PRIMARY KEY,
        status_code VARCHAR NOT NULL UNIQUE,
        status_label_sv VARCHAR NOT NULL,
        sort_order INT
    );

    INSERT INTO {fq_table_name} VALUES 
        (1, 'pass', 'Tjänligt', 1),
        (2, 'warning', 'Tjänligt med anmärkning', 2),
        (3, 'fail', 'Otjänligt', 3),
        (0, 'no_data', 'Uppgift saknas', 4)
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
def ref_lookup_water_type_id(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Lookup table for water body type codes (sea, lake, river, delta)."""
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_water_type_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} (
        water_type_id INT PRIMARY KEY,
        status_code VARCHAR NOT NULL UNIQUE,
        status_label_sv VARCHAR NOT NULL,
        sort_order INT
    );

    INSERT INTO {fq_table_name} VALUES 
        (1, 'sea', 'Hav', 1),
        (2, 'river', 'Vattendrag', 2),
        (3, 'lake', 'Sjö', 3),
        (4, 'delta', 'Delta', 4)
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
def ref_lookup_quality_class_id(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Lookup table for EU bathing water quality classifications."""
    schema_name = SCHEMA_CORE
    table_name = "ref_lookup_quality_class_id"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} (
        quality_class_id INT PRIMARY KEY,
        status_code VARCHAR NOT NULL,
        status_label_sv VARCHAR NOT NULL,
        sort_order INT
    );

    INSERT INTO {fq_table_name} VALUES 
        (1, 'excellent', 'Utmärkt kvalitet', 1),
        (2, 'good', 'Bra kvalitet', 2),
        (3, 'satisfactory', 'Tillfredställande kvalitet', 3),
        (4, 'poor', 'Dålig kvalitet', 4),
        (6, 'new', 'Ny badplats', 10),
        (5, 'insufficient_data', 'Otillräckligt provtagen', 11),
        (7, 'pending', 'Klassificering pågår', 12),
        (0, 'not_classified', 'Ej klassificerad', 99)
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
        "ref_lookup_quality_class_id",
        "ref_municipalities_aliases",
        "ref_municipalities",
    ],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def dim_bathing_waters(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Dimension table of bathing water locations with geography and classification."""
    schema_name = SCHEMA_CORE
    table_name = "dim_bathing_waters"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    WITH latest_classification AS (
        SELECT 
            _dlt_parent_id,
            quality_class_id,
            "year" AS classification_year

        FROM {SCHEMA_RAW_HAVOCHVATTEN}.profiles__last_four_classifications
        
        -- exclude pending
        WHERE quality_class_id != 7

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY _dlt_parent_id ORDER BY "year" DESC
        ) = 1
    )

    SELECT
        p._waters_id AS id,
        p.bathing_water__name AS name,

        l.water_type_id,
        l.status_code AS water_type_status_code,

        p.bathing_water__eu_type AS is_eu_designated,

        -- classification
        
        lc.classification_year,
        lq.status_code AS quality_status_code,

        -- risk assesment

        p.algae AS algae_bloom_risk,
        p.cyano AS cyanobacteria_risk,

        -- location

        TRIM(p.bathing_water__municipality__name) AS municipality_raw,
        m.municipality,
        m.county,
        m.county_name,
        m.land,
        m.nuts1_name,
        m.nuts2_name,
        
        -- spatial

        TRY_CAST(p.bathing_water__sampling_point_position__longitude AS DOUBLE) AS longitude,
        TRY_CAST(p.bathing_water__sampling_point_position__latitude AS DOUBLE) AS latitude,

        p._dlt_load_id,
        p._dlt_id

    FROM {SCHEMA_RAW_HAVOCHVATTEN}.profiles p

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_water_type_id l
        ON p.bathing_water__water_type_id = l.water_type_id

    LEFT JOIN latest_classification lc
        ON p._dlt_id = lc._dlt_parent_id

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_quality_class_id lq
        ON lc.quality_class_id = lq.quality_class_id

    LEFT JOIN {SCHEMA_CORE}.ref_municipalities_aliases a
        ON TRIM(p.bathing_water__municipality__name) = a.source_name

    LEFT JOIN {SCHEMA_CORE}.ref_municipalities m
        ON COALESCE(a.canonical_name, TRIM(p.bathing_water__municipality__name)) = m.municipality

    WHERE _dlt_valid_to IS NULL
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Fact ---


@dg.asset(
    deps=[
        "dlt_havochvatten_source_results",
        "ref_lookup_algal_id",
        "ref_lookup_bacteria_assess_id",
    ],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def fact_water_samples(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Fact table of water quality samples with algae and bacteria measurements."""
    schema_name = SCHEMA_CORE
    table_name = "fact_water_samples"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    SELECT
        r._waters_id AS id,

        -- temporal
        r.taken_at,
        r.taken_at::DATE AS sample_date,
        EXTRACT(YEAR FROM r.taken_at)::INTEGER AS sample_year,
        EXTRACT(MONTH FROM r.taken_at)::INTEGER AS sample_month,
        EXTRACT(WEEK FROM r.taken_at)::INTEGER AS sample_week,

        -- algae: bloom/no_bloom = valid observation, no_data = missing
        algae_lookup.algal_id,
        algae_lookup.status_code AS algal_status_code,

        algae_lookup.status_code IN ('bloom', 'no_bloom') AS has_algae_data,
        algae_lookup.status_code = 'bloom' AS is_bloom,

        -- e. coli: pass/warning/fail = tested, no_data = untested
        r.escherichia_coli_assess_id AS ecoli_assess_id,
        ecoli_lookup.status_code AS ecoli_status_code,

        r.escherichia_coli_count AS ecoli_count,
        r.escherichia_coli_prefix AS ecoli_count_prefix,

        ecoli_lookup.status_code IN ('pass', 'warning', 'fail') AS has_ecoli_data,
        ecoli_lookup.status_code = 'fail' AS is_ecoli_fail,
        ecoli_lookup.status_code = 'warning' AS is_ecoli_warning,

        -- enterococci: pass/warning/fail = tested, no_data = untested
        r.intestinal_enterococci_assess_id AS enterococci_assess_id,
        enterococci_lookup.status_code AS enterococci_status_code,

        r.intestinal_enterococci_count AS enterococci_count,
        r.intestinal_enterococci_prefix AS enterococci_count_prefix,

        enterococci_lookup.status_code IN ('pass', 'warning', 'fail') AS has_enterococci_data,
        enterococci_lookup.status_code = 'fail' AS is_enterococci_fail,
        enterococci_lookup.status_code = 'warning' AS is_enterococci_warning,

        -- bacteria (combined): fail takes precedence over warning
        ecoli_lookup.status_code IN ('pass', 'warning', 'fail')
            OR enterococci_lookup.status_code IN ('pass', 'warning', 'fail')
            AS has_bacteria_data,

        ecoli_lookup.status_code = 'fail'
            OR enterococci_lookup.status_code = 'fail'
            AS is_bacteria_fail,

        (ecoli_lookup.status_code = 'warning' OR enterococci_lookup.status_code = 'warning')
            AND COALESCE(ecoli_lookup.status_code, 'no_data') != 'fail'
            AND COALESCE(enterococci_lookup.status_code, 'no_data') != 'fail'
            AS is_bacteria_warning,

        TRY_CAST(r.water_temp AS DOUBLE) AS water_temp,

        r._dlt_load_id,
        r._dlt_id

    FROM {SCHEMA_RAW_HAVOCHVATTEN}.results r

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_algal_id algae_lookup
        ON r.algal_id = algae_lookup.algal_id

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_bacteria_assess_id ecoli_lookup
        ON r.escherichia_coli_assess_id = ecoli_lookup.assess_id

    LEFT JOIN {SCHEMA_CORE}.ref_lookup_bacteria_assess_id enterococci_lookup
        ON r.intestinal_enterococci_assess_id = enterococci_lookup.assess_id
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# --- Intermediate ---


@dg.asset(
    deps=[
        "fact_water_samples",
        "dim_bathing_waters",
    ],
    group_name="core_bathing_waters",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def int_effective_season_bounds(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Computes regional bathing season date ranges from sampling patterns."""
    schema_name = SCHEMA_CORE
    table_name = "int_effective_season_bounds"
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
            OR f.has_bacteria_data

        GROUP BY d.nuts2_name, d.water_type_status_code
    ),

    event_extent AS (
        SELECT
            d.nuts2_name,
            d.water_type_status_code,

            MAX(f.sample_week) FILTER (WHERE f.is_bloom) AS latest_bloom_week,
            MAX(f.sample_week) FILTER (WHERE f.is_bacteria_fail) AS latest_bacteria_fail_week
        
        FROM {SCHEMA_CORE}.fact_water_samples f
        
        INNER JOIN {SCHEMA_CORE}.dim_bathing_waters d
            ON f.id = d.id
        
        GROUP BY d.nuts2_name, d.water_type_status_code
    )

    SELECT
        sp.nuts2_name,
        sp.water_type_status_code,

        sp.p10_week AS effective_start_week,
        GREATEST(
            sp.p90_week,
            COALESCE(ee.latest_bloom_week, 0),
            COALESCE(ee.latest_bacteria_fail_week, 0)
        ) + 1 AS effective_end_week,

        sp.n_samples

    FROM sample_percentiles sp

    LEFT JOIN event_extent ee 
        ON sp.nuts2_name = ee.nuts2_name 
        AND sp.water_type_status_code = ee.water_type_status_code
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# === Mart layer ===

# --- Analytics ---


@dg.asset(
    deps=[
        "fact_water_samples",
        "dim_bathing_waters",
        "int_effective_season_bounds",
    ],
    group_name="mart_analytics",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def mart_weekly_bacteria_metrics(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Weekly bacteria fail/warning rates by region with 3-week rolling averages."""
    schema_name = SCHEMA_MART
    table_name = "mart_weekly_bacteria_metrics"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    WITH weekly_data AS (
        SELECT
            d.nuts2_name,
            d.water_type_status_code,
            f.sample_week,
            COUNT(*) AS n_samples,
            COUNT(*) FILTER (WHERE f.is_bacteria_fail) AS n_fails,
            COUNT(*) FILTER (WHERE f.is_bacteria_warning) AS n_warnings,
            COUNT(DISTINCT d.id) AS n_locations

        FROM {SCHEMA_CORE}.fact_water_samples f

        INNER JOIN {SCHEMA_CORE}.dim_bathing_waters d
            ON f.id = d.id

        INNER JOIN {SCHEMA_CORE}.int_effective_season_bounds esb 
            ON d.nuts2_name = esb.nuts2_name 
            AND d.water_type_status_code = esb.water_type_status_code

        WHERE f.has_bacteria_data
            AND f.sample_week BETWEEN esb.effective_start_week AND esb.effective_end_week

        GROUP BY d.nuts2_name, d.water_type_status_code, f.sample_week
    ),

    with_rolling AS (
        SELECT
            *,
            SUM(n_samples) OVER w AS n_samples_3wk,
            SUM(n_fails) OVER w AS n_fails_3wk,
            SUM(n_warnings) OVER w AS n_warnings_3wk

        FROM weekly_data

        WINDOW w AS (
            PARTITION BY nuts2_name, water_type_status_code 
            ORDER BY sample_week 
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    )

    SELECT
        nuts2_name AS region,
        water_type_status_code AS water_type,
        sample_week AS week,

        -- 3-week rolling
        ROUND(100.0 * n_fails_3wk / NULLIF(n_samples_3wk, 0), 2) AS fail_rate_pct,
        ROUND(100.0 * n_warnings_3wk / NULLIF(n_samples_3wk, 0), 2) AS warning_rate_pct,

        -- single week

        n_samples,
        n_fails,
        n_warnings,
        ROUND(100.0 * n_fails / NULLIF(n_samples, 0), 2) AS fail_rate_week_pct,
        ROUND(100.0 * n_warnings / NULLIF(n_samples, 0), 2) AS warning_rate_week_pct,
        n_locations,

        -- confidence

        CASE 
            WHEN n_locations >= 50 THEN 'high'
            WHEN n_locations >= 20 THEN 'medium'
            ELSE 'low'
        END AS confidence

    FROM with_rolling

    ORDER BY region, water_type, week
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


@dg.asset(
    deps=[
        "fact_water_samples",
        "dim_bathing_waters",
        "int_effective_season_bounds",
    ],
    group_name="mart_analytics",
    pool=_DLT_DUCKDB_POOL,
    kinds={"duckdb"},
)
def mart_weekly_bloom_metrics(duckdb: DuckDBResource) -> dg.MaterializeResult[Any]:
    """Weekly algae bloom rates by region with 3-week rolling averages."""
    schema_name = SCHEMA_MART
    table_name = "mart_weekly_bloom_metrics"
    fq_table_name = f"{schema_name}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE {fq_table_name} AS

    WITH weekly_data AS (
        SELECT
            d.nuts2_name,
            d.water_type_status_code,
            f.sample_week,
            COUNT(*) AS n_samples,
            COUNT(*) FILTER (WHERE f.is_bloom) AS n_blooms,
            COUNT(DISTINCT d.id) AS n_locations

        FROM {SCHEMA_CORE}.fact_water_samples f

        INNER JOIN {SCHEMA_CORE}.dim_bathing_waters d
            ON f.id = d.id

        INNER JOIN {SCHEMA_CORE}.int_effective_season_bounds esb 
            ON d.nuts2_name = esb.nuts2_name 
            AND d.water_type_status_code = esb.water_type_status_code

        WHERE f.has_algae_data
            AND f.sample_week BETWEEN esb.effective_start_week AND esb.effective_end_week

        GROUP BY d.nuts2_name, d.water_type_status_code, f.sample_week
    ),

    with_rolling AS (
        SELECT
            *,
            SUM(n_samples) OVER w AS n_samples_3wk,
            SUM(n_blooms) OVER w AS n_blooms_3wk

        FROM weekly_data

        WINDOW w AS (
            PARTITION BY nuts2_name, water_type_status_code 
            ORDER BY sample_week 
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    )

    SELECT
        nuts2_name AS region,
        water_type_status_code AS water_type,
        sample_week AS week,

        -- 3-week rolling

        ROUND(100.0 * n_blooms_3wk / NULLIF(n_samples_3wk, 0), 2) AS bloom_rate_pct,

        -- single week

        n_samples,
        n_blooms,
        ROUND(100.0 * n_blooms / NULLIF(n_samples, 0), 2) AS bloom_rate_week_pct,
        n_locations,

        -- confidence

        CASE 
            WHEN n_locations >= 50 THEN 'high'
            WHEN n_locations >= 20 THEN 'medium'
            ELSE 'low'
        END AS confidence

    FROM with_rolling

    ORDER BY region, water_type, week
    ;
    """
    with duckdb.get_connection() as conn:
        ensure_schema(conn, schema_name)
        _ = conn.execute(query=query)

        return build_materialize_result(conn, schema_name, table_name)


# === Asset checks ===


@dg.asset_check(asset="dim_bathing_waters")
def check_municipality_coverage(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verify all bathing waters have valid municipality mappings."""
    query = f"""
    SELECT

        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE municipality IS NULL) AS orphaned

    FROM {SCHEMA_CORE}.dim_bathing_waters
    """
    with duckdb.get_connection() as conn:
        row = conn.execute(query).fetchone()

    if not row:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "Query returned no results"},
        )

    total, orphaned = row[0], row[1]
    coverage_pct = round(100 * (total - orphaned) / total, 2) if total > 0 else 0.0

    return dg.AssetCheckResult(
        passed=orphaned == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "total_locations": total,
            "orphaned_locations": orphaned,
            "coverage_pct": coverage_pct,
        },
    )
