from collections.abc import Sequence

import dagster as dg
from dagster._core.definitions.module_loaders.load_assets_from_modules import AssetLoaderTypes

from cyano_pipeline.defs import assets
from cyano_pipeline.defs.resources import dlt_res, duckdb_res

all_assets: Sequence[AssetLoaderTypes] = dg.load_assets_from_modules(modules=[assets])


# === Jobs ===


seed_data_job = dg.define_asset_job(
    name="seed_data_refresh",
    selection=dg.AssetSelection.assets(
        "ref_municipalities",
        "ref_municipalities_aliases",
        "ref_lookup_algal_id",
        "ref_lookup_water_type_id",
    ),
)

core_dimensions_job = dg.define_asset_job(
    name="core_dimensions_refresh",
    selection=dg.AssetSelection.assets(
        "dlt_havochvatten_source_profiles",
        "dim_bathing_waters",
    ),
)

core_facts_job = dg.define_asset_job(
    name="core_facts_refresh",
    selection=dg.AssetSelection.assets(
        "dlt_havochvatten_source_results",
        "fact_water_samples",
    ),
)

core_intermediate_job = dg.define_asset_job(
    name="core_intermediate_refresh",
    selection=dg.AssetSelection.assets(
        "int_effective_season_bounds",
    ),
)

mart_analytics_job = dg.define_asset_job(
    name="mart_analytics_refresh",
    selection=dg.AssetSelection.assets(
        "mart_weekly_bloom_metrics",
    ),
)


# === Schedules ===


core_dimensions_schedule = dg.ScheduleDefinition(
    job=core_dimensions_job,
    cron_schedule="0 3 * * 0",  # Weekly
)

core_facts_schedule = dg.ScheduleDefinition(
    job=core_facts_job,
    cron_schedule="0 2 * * *",  # Daily
)

core_intermediate_schedule = dg.ScheduleDefinition(
    job=core_intermediate_job,
    cron_schedule="0 4 * * 0",  # Weekly
)

mart_analytics_schedule = dg.ScheduleDefinition(
    job=mart_analytics_job,
    cron_schedule="0 5 * * *",  # Daily
)


# === Definitions ===


defs = dg.Definitions(
    assets=all_assets,
    resources={
        "dlt": dlt_res,
        "duckdb": duckdb_res,
    },
    jobs=[
        seed_data_job,
        core_dimensions_job,
        core_facts_job,
        core_intermediate_job,
        mart_analytics_job,
    ],
    schedules=[
        core_dimensions_schedule,
        core_facts_schedule,
        core_intermediate_schedule,
        mart_analytics_schedule,
    ],
)
