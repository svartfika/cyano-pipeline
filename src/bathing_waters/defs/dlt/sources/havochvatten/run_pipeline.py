"""Run with: uv run python -m src.bathing_waters.defs.dlt.sources.havochvatten"""

import dlt

from . import havochvatten_source

DB_NAME = "bathing_waters"
DB_SCHEMA = "raw"


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name=DB_NAME,  # database
        destination=dlt.destinations.duckdb(),
        dataset_name=DB_SCHEMA,  # schema
        progress="log",
    )

    load_info = pipeline.run(havochvatten_source())
    print(load_info)
    return load_info
