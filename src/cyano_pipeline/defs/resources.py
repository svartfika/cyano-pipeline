import os
from pathlib import Path

from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

DUCKDB_PATH: Path = Path(os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")).resolve()

dlt_res: DagsterDltResource = DagsterDltResource()
duckdb_res: DuckDBResource = DuckDBResource(database=str(DUCKDB_PATH))
