import os
from pathlib import Path

from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

_default_db = Path("data/warehouse.duckdb")
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(_default_db))).resolve()

DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

dlt_res: DagsterDltResource = DagsterDltResource()
duckdb_res: DuckDBResource = DuckDBResource(database=str(DUCKDB_PATH))
