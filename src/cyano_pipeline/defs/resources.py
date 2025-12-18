from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

dlt_res = DagsterDltResource()
duckdb_res = DuckDBResource(database="ddb.duckdb")
