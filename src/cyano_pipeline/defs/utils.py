from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from duckdb import DuckDBPyConnection


def ensure_schema(conn: DuckDBPyConnection, schema_name: str) -> None:
    _ = conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def count_table_rows(conn: DuckDBPyConnection, schema_name: str, table_name: str) -> int | None:
    """Get the row count for a table (fully qualified table name)."""
    fq_table_name = f"{schema_name}.{table_name}"

    row_count = conn.execute(
        query=f"SELECT COUNT(*) FROM {fq_table_name};",
    ).fetchone()

    return row_count[0] if row_count else None


def build_table_schema(conn: DuckDBPyConnection, schema_name: str, table_name: str) -> tuple[int, dg.TableSchema]:
    """Build a TableSchema object from database column metadata."""
    query = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = ?
AND table_name = ?
ORDER BY ordinal_position
    """

    schema_info = conn.execute(
        query=query,
        parameters=[schema_name, table_name],
    ).fetchall()

    table_schema = dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=col[0],
                type=col[1],
            )
            for col in schema_info
        ]
    )

    return len(schema_info), table_schema


def build_materialize_result(
    conn: DuckDBPyConnection,
    schema_name: str,
    table_name: str,
    *,
    tags: Mapping[str, str] | None = None,
    check_results: Sequence[dg.AssetCheckResult] | None = None,
    **extra_metadata: Any,
) -> dg.MaterializeResult[Any]:
    """Build standardized MaterializeResult with table metadata, tags and AssetCheckResult."""
    fq_table_name = f"{schema_name}.{table_name}"

    col_count, table_schema = build_table_schema(conn, schema_name, table_name)

    metadata = {
        "row_count": count_table_rows(conn, schema_name, table_name),
        "table": fq_table_name,
        "columns": col_count,
        "schema": table_schema,
        **extra_metadata,
    }

    return dg.MaterializeResult(
        metadata=metadata,
        tags=tags,
        check_results=check_results,
    )
