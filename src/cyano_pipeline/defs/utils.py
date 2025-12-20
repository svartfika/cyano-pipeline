from dagster import TableColumn, TableSchema
from duckdb import DuckDBPyConnection


def count_table_rows(conn: DuckDBPyConnection, schema_name: str, table_name: str) -> int | None:
    """Get the row count for a table (fully qualified table name)."""
    fq_table_name = f"{schema_name}.{table_name}"

    row_count = conn.execute(
        query=f"SELECT COUNT(*) FROM {fq_table_name};",
    ).fetchone()

    return row_count[0] if row_count else None


def build_table_schema(conn: DuckDBPyConnection, schema_name: str, table_name: str) -> tuple[int, TableSchema]:
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

    table_schema = TableSchema(
        columns=[
            TableColumn(
                name=col[0],
                type=col[1],
            )
            for col in schema_info
        ]
    )

    return len(schema_info), table_schema
