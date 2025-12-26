import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", sql_output="native")

with app.setup:
    import os
    from pathlib import Path
    import duckdb


@app.function
def get_duckdb_path():
    _default_db = Path("data/warehouse.duckdb")
    DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(_default_db))).resolve()
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DUCKDB_PATH


@app.function
def get_duckdb_conn():
    return duckdb.connect(
        database=str(get_duckdb_path()),
        read_only=True,
    )


@app.cell
def _():
    import marimo as mo
    return


if __name__ == "__main__":
    app.run()
