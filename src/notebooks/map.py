import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", sql_output="native")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import os
    from pathlib import Path
    import duckdb


    def get_root(root_marker="pyproject.toml") -> Path:
        return next(
            p
            for p in Path(__file__).resolve().parents
            if (p / root_marker).exists()
        )


    DUCKDB_PATH: Path = Path(os.getenv("DUCKDB_PATH", "data/warehouse.duckdb"))

    conn = duckdb.connect(
        database=str(get_root() / DUCKDB_PATH),
        read_only=True,
    )
    return (conn,)


@app.cell
def _(conn, mo):
    rel_map = mo.sql(
        f"""
        SELECT
            id,
            name,
            municipality,
            county,
            nuts2_name AS region,
            water_type_status_code AS water_type,
            latitude,
            longitude

        FROM core.dim_bathing_waters

        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
        ;
        """,
        output=False,
        engine=conn
    )
    return (rel_map,)


@app.cell
def _(rel_map):
    df_map = rel_map.df()

    df = df_map.dropna(subset=['latitude', 'longitude'])
    return (df,)


@app.cell
def _(df):
    import leafmap

    m = leafmap.Map(
        center=[df.latitude.mean(), df.longitude.mean()], 
        zoom=6,
        draw_control=False,
        measure_control=False
    )

    m.add_points_from_xy(
        df, 
        x="longitude", 
        y="latitude", 
        layer_name="Bathing Waters"
    )

    m
    return


if __name__ == "__main__":
    app.run()
