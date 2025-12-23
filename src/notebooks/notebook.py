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
        return next(p for p in Path(__file__).resolve().parents if (p / root_marker).exists())

    DUCKDB_PATH: Path = Path(os.getenv("DUCKDB_PATH", "data/warehouse.duckdb"))

    conn = duckdb.connect(
        database=str(get_root() / DUCKDB_PATH),
        read_only=False,
    )
    return (conn,)


@app.cell
def _(conn, mo):
    df_conn = mo.sql(
        f"""
        SELECT * FROM mart.mart_weekly_bloom_metrics

        WHERE confidence IN ('medium', 'high')
        	OR n_samples >= 5;
        """,
        output=False,
        engine=conn
    )
    return (df_conn,)


@app.cell
def _(df_conn):
    df = df_conn.df()

    df["water_type"] = df["water_type"].str.capitalize()
    return (df,)


@app.cell
def _(df):
    import altair as alt

    base = alt.Chart(df).encode(
        alt.X("week", type="ordinal").axis(labelAngle=0).title("Week"),
        alt.Y("region", type="ordinal").sort("-x").title("Region"),
    )

    heatmap = base.mark_rect().encode(
        color=alt.Color(
            "bloom_rate_pct",
            type="quantitative",
        )
        .scale(
            domain=[0, 0.5, 5, 10, 20],
            range=["#81c4e7", "#fff59d", "#ffb74d", "#ff7043", "#e53935"],
            clamp=True,
        )
        .title(["Bloom Risk (3-week avg %)"]),
        opacity=alt.Opacity("confidence", type="nominal")
        .scale(
            domain=["low", "medium", "high"],
            range=[0.6, 0.85, 1.0],
        )
        .legend(None),
        tooltip=[
            alt.Tooltip("region:N", title="Region"),
            alt.Tooltip("water_type:N", title="Water Type"),
            alt.Tooltip("week:O", title="Week"),
            alt.Tooltip("bloom_rate_pct:Q", title="3-week avg", format=".1f"),
            alt.Tooltip("bloom_rate_week_pct:Q", title="This week", format=".1f"),
            alt.Tooltip("n_samples:Q", title="Samples"),
            alt.Tooltip("n_locations:Q", title="Locations"),
            alt.Tooltip("confidence:N", title="Confidence ⚠️"),
        ],
    )

    chart = (
        heatmap.facet(column=alt.Column("water_type:N").title(None))
        .resolve_scale(color="shared")
        .configure_view(fill="#f8f9fa")
        .properties(
            title=alt.Title(
                text="Weekly Algae Bloom Risk",
                subtitle="3-week rolling average",
            )
        )
    )

    chart
    return


if __name__ == "__main__":
    app.run()
