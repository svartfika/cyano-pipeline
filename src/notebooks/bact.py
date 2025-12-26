import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", sql_output="native")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from duck import get_duckdb_conn
    conn = get_duckdb_conn()
    return (conn,)


@app.cell
def _(conn, mo):
    rel_bact = mo.sql(
        f"""
        SELECT * FROM mart.mart_weekly_bacteria_metrics

        WHERE confidence IN ('medium', 'high')
            OR n_samples >= 5;
        """,
        output=False,
        engine=conn
    )
    return (rel_bact,)


@app.cell
def _(rel_bact):
    df_bact = rel_bact.df()

    df_bact["water_type"] = df_bact["water_type"].str.capitalize()
    return (df_bact,)


@app.cell
def _(df_bact):
    import altair as alt

    base = alt.Chart(df_bact).encode(
        alt.X("week", type="ordinal").axis(labelAngle=0).title("Week"),
        alt.Y("region", type="ordinal").sort("-x").title("Region"),
    )

    heatmap = base.mark_rect().encode(
        color=alt.Color(
            "fail_rate_pct",
            type="quantitative",
        )
        .scale(
            domain=[0, 1, 5, 10, 20],
            range=["#c8e6c9", "#fff59d", "#ffb74d", "#ff7043", "#e53935"],
            clamp=True,
        )
        .title("Fail rate (%)"),
        opacity=alt.Opacity("confidence", type="nominal")
        .scale(
            domain=["low", "medium", "high"],
            range=[0.6, 0.85, 1.0],
        )
        .legend(None),
        tooltip=[
            alt.Tooltip("region:N", title="Region"),
            alt.Tooltip("water_type:N", title="Water type"),
            alt.Tooltip("week:O", title="Week"),
            alt.Tooltip("fail_rate_pct:Q", title="Rolling %", format=".1f"),
            alt.Tooltip("warning_rate_pct:Q", title="Warning %", format=".1f"),
            alt.Tooltip("fail_rate_week_pct:Q", title="Week %", format=".1f"),
            alt.Tooltip("n_samples:Q", title="Samples"),
            alt.Tooltip("n_locations:Q", title="Locations"),
            alt.Tooltip("confidence:N", title="Confidence"),
        ],
    )

    chart = (
        heatmap.facet(column=alt.Column("water_type:N").title(None))
        .resolve_scale(color="shared")
        .configure_view(fill="#f8f9fa")
        .properties(
            title=alt.Title(
                text="Bacteria Risk",
                subtitle="3-week rolling fail rate",
            )
        )
    )

    chart
    return


if __name__ == "__main__":
    app.run()
