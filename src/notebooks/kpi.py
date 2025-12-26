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
    rel_summary = mo.sql(
        f"""
        SELECT
            COUNT(DISTINCT f.id) AS total_locations,
            COUNT(*) AS total_samples,

            --- algae
            COUNT(*) FILTER (WHERE f.has_algae_data) AS algae_samples,
            COUNT(*) FILTER (WHERE f.is_bloom) AS bloom_events,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_bloom) 
            	/ NULLIF(COUNT(*) FILTER (WHERE f.has_algae_data), 0), 1) AS bloom_rate_pct,

            --- bacteria
            COUNT(*) FILTER (WHERE f.has_bacteria_data) AS bacteria_samples,
            COUNT(*) FILTER (WHERE f.is_bacteria_fail) AS bacteria_fails,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_bacteria_fail) 
            	/ NULLIF(COUNT(*) FILTER (WHERE f.has_bacteria_data), 0), 1) AS bacteria_fail_rate_pct

        FROM core.fact_water_samples f
        INNER JOIN core.dim_bathing_waters d ON f.id = d.id
        WHERE f.sample_year = EXTRACT(YEAR FROM CURRENT_DATE);
        """,
        output=False,
        engine=conn
    )
    return (rel_summary,)


@app.cell
def _(rel_summary):
    df_summary = rel_summary.df()
    return (df_summary,)


@app.cell
def _(df_summary, mo):
    ## Water Safety & Sampling Overview

    row = df_summary.iloc[0]

    total_stats = mo.stat(
        value=f"{int(row['total_samples']):,}",  # Adds comma separator (e.g., 1,200)
        label="Total Samples",
        caption=f"Across {row['total_locations']} locations"
    )

    algae_stats = mo.stat(
        value=f"{row['bloom_rate_pct']}%",
        label="Algae Bloom Rate",
        caption=f"{int(row['bloom_events'])} bloom events confirmed",
    )

    bacteria_stats = mo.stat(
        value=f"{row['bacteria_fail_rate_pct']}%",
        label="Bacteria Fail Rate",
        caption=f"{int(row['bacteria_fails'])} safety failures recorded",
    )

    # 3. Display them horizontally
    mo.hstack(
        [total_stats, algae_stats, bacteria_stats], 
        justify="start", 
        gap="2rem"
    )
    return


if __name__ == "__main__":
    app.run()
