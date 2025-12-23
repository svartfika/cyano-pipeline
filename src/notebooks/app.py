import marimo

__generated_with = "0.18.4"
app = marimo.App(
    width="full",
    app_title="Water Quality Assesment",
    sql_output="native",
)


@app.cell
def _():
    import marimo as mo

    from algae import app as app_algae
    from bact import app as app_bact
    return app_algae, app_bact, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Weekly Risk Patterns
    > Bathing water monitoring by region
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Algae
    """)
    return


@app.cell
async def _(app_algae):
    embed_algae = await app_algae.embed()
    embed_algae.output
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Bacteria
    """)
    return


@app.cell
async def _(app_bact):
    embed_bact = await app_bact.embed()
    embed_bact.output
    return


if __name__ == "__main__":
    app.run()
