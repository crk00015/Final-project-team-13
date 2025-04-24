import marimo

__generated_with = "0.10.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(pl):
    data = pl.read_parquet("vgchartz-2024.parquet")
    data
    return (data,)


@app.cell
def _(data, pl):
    consoles = data.group_by("console").agg([
        pl.col("genre").alias("genre")
    ])

    do = consoles.with_columns([
        pl.when(pl.col("console") )
    ])
    consoles
    return consoles, do


if __name__ == "__main__":
    app.run()
