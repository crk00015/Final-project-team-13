import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(pl):
    Dict = pl.read_csv("vg_data_dictionary.csv")
    Data = pl.read_parquet("vgchartz-2024.parquet")
    return Data, Dict


@app.cell
def _(Data, pl):
    DF1 = Data.filter(pl.col("na_sales").is_not_null() & 
                      pl.col("jp_sales").is_not_null() &
                      pl.col("pal_sales").is_not_null())
    return (DF1,)


@app.cell
def _(DF1, pl):
    AVE = DF1.select(
        pl.col("na_sales").mean(),
        pl.col("jp_sales").mean(),
        pl.col("pal_sales").mean()
    )
    AVE
    return (AVE,)


@app.cell
def _(DF1, pl):
    DF2 = DF1.group_by("title").agg(
        pl.col("na_sales").cast(pl.Float64).sum().alias("na_sales"),
        pl.col("jp_sales").cast(pl.Float64).sum().alias("jp_sales"),
        pl.col("pal_sales").cast(pl.Float64).sum().alias("pal_sales")
    )
    DF2
    return (DF2,)


if __name__ == "__main__":
    app.run()
