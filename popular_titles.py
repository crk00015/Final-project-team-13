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
                      pl.col("pal_sales").is_not_null()
                     )
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


@app.cell
def _(DF2, pl):
    DF3 = DF2.with_columns(
        (pl.col('na_sales') / pl.col('jp_sales')
        ).alias('NA to JP'),
        (pl.col('na_sales') / pl.col('pal_sales')
        ).alias('NA to PAL'),
        (pl.col('jp_sales') / pl.col('na_sales')
        ).alias('JP to NA'),
        (pl.col('jp_sales') / pl.col('pal_sales')
        ).alias('JP to PAL'),
        (pl.col('pal_sales') / pl.col('na_sales')
        ).alias('PAL to NA'),
        (pl.col('pal_sales') / pl.col('jp_sales')
        ).alias('PAL to JP')
    )
    return (DF3,)


@app.cell
def _(DF3):
    DF4 = DF3.select("title", "NA to JP", "NA to PAL", "JP to NA", "JP to PAL", "PAL to NA", "PAL to JP")
    DF4
    return (DF4,)


@app.cell
def _(DF4, pl):
    DF5 = DF4.select("title", "NA to PAL", "NA to JP")
    DF5_FILT = DF5.filter(
        pl.col("NA to PAL").is_finite() & 
        pl.col("NA to JP").is_finite()
    )
    DF5_FILT
    return DF5, DF5_FILT


if __name__ == "__main__":
    app.run()
