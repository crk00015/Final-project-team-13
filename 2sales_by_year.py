import marimo

__generated_with = "0.10.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(pl):
    data1 = pl.read_parquet("vgchartz-2024.parquet").with_columns( 
                pl.col("release_date").dt.year().alias("year"),
                pl.col("release_date").dt.month().alias("month"),
                pl.col("release_date").dt.day().alias("day"),
    )
    data1
    return (data1,)


@app.cell
def _(pl):
    data2 = pl.read_csv("vg_data_dictionary.csv")
    data2
    return (data2,)


@app.cell
def _(data1, pl):
    best_year = data1.group_by("year").agg([
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ]).sort("Worldwide_sales",descending=True)


    best_year
    return (best_year,)


@app.cell
def _(best_year, px):
    best_year_chart = px.bar(x = "year",
                             y ="Worldwide_sales",
                             title = "Growth of Video Game Industry By Year"
                            
                            )

    best_year
    best_year_chart
    return (best_year_chart,)


if __name__ == "__main__":
    app.run()
