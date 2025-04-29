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
    data1 = pl.read_parquet("vgchartz-2024.parquet").with_columns( 
                pl.col("release_date").dt.year().alias("Year"),
                pl.col("release_date").dt.month().alias("Month"),
                pl.col("release_date").dt.day().alias("Day"),
    )
    data1
    return (data1,)


@app.cell
def _(data1, pl):
    data11 = data1.filter(
        pl.col("total_sales").is_not_null()
    )
    data11
    return (data11,)


@app.cell
def _(pl):
    data2 = pl.read_csv("vg_data_dictionary.csv")
    data2
    return (data2,)


@app.cell
def _(data11, pl):
    best_year = data11.group_by("Year").agg(
        pl.col("total_sales").sum().round(2).alias("Worldwide Sales")
    ).sort("Worldwide Sales",descending=True)




    best_year
    return (best_year,)


@app.cell
def _(best_year, px):
    best_year_chart = px.bar(
        best_year,
        x="Year",
        y="Worldwide Sales",
        title="Growth of Video Game Industry By Year",
    
    )
    best_year
    best_year_chart
    return (best_year_chart,)


if __name__ == "__main__":
    app.run()
