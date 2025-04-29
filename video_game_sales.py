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
    return (data,)


@app.cell
def _(data, pl, px):
    #Sold most titles
    most_sold = data.group_by("title").agg([
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ]).sort("Worldwide_sales",descending=True)

    shorten = most_sold.head(15)

    most_sold_chart = px.bar(shorten,x= "title", y="Worldwide_sales", title = "Worldwide Sales of the Top 15 Game Titles")
    most_sold,most_sold_chart
    return most_sold, most_sold_chart, shorten


if __name__ == "__main__":
    app.run()
