import marimo

__generated_with = "0.10.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


app._unparsable_cell(
    r"""
    What video game month release has the best sales worlwide
    what is north americas best month, japan , and africas
    compare to japan month release.
    """,
    name="_"
)


@app.cell
def _(pl):
    data1 = pl.read_parquet("vgchartz-2024.parquet").with_columns( 
            pl.col("release_date").dt.year().alias("year"),
            pl.col("release_date").dt.month().alias("month"),
            pl.col("release_date").dt.day().alias("day"),
        ).with_columns(
            pl.when(pl.col("month") == 1).then(pl.lit("January"))
            .when(pl.col("month") == 2).then(pl.lit("February"))
            .when(pl.col("month") == 3).then(pl.lit("March"))
            .when(pl.col("month") == 4).then(pl.lit("April"))
            .when(pl.col("month") == 5).then(pl.lit("May"))
            .when(pl.col("month") == 6).then(pl.lit("June"))
            .when(pl.col("month") == 7).then(pl.lit("July"))
            .when(pl.col("month") == 8).then(pl.lit("August"))
            .when(pl.col("month") == 9).then(pl.lit("September"))
            .when(pl.col("month") == 10).then(pl.lit("October"))
            .when(pl.col("month") == 11).then(pl.lit("November"))
            .when(pl.col("month") == 12).then(pl.lit("December"))
            .otherwise(pl.lit("Invalid month"))
            .alias("month_name")
        )
    data1
    return (data1,)


@app.cell
def _(pl):
    datadict = pl.read_csv("vg_data_dictionary.csv")
    datadict
    return (datadict,)


@app.cell
def _(data1, pl):
    best_monthww = data1.group_by("month").agg([
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ]).sort("Worldwide_sales",descending=True)


    best_monthww
    return (best_monthww,)


@app.cell
def _(data1, pl):
    best_monthna = data1.group_by("month").agg([
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ]).sort("Worldwide_sales",descending=True)


    best_monthna
    return (best_monthna,)


@app.cell
def _(diamonds, px):
    # Scatter plot with adjusted scales
    fig = px.scatter(
        diamonds.sample(n=1000), 
        x="carat", 
        y="price",
        color="depth",
        title="Diamond Price vs Weight with Color Scale",
        labels={
            "carat": "Weight (carats)",
            "price": "Price (USD)",
            "depth": "Depth Percentage"
        }
    )

    # Update color scale
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Depth %",
            tickvals=[55, 60, 65, 70],
            ticktext=["55%", "60%", "65%", "70%"]
        )
    )

    # Log scale for price
    fig.update_yaxes(type="log")

    fig.show()
    return (fig,)


if __name__ == "__main__":
    app.run()
