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
            pl.col("release_date").dt.year().alias("year"),
            pl.col("release_date").dt.month().alias("month"),
            pl.col("release_date").dt.day().alias("day"))

    data1
    return (data1,)


@app.cell
def _():
    return


@app.cell
def _(pl):
    datadict = pl.read_csv("vg_data_dictionary.csv")
    datadict
    return (datadict,)


@app.cell
def _(data1, pl):
    best_monthww = data1.group_by("month").agg([
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ]).sort("Worldwide_sales",descending=True).with_columns(
    
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
            .alias("month_name"))

    
       

    best_monthww
    return (best_monthww,)


@app.cell
def _(data1, pl):
    region = data1.group_by("month").agg(
        pl.col("total_sales").sum().alias("Worldwide"),
        pl.col("na_sales").sum().alias("North_America_sales"),
        pl.col("jp_sales").sum().alias("Japan_sales"),
        pl.col("pal_sales").sum().alias("Europe/Africa_sales"),
        pl.col("other_sales").sum().alias("Rest_of_World_sales"))
    return (region,)


@app.cell
def _(data1, pl):
    best_monthna = data1.group_by("month").agg([
        pl.col("na_sales").sum().round(2).alias("North_American_sales")
    ]).sort("North_American_sales",descending=True).with_columns(
    
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
            .alias("month_name"))

    best_monthna
    return (best_monthna,)


@app.cell
def _(data1, pl):
    best_monthjp = data1.group_by("month").agg([
        pl.col("jp_sales").sum().round(2).alias("Japanese_sales")
    ]).sort("Japanese_sales",descending=True).with_columns(

    
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
            .alias("month_name"))

    best_monthjp
    return (best_monthjp,)


@app.cell
def _(data1, pl):
    best_monthpal = data1.group_by("month").agg([
        pl.col("pal_sales").sum().round(2).alias("Europe/Africa_sales")
    ]).sort("Europe/Africa_sales",descending=True).with_columns(

    
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
            .alias("month_name"))


    best_monthpal
    return (best_monthpal,)


@app.cell
def _(data1, pl):
    best_month_other = data1.group_by("month").agg([
        pl.col("other_sales").sum().round(2).alias("Rest_of_World_sales")
    ]).sort("Rest_of_World_sales",descending=True).with_columns(

    
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
            .alias("month_name"))


    best_month_other
    return (best_month_other,)


@app.cell
def _(best_monthww, px):
    fig2 = px.bar(
        best_monthww,
        x="month", 
        y="Worldwide_sales",
        facet_col= "japenese_sales",
        category_orders ={"region": ["North_America_sales", "Japanese_sales", "Europe/Africa_sales", "Rest_of_World_sales"]}
    )
    fig2
    return (fig2,)


if __name__ == "__main__":
    app.run()
