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
            pl.col("release_date").dt.day().alias("day")).with_columns(

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



    data1
    return (data1,)


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
def _(pl):
    datadict = pl.read_csv("vg_data_dictionary.csv")
    datadict
    return (datadict,)


@app.cell
def _(data1, pl):
    best_monthww = data1.group_by("month").agg(
        pl.col("total_sales").sum().round(2).alias("Worldwide_sales")
    ).sort("Worldwide_sales",descending=True)

       
    best_monthww
    return (best_monthww,)


@app.cell
def _(data1, pl):
    best_monthna = data1.group_by("month").agg(
        pl.col("na_sales").sum().round(2).alias("North_American_sales")
    ).sort("North_American_sales",descending=True)

    best_monthna
    return (best_monthna,)


@app.cell
def _(data1, pl):
    best_monthjp = data1.group_by("month").agg(
        pl.col("jp_sales").sum().round(2).alias("Japanese_sales")
    ).sort("Japanese_sales",descending=True)

    best_monthjp
    return (best_monthjp,)


@app.cell
def _(data1, pl):
    best_month_eu_af = data1.group_by("month").agg(
        pl.col("pal_sales").sum().round(2).alias("Europe/Africa_sales")
    ).sort("Europe/Africa_sales",descending=True)

    best_month_eu_af
    return (best_month_eu_af,)


@app.cell
def _(data1, pl):
    best_month_other = data1.group_by("month").agg(
        pl.col("other_sales").sum().round(2).alias("Rest_of_World_sales")
    ).sort("Rest_of_World_sales",descending=True)

    best_month_other
    return (best_month_other,)


@app.cell
def _(best_monthww, px):
    fig1 = px.bar(
        best_monthww,
        x="month", 
        y="Worldwide_sales",
        title= "1973-2024 Worlwide Sales by Month",
        labels= {"Worldwide_sales": "Worldwide Sales (Millions)" }
    )
    fig1

    return (fig1,)


@app.cell
def _(best_monthww, pl, px):
    pie_best_month_ww = best_monthww.with_columns(
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

    fig15 = px.pie(
        pie_best_month_ww, 
        names="month_name",             
        values="Worldwide_sales", 
        title="1973-2024 Worldwide Sales by Month"
    )

    fig15
    return fig15, pie_best_month_ww


@app.cell
def _(best_monthjp, pl, px):
    pie_best_month_jp = best_monthjp.with_columns(
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

    fig16 = px.pie(
        pie_best_month_jp, 
        names="month_name",             
        values="Japanese_sales", 
        title="1973-2024 Japanese Sales by Month",
    
    )

    fig16
    return fig16, pie_best_month_jp


@app.cell
def _(best_monthna, pl, px):
    pie_best_month_na = best_monthna.with_columns(
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

    fig17 = px.pie(
        pie_best_month_na, 
        names="month_name",             
        values="North_American_sales", 
        title="1973-2024 North American Sales by Month"
     


    )

    fig17
    return fig17, pie_best_month_na


@app.cell
def _(best_month_eu_af, pl, px):
    pie_best_month_eu_af = best_month_eu_af.with_columns(
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

    fig18 = px.pie(
        pie_best_month_eu_af, 
        names="month_name",             
        values="Europe/Africa_sales", 
        title="1973-2024 Europe & Africa Sales by Month"
    )

    fig18
    return fig18, pie_best_month_eu_af


@app.cell
def _(best_month_other, pl, px):
    pie_best_month_row = best_month_other.with_columns(
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
    fig19 = px.pie(
        pie_best_month_row, 
        names="month_name",             
        values="Rest_of_World_sales", 
        title="1973-2024 Rest of World Sales by Month"
    )

    fig19
    return fig19, pie_best_month_row


if __name__ == "__main__":
    app.run()
