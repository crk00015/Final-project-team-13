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
    data1 = pl.read_parquet(\"vgchartz-2024.parquet\").with_columns( 
                pl.col(\"release_date\").dt.year().alias(\"year\"),
                pl.col(\"release_date\").dt.month().alias(\"month\"),
                pl.col(\"release_date\").dt.day().alias(\"day\"),
    )
    .with_columns(pl.when(pl.col(\"month\") == 1).then(pl.lit(\"January\"))
                .when(pl.col(\"month\") == 2).then(pl.lit(\"February\"))
                .when(pl.col(\"month\") == 3).then(pl.lit(\"March\"))
                .when(pl.col(\"month\") == 4).then(pl.lit(\"April\"))
                .when(pl.col(\"month\") == 5).then(pl.lit(\"May\"))
                .when(pl.col(\"month\") == 6).then(pl.lit(\"June\"))
                .when(pl.col(\"month\") == 7).then(pl.lit(\"July\"))
                .when(pl.col(\"month\") == 8).then(pl.lit(\"August\"))
                .when(pl.col(\"month\") == 9).then(pl.lit(\"September\"))
                .when(pl.col(\"month\") == 10).then(pl.lit(\"October\"))
                .when(pl.col(\"month\") == 11).then(pl.lit(\"November\"))
                .when(pl.col(\"month\") == 12).then(pl.lit(\"December\"))
                .otherwise(pl.lit(\"Invalid month\"))
                .alias(\"month\")
    )
    data1
    """,
    name="_"
)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
