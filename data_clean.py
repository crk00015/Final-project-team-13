import marimo

__generated_with = "0.11.18"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(pl):
    data2 = pl.read_csv("vgchartz-2024.csv")
    return (data2,)


@app.cell
def _(data2, pl):
    CD = data2.with_columns(
        pl.col("release_date").str.to_date().alias("release_date"),
        pl.col("last_update").str.to_date().alias("last_update"),
    )
    return (CD,)


@app.cell
def _(CD):
    CD.write_parquet("vgchartz-2024.parquet")
    return


if __name__ == "__main__":
    app.run()
