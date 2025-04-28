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
    yes = data.group_by(["console", "genre"]).len()

    best = yes.sort(["genre","len"],descending=True)

    Si = best.group_by("genre").first()

    Si = Si.with_columns(
        (pl.col("genre")+ ":" + pl.col("console")).alias("genre_and_console")
    )
    popular_consoles = px.bar(Si, x= "genre_and_console", y= "len")

    popular_consoles

    return Si, best, popular_consoles, yes


if __name__ == "__main__":
    app.run()
