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
def _(Dict):
    Dict
    return


@app.cell
def _(Data):
    Data
    return


if __name__ == "__main__":
    app.run()
