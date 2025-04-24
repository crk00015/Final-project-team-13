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
def _(data2):
    data2
    return


app._unparsable_cell(
    r"""
    CD = 
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
