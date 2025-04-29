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
def _(Data, pl, px):
    DF1_Filt = Data.filter(pl.col("total_sales").is_not_null())
    DF1 = DF1_Filt.group_by("developer").agg(
        pl.col("total_sales")
        .cast(pl.Int64)
        .mean()
        .alias("Average # of Copys Sold Per Game (millions)")
    ).sort("Average # of Copys Sold Per Game (millions)", descending=True).head(10)
    Fig1 = px.bar(
        DF1,
        x = "developer",
        y = "Average # of Copys Sold Per Game (millions)",
        title = "Average Game Sales Per Developer"
    )
    Fig1.update_layout(
        xaxis_title="Developer"
    )
    Fig1
    return DF1, DF1_Filt, Fig1


@app.cell
def _(DF1_Filt, pl, px):
    DF1_TOT = DF1_Filt.group_by("developer").agg(
        pl.col("total_sales")
        .cast(pl.Int64)
        .sum()
        .alias("Total Copys Sold (millions)")
    ).sort("Total Copys Sold (millions)", descending=True).head(10)
    Fig1_TOT = px.bar(
        DF1_TOT,
        x = "developer",
        y = "Total Copys Sold (millions)",
        title = "Total Sales Per Developer"
    )
    Fig1_TOT.update_layout(
        xaxis_title="Developer"
    )
    Fig1_TOT
    return DF1_TOT, Fig1_TOT


@app.cell
def _(Data, pl, px):
    DF2_Filt = Data.filter(pl.col("critic_score").is_not_null())
    DF2 = DF2_Filt.group_by("developer").agg(
        pl.col("critic_score")
        .cast(pl.Int64)
        .mean()
        .alias("Average Critic Score")
    ).sort("Average Critic Score", descending=True).head(10)
    Fig2 = px.bar(
        DF2,
        x = "developer",
        y = "Average Critic Score",
        title = "Average Critic Score Per Developer"
    )
    Fig2.update_layout(
        xaxis_title="Developer",
        yaxis=dict(range=[8, 10])
    )
    Fig2
    return DF2, DF2_Filt, Fig2


@app.cell
def _(DF2_Filt, pl, px):
    DF2_5 = DF2_Filt.group_by("developer").agg(
        pl.col("critic_score")
        .cast(pl.Int64)
        .mean()
        .alias("Average Critic Score"),
        pl.len()
        .alias("Number of Games")
    ).filter(
        pl.col("Number of Games") > 5
    ).sort("Average Critic Score", descending=True).head(10)
    Fig2_5 = px.bar(
        DF2_5,
        x = "developer",
        y = "Average Critic Score",
        title = "Average Critic Score Per Developer With More Than 5 Entries"
    )
    Fig2_5.update_layout(
        xaxis_title="Developer",
        yaxis=dict(range=[8, 9])
    )
    Fig2_5
    return DF2_5, Fig2_5


if __name__ == "__main__":
    app.run()
