import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import geopandas as gpd
    import marimo as mo
    import pandas as pd


@app.cell
def _():
    data_path = Path(os.environ["DATA_PATH"])
    return (data_path,)


@app.cell
def _(data_path):
    df_comp = pd.read_excel(
        data_path
        / "processing"
        / "2"
        / "Matriz de Competencia - Interés Social - 2026.xlsx",
        sheet_name=1,
    )
    return (df_comp,)


@app.cell
def _(df_comp):
    df_comp
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
