import marimo

__generated_with = "0.17.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os
    return (json,)


@app.cell
def _():
    import pathlib
    return (pathlib,)


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _():
    import altair as alt
    import marimo as mo
    return alt, mo


@app.cell
def _(pathlib):
    results_dir = pathlib.Path("benchmark_results/")
    return (results_dir,)


@app.cell
def _(json, results_dir):
    data = [
        json.load(f.open())
        for f in results_dir.iterdir()
    ]
    return (data,)


@app.cell
def _(data, pd):
    data_df = pd.DataFrame.from_records([
        {
            "benchmark_name": i["benchmark_name"],
            "runtime_env": i["runner_metadata"]["runtime_env"],
            "step": step,
            "step_value": step_value,
            "parameters": i["parameters"],
        }
        for i in data
        for (step, step_value) in i.get("measurements", {}).items() if step.startswith("step")
    ])
    return (data_df,)


@app.cell
def _(data_df):
    data_df
    return


@app.cell
def _(data_df, mo):
    benchmark_select = mo.ui.dropdown(
        data_df["benchmark_name"].unique(), label="Select Benchmark", value=data_df["benchmark_name"].unique()[0]
    )
    return (benchmark_select,)


@app.cell
def _(benchmark_select, data_df, json, mo):
    filtered_by_name_df = data_df[data_df["benchmark_name"] == benchmark_select.value]
    parameters_select = mo.ui.dropdown(
        filtered_by_name_df["parameters"].apply(lambda x: json.dumps(x)).unique(), 
        label="Select Parameters",
        value=filtered_by_name_df["parameters"].apply(lambda x: json.dumps(x)).unique()[0]
    )
    return filtered_by_name_df, parameters_select


@app.cell
def _(alt, filtered_by_name_df, json, parameters_select):
    filtered_df = filtered_by_name_df[
        filtered_by_name_df["parameters"].apply(lambda x: json.dumps(x)) == parameters_select.value
    ]

    chart = (
        alt.Chart(filtered_df)
        .mark_bar()
        .encode(
            x=alt.X("runtime_env:N", title="Runtime Environment"),
            y=alt.Y("step_value:Q", title="Step Value"),
            color="runtime_env:N",
            column="step:N",
        )
        .properties(width=200)
    )
    return (chart,)


@app.cell
def _(benchmark_select, chart, mo, parameters_select):
    mo.vstack([benchmark_select, parameters_select, chart])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
