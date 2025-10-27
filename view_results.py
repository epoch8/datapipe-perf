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


@app.cell
def _(data, pd):
    # Prepare data for scaling chart
    scaling_data = []
    for i in data:
        if not i.get("success", False):
            continue
        params = i.get("parameters", {})
        measurements = i.get("measurements", {})

        # Extract data size parameter
        data_size = params.get("num_records") or params.get("initial_records")
        if data_size is None:
            continue

        for step, step_value in measurements.items():
            if step.startswith("step"):
                scaling_data.append({
                    "benchmark_name": i["benchmark_name"],
                    "runtime_env": i["runner_metadata"]["runtime_env"],
                    "data_size": data_size,
                    "step": step,
                    "step_value": step_value,
                })

    scaling_df = pd.DataFrame(scaling_data)
    return (scaling_df,)


@app.cell
def _(scaling_df):
    scaling_df
    return


@app.cell
def _(mo, scaling_df):
    # Dropdown for selecting benchmark for scaling chart
    scaling_benchmark_select = mo.ui.dropdown(
        scaling_df["benchmark_name"].unique(),
        label="Select Benchmark for Scaling Analysis",
        value=scaling_df["benchmark_name"].unique()[0]
    )
    return (scaling_benchmark_select,)


@app.cell
def _(mo, scaling_benchmark_select, scaling_df):
    # Dropdown for selecting step
    filtered_scaling_df = scaling_df[scaling_df["benchmark_name"] == scaling_benchmark_select.value]
    scaling_step_select = mo.ui.dropdown(
        filtered_scaling_df["step"].unique(),
        label="Select Step",
        value=filtered_scaling_df["step"].unique()[0]
    )
    return filtered_scaling_df, scaling_step_select


@app.cell
def _(alt, filtered_scaling_df, scaling_step_select):
    # Create scaling chart
    step_scaling_df = filtered_scaling_df[filtered_scaling_df["step"] == scaling_step_select.value]

    scaling_chart = (
        alt.Chart(step_scaling_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("data_size:Q", title="Data Size (records)", scale=alt.Scale(type="log")),
            y=alt.Y("step_value:Q", title="Time (seconds)"),
            color=alt.Color("runtime_env:N", title="Runtime Environment"),
            tooltip=["data_size:Q", "step_value:Q", "runtime_env:N"]
        )
        .properties(width=600, height=400, title="Scaling: Time vs Data Size")
    )
    return (scaling_chart, step_scaling_df)


@app.cell
def _(mo, scaling_benchmark_select, scaling_chart, scaling_step_select):
    mo.vstack([
        mo.md("## Scaling Analysis"),
        scaling_benchmark_select,
        scaling_step_select,
        scaling_chart
    ])
    return


if __name__ == "__main__":
    app.run()
