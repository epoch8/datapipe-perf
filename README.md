# Main entities:

## Scenario

Scenario - is a specific way to run datapipe on a specific pipeline. Each
scenario defines it's own steps which it reports in measurements.

Each scenario is defined with a python module, which is executed with `python
-m` and benchmarking time

## Benchmark preset

Benchmark preset is a set of parameters for a specific scenario. Typically you
define several presets to control the "size" of a benchmark run

Each benchmark preset has a name

## Runtime environment

Runtime environment is a set of libraries, tools and services which define which
specific version of datapipe is running with which database on which executor on
which version of python etc

# Configuration

Scenarios + benchmark presets are configured in a file `bench_matrix.toml`

Runtime environments are defined in `pixi.toml` and are managed by pixi
