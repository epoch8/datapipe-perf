#!/usr/bin/env python3
"""
Benchmark Runner

Reads benchmark configuration from bench_matrix.toml and runs benchmarks
with different parameter combinations. Results are saved to JSON files.

Usage:
    python run_benchmarks.py [--benchmark BENCHMARK_NAME] [--output-dir OUTPUT_DIR]

Examples:
    # Run all benchmarks
    python run_benchmarks.py

    # Run specific benchmark
    python run_benchmarks.py --benchmark incremental_transform_performance

    # Save results to specific directory
    python run_benchmarks.py --output-dir results/
"""

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import psutil
import toml


def get_system_info() -> Dict[str, Any]:
    """Get system information including Python version, CPU, and RAM."""
    try:
        # Python version
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        # CPU information
        cpu_count = psutil.cpu_count(logical=True)
        cpu_count_physical = psutil.cpu_count(logical=False)
        cpu_freq = psutil.cpu_freq()
        cpu_freq_current = cpu_freq.current if cpu_freq else None

        # Memory information (in GB)
        memory = psutil.virtual_memory()
        memory_total_gb = round(memory.total / (1024**3), 2)
        memory_available_gb = round(memory.available / (1024**3), 2)

        # Platform information
        system_info = {
            "python_version": python_version,
            "platform": platform.system(),
            "platform_release": platform.release(),
            "architecture": platform.machine(),
            "cpu_count_logical": cpu_count,
            "cpu_count_physical": cpu_count_physical,
            "cpu_freq_mhz": cpu_freq_current,
            "memory_total_gb": memory_total_gb,
            "memory_available_gb": memory_available_gb,
            "memory_usage_percent": memory.percent,
        }

        return system_info

    except Exception as e:
        return {
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "system_info_error": str(e),
        }


class BenchmarkRunner:
    def __init__(
        self, config_file: str = "bench_matrix.toml", output_dir: str | None = None
    ):
        self.config_file = Path(config_file)
        self.config = self._load_config()
        self.output_dir = Path(output_dir) if output_dir else Path("benchmark_results")
        self.output_dir.mkdir(exist_ok=True)

    def _load_config(self) -> Dict[str, Any]:
        """Load benchmark configuration from TOML file."""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_file}")

        with open(self.config_file, "r") as f:
            return toml.load(f)

    def _expand_path(self, path_str: str) -> str:
        """Expand ~ and environment variables in paths."""
        return os.path.expandvars(os.path.expanduser(path_str))

    def run_benchmark(
        self, benchmark_name: str, parameters: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """
        Run a single benchmark with given parameters.

        Args:
            benchmark_name: Name of the benchmark
            parameters: Parameter dictionary for the benchmark

        Returns:
            Benchmark result dictionary or None if failed
        """
        benchmark_config = self.config["benchmarks"][benchmark_name]
        module_name = benchmark_config["module"]

        # Prepare environment variables
        env = os.environ.copy()
        for key, value in parameters.items():
            env[key.upper()] = str(value)

        # Add global settings to environment
        if "settings" in self.config:
            for key, value in self.config["settings"].items():
                env_key = key.upper()
                if env_key not in env:  # Don't override parameter-specific values
                    env[env_key] = str(value)

        print(
            f"Running {benchmark_name} (module: {module_name}) with parameters: {parameters}"
        )

        result = None

        try:
            # Run the benchmark as a module
            result = subprocess.run(
                [sys.executable, "-m", module_name],
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse JSON output
            benchmark_result = json.loads(result.stdout)

            # Add runner metadata including system info
            benchmark_result["runner_metadata"] = {
                "config_file": str(self.config_file),
                "run_timestamp": datetime.now().isoformat(),
            }

            # Add system info to metadata if not already present
            if "metadata" not in benchmark_result:
                benchmark_result["metadata"] = {}
            if "system_info" not in benchmark_result["metadata"]:
                benchmark_result["metadata"]["system_info"] = get_system_info()

            return benchmark_result

        except subprocess.CalledProcessError as e:
            print(f"Error running {benchmark_name}: {e}")
            print(f"Stdout: {e.stdout}")
            print(f"Stderr: {e.stderr}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output from {benchmark_name}: {e}")
            print(f"Raw output: {result.stdout if result else '-'}")
            return None

    def run_benchmark_suite(self, benchmark_name: str) -> List[Dict[str, Any]]:
        """
        Run all parameter combinations for a benchmark.

        Args:
            benchmark_name: Name of the benchmark to run

        Returns:
            List of benchmark results
        """
        if benchmark_name not in self.config["benchmarks"]:
            raise ValueError(f"Unknown benchmark: {benchmark_name}")

        benchmark_config = self.config["benchmarks"][benchmark_name]
        results = []

        print(f"\n{'=' * 60}")
        print(f"Running benchmark suite: {benchmark_name}")
        print(f"{'=' * 60}")

        for i, parameters in enumerate(benchmark_config["parameters"], 1):
            print(f"\n--- Run {i}/{len(benchmark_config['parameters'])} ---")

            result = self.run_benchmark(benchmark_name, parameters)
            if result:
                results.append(result)

                # Save individual result
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                param_str = "_".join(f"{k}{v}" for k, v in parameters.items())
                filename = f"{benchmark_name}_{param_str}_{timestamp}.json"

                result_file = self.output_dir / filename
                with open(result_file, "w") as f:
                    json.dump(result, f, indent=2)

                print(f"✓ Results saved to: {result_file}")
            else:
                print("✗ Benchmark failed")

        return results

    def run_all_benchmarks(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Run all benchmarks defined in the configuration.

        Returns:
            Dictionary mapping benchmark names to their results
        """
        all_results = {}

        for benchmark_name in self.config["benchmarks"]:
            try:
                results = self.run_benchmark_suite(benchmark_name)
                all_results[benchmark_name] = results
            except Exception as e:
                print(f"Error running benchmark suite {benchmark_name}: {e}")
                all_results[benchmark_name] = []

        # Save summary
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = self.output_dir / f"benchmark_summary_{timestamp}.json"

        summary = {
            "run_info": {
                "timestamp": datetime.now().isoformat(),
                "config_file": str(self.config_file),
            },
            "results": all_results,
        }

        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"\n{'=' * 60}")
        print("All benchmarks completed!")
        print(f"Summary saved to: {summary_file}")
        print(f"{'=' * 60}")

        return all_results


def main():
    parser = argparse.ArgumentParser(description="Run datapipe benchmarks")
    parser.add_argument("--benchmark", help="Run specific benchmark only")
    parser.add_argument(
        "--output-dir",
        default="benchmark_results",
        help="Directory to save results (default: benchmark_results)",
    )
    parser.add_argument(
        "--config",
        default="bench_matrix.toml",
        help="Configuration file (default: bench_matrix.toml)",
    )

    args = parser.parse_args()

    try:
        runner = BenchmarkRunner(args.config, args.output_dir)

        if args.benchmark:
            # Run specific benchmark
            runner.run_benchmark_suite(args.benchmark)
        else:
            # Run all benchmarks
            runner.run_all_benchmarks()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
