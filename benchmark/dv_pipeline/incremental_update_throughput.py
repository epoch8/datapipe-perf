#!/usr/bin/env python3
"""
Incremental Update Throughput Benchmark

This benchmark measures the throughput of incremental updates with different
ratios of initial vs new data. It focuses on how processing time scales
with the size of the existing dataset when adding new records.

Environment Variables:
    INITIAL_RECORDS: Number of initial records (required)
    NEW_RECORDS: Number of new records to add (required)
    CHUNK_SIZE: Batch processing chunk size (default: 100)
    BATCH_SIZE: Data generation batch size (default: 50000)

Output: JSON with benchmark results
"""

import sys

from datapipe.compute import run_steps
from pydantic_settings import BaseSettings

from benchmark.common import (
    benchmark_timer,
    output_benchmark_result,
    prepare_large_dataset,
)
from benchmark.dv_pipeline.pipeline import get_app


class BenchmarkSettings(BaseSettings):
    initial_records: int = 100
    new_records: int = 100
    chunk_size: int = 100
    batch_size: int = 50000


benchmark_settings = BenchmarkSettings()  # type: ignore


def main(benchmark_settings: BenchmarkSettings = benchmark_settings) -> None:
    """Run the incremental update throughput benchmark."""

    app = get_app()

    try:
        source = app.ds.get_table("perf_source")
        target = app.ds.get_table("perf_target")

        # Step 1: Load initial data
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.initial_records,
                "id",
                batch_size=benchmark_settings.batch_size,
            ):
                source.store_chunk(data_chunk)
        step1_time = get_step1_time()

        # Step 2: Initial processing (full run)
        with benchmark_timer() as get_step2_time:
            run_steps(app.ds, app.steps)
        step2_time = get_step2_time()

        # Verify initial processing
        initial_count = len(target.get_data())
        assert initial_count == benchmark_settings.initial_records, (
            f"Expected {benchmark_settings.initial_records}, got {initial_count}"
        )

        # Step 3: Add new records
        with benchmark_timer() as get_step3_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.new_records,
                "id",
                start_idx=benchmark_settings.initial_records,
                batch_size=benchmark_settings.batch_size,
            ):
                source.store_chunk(data_chunk)
        step3_time = get_step3_time()

        # Step 4: Incremental processing
        with benchmark_timer() as get_step4_time:
            run_steps(app.ds, app.steps)
        step4_time = get_step4_time()

        # Verify final processing
        final_count = len(target.get_data())
        expected_final = (
            benchmark_settings.initial_records + benchmark_settings.new_records
        )
        assert final_count == expected_final, (
            f"Expected {expected_final}, got {final_count}"
        )

        # Calculate metrics
        total_time = step1_time + step2_time + step3_time + step4_time
        incremental_throughput = (
            benchmark_settings.new_records / step4_time if step4_time > 0 else 0
        )
        initial_throughput = (
            benchmark_settings.initial_records / step2_time if step2_time > 0 else 0
        )

        # Calculate efficiency ratios
        data_ratio = (
            benchmark_settings.new_records / benchmark_settings.initial_records
            if benchmark_settings.initial_records > 0
            else 0
        )
        time_ratio = step4_time / step2_time if step2_time > 0 else 0

        measurements = {
            "step1_load_initial_time": step1_time,
            "step2_transform_initial_time": step2_time,
            "step3_load_incremental_time": step3_time,
            "step4_transform_incremental_time": step4_time,
            "total_time": total_time,
            "incremental_throughput_rps": incremental_throughput,
            "initial_throughput_rps": initial_throughput,
            "initial_records_processed": initial_count,
            "final_records_processed": final_count,
            "data_ratio": data_ratio,
            "time_ratio": time_ratio,
            "efficiency_score": data_ratio / time_ratio if time_ratio > 0 else 0,
        }

        metadata = {
            "benchmark_description": "Measures incremental update throughput with different data ratios",
            "steps": [
                "Load initial dataset",
                "Initial processing (full run)",
                "Add new records",
                "Incremental processing",
            ],
            "metrics_explanation": {
                "data_ratio": "new_records / initial_records",
                "time_ratio": "incremental_time / initial_time",
                "efficiency_score": "data_ratio / time_ratio (higher is better)",
            },
        }

        output_benchmark_result(
            "incremental_update_throughput",
            benchmark_settings.model_dump(),
            measurements,
            metadata,
        )

    finally:
        # Clean up database connection
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Benchmark failed: {e}", file=sys.stderr)
        sys.exit(1)
