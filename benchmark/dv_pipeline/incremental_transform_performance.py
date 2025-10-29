#!/usr/bin/env python3
"""
Incremental Transform Performance Benchmark

This benchmark measures the performance of incremental data processing
by testing the time it takes to process new data after an initial dataset
has already been processed.

Environment Variables:
    NUM_RECORDS: Number of initial records to process (required)
    NEW_RECORDS: Number of new records to add for incremental processing (default: 1000)
    CHUNK_SIZE: Batch processing chunk size (default: 100)
    BATCH_SIZE: Data generation batch size (default: 50000)

Output: JSON with benchmark results
"""

import sys

from datapipe.compute import run_steps
from datapipe.run_config import RunConfig
from pydantic_settings import BaseSettings

from benchmark.common import (
    benchmark_timer,
    output_benchmark_result,
    prepare_large_dataset,
)
from benchmark.dv_pipeline.pipeline import get_app


class BenchmarkSettings(BaseSettings):
    num_records: int = 1000
    new_records: int = 1000
    chunk_size: int = 100
    batch_size: int = 50000


benchmark_settings = BenchmarkSettings()  # type: ignore


def main(benchmark_settings: BenchmarkSettings = benchmark_settings) -> None:
    """Run the incremental transform performance benchmark."""

    app = get_app()
    run_config = RunConfig(labels={"use_offset_optimization": True})

    try:
        source = app.ds.get_table("perf_source")
        target = app.ds.get_table("perf_target")

        # Step 1: Load initial data
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.num_records,
                "id",
                batch_size=benchmark_settings.batch_size,
            ):
                source.store_chunk(data_chunk)
        step1_time = get_step1_time()

        # Step 2: Initial processing (full run)
        with benchmark_timer() as get_step2_time:
            run_steps(app.ds, app.steps, run_config=run_config)
        step2_time = get_step2_time()

        # Verify initial processing
        initial_count = len(target.get_data())
        assert initial_count == benchmark_settings.num_records, (
            f"Expected {benchmark_settings.num_records}, got {initial_count}"
        )

        # Step 3: Add new records
        with benchmark_timer() as get_step3_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.new_records,
                "id",
                start_idx=benchmark_settings.num_records,
                batch_size=benchmark_settings.batch_size,
            ):
                source.store_chunk(data_chunk)
        step3_time = get_step3_time()

        # Step 4: Incremental processing
        with benchmark_timer() as get_step4_time:
            run_steps(app.ds, app.steps, run_config=run_config)
        step4_time = get_step4_time()

        # Verify final processing
        final_count = len(target.get_data())
        expected_final = benchmark_settings.num_records + benchmark_settings.new_records
        assert final_count == expected_final, (
            f"Expected {expected_final}, got {final_count}"
        )

        # Calculate metrics
        total_time = step1_time + step2_time + step3_time + step4_time
        incremental_throughput = (
            benchmark_settings.new_records / step4_time if step4_time > 0 else 0
        )

        measurements = {
            "step1_load_initial_time": step1_time,
            "step2_transform_initial_time": step2_time,
            "step3_load_incremental_time": step3_time,
            "step4_transform_incremental_time": step4_time,
            "total_time": total_time,
            "incremental_throughput_rps": incremental_throughput,
            "initial_records_processed": initial_count,
            "final_records_processed": final_count,
        }

        metadata = {
            "benchmark_description": "Measures incremental transform performance",
            "steps": [
                "Load initial dataset",
                "Initial processing (full run)",
                "Add new records",
                "Incremental processing",
            ],
        }

        output_benchmark_result(
            "incremental_transform_performance",
            benchmark_settings.model_dump(),  # type: ignore
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
