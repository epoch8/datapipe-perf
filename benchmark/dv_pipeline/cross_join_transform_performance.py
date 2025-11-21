#!/usr/bin/env python3
"""
Performance with inner join benchmark

This benchmark measures the performance of data processing pipeline
with inner joins included by measuring time it takes to load and process data

Environment Variables:
    NUM_RECORDS: Number of initial records to process (required)
    NEW_RECORDS: Number of new records to add for incremental processing
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
from benchmark.dv_pipeline.pipeline_with_cross_join import get_app


class BenchmarkSettings(BaseSettings):
    num_records_first_table: int = 1000
    num_records_second_table: int = 1000
    new_records: int = 1000
    chunk_size: int = 100
    batch_size: int = 50_000


benchmark_settings = BenchmarkSettings() # type: ignore


def main(benchmark_settings: BenchmarkSettings = benchmark_settings) -> None:
    """Run the benchmark"""
    app = get_app()

    try:
        source_1 = app.ds.get_table("cross_source_1")
        source_2 = app.ds.get_table("cross_source_2")
        target = app.ds.get_table("cross_target")

        # Step 1: Load initial data
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.num_records_first_table,
                "id_1",
                batch_size=benchmark_settings.batch_size,
            ):
                source_1.store_chunk(data_chunk)

            for data_chunk in prepare_large_dataset(
                benchmark_settings.num_records_second_table,
                "id_2",
                batch_size=benchmark_settings.batch_size
            ):
                source_2.store_chunk(data_chunk)

        step1_time = get_step1_time()

        # Step 2: Initial processing (full run)
        with benchmark_timer() as get_step2_time:
            run_steps(app.ds, app.steps)
        step2_time = get_step2_time()

        initial_count = len(target.get_data())

        # Step 3: Add new records
        with benchmark_timer() as get_step3_time:
            for data_chunk in prepare_large_dataset(
                benchmark_settings.num_records_first_table,
                "id_1",
                batch_size=benchmark_settings.batch_size,
                start_idx=benchmark_settings.num_records_first_table
            ):
                source_1.store_chunk(data_chunk)
            for data_chunk in prepare_large_dataset(
                benchmark_settings.num_records_second_table,
                "id_2",
                batch_size=benchmark_settings.batch_size,
                start_idx=benchmark_settings.num_records_second_table
            ):
                source_2.store_chunk(data_chunk)

        step3_time = get_step3_time()

        # Step 4: Incremental processing
        with benchmark_timer() as get_step4_time:
            run_steps(app.ds, app.steps)
        step4_time = get_step4_time()

        final_count = len(target.get_data())

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
