#!/usr/bin/env python3
"""
Multi-table Write Performance Benchmark

This benchmark tests the performance issue in PR #355 where store_chunk()
reads entire related tables during write operations.

Scenario:
- Small profiles table (1K-100K records)
- Large posts table (100K-1M records)
- Measure how long it takes to write new posts when profiles table grows

Expected behavior:
- PR #355: Write time grows with profiles table size (reads all profiles on each write)
- Offsets: Write time constant regardless of profiles table size

Environment Variables:
    NUM_PROFILES: Number of profile records (required)
    NUM_POSTS: Number of initial post records (required)
    NEW_POSTS: Number of new posts to add (default: 1000)
    CHUNK_SIZE: Batch processing chunk size (default: 100)
    BATCH_SIZE: Data generation batch size (default: 50000)

Output: JSON with benchmark results
"""

import sys

import pandas as pd
from datapipe.compute import run_steps
from datapipe.run_config import RunConfig
from pydantic_settings import BaseSettings

from benchmark.common import (
    benchmark_timer,
    output_benchmark_result,
    prepare_large_dataset,
)
from benchmark.dv_pipeline.multi_table_pipeline import get_app, get_pipeline_info


class BenchmarkSettings(BaseSettings):
    num_profiles: int = 1000
    num_posts: int = 10000
    new_posts: int = 1000
    chunk_size: int = 100
    batch_size: int = 50000


benchmark_settings = BenchmarkSettings()  # type: ignore


def prepare_profiles_dataset(
    num_records: int, start_idx: int = 0, batch_size: int = 50_000
):
    """Generate profiles data."""
    for i in range(0, num_records, batch_size):
        chunk_size = min(batch_size, num_records - i)
        data = pd.DataFrame(
            {
                "id": [f"user_{(j + start_idx):010d}" for j in range(i, i + chunk_size)],
                "name": [f"User {(j + start_idx)}" for j in range(i, i + chunk_size)],
                "age": [(20 + ((j + start_idx) % 60)) for j in range(i, i + chunk_size)],
            }
        )
        yield data


def prepare_posts_dataset(
    num_records: int, num_profiles: int, start_idx: int = 0, batch_size: int = 50_000
):
    """Generate posts data linked to profiles."""
    for i in range(0, num_records, batch_size):
        chunk_size = min(batch_size, num_records - i)
        data = pd.DataFrame(
            {
                "id": [f"post_{(j + start_idx):010d}" for j in range(i, i + chunk_size)],
                "user_id": [
                    f"user_{((j + start_idx) % num_profiles):010d}"
                    for j in range(i, i + chunk_size)
                ],
                "content": [
                    f"Post content {(j + start_idx)}" for j in range(i, i + chunk_size)
                ],
            }
        )
        yield data


def main(benchmark_settings: BenchmarkSettings = benchmark_settings) -> None:
    """Run the multi-table write performance benchmark."""

    app = get_app()
    run_config = RunConfig(labels={"use_offset_optimization": True})

    # Get pipeline version info for metadata
    pipeline_info = get_pipeline_info()

    try:
        profiles = app.ds.get_table("perf_profiles")
        posts = app.ds.get_table("perf_posts")
        result = app.ds.get_table("perf_joined_result")

        # Step 1: Load profiles (small table)
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_profiles_dataset(
                benchmark_settings.num_profiles,
                batch_size=benchmark_settings.batch_size,
            ):
                profiles.store_chunk(data_chunk)
        step1_time = get_step1_time()

        # Step 2: Load initial posts (large table)
        with benchmark_timer() as get_step2_time:
            for data_chunk in prepare_posts_dataset(
                benchmark_settings.num_posts,
                benchmark_settings.num_profiles,
                batch_size=benchmark_settings.batch_size,
            ):
                posts.store_chunk(data_chunk)
        step2_time = get_step2_time()

        # Step 3: Initial processing (full run)
        with benchmark_timer() as get_step3_time:
            run_steps(app.ds, app.steps, run_config=run_config)
        step3_time = get_step3_time()

        # Verify initial processing
        initial_count = len(result.get_data())

        # Step 4: Add new posts (THIS IS WHERE PR #355 ISSUE SHOWS UP)
        # PR #355: store_chunk will read ALL profiles on each chunk write
        # Expected: Time should grow with num_profiles
        with benchmark_timer() as get_step4_time:
            for data_chunk in prepare_posts_dataset(
                benchmark_settings.new_posts,
                benchmark_settings.num_profiles,
                start_idx=benchmark_settings.num_posts,
                batch_size=benchmark_settings.batch_size,
            ):
                posts.store_chunk(data_chunk)
        step4_time = get_step4_time()

        # Step 5: Incremental processing
        with benchmark_timer() as get_step5_time:
            run_steps(app.ds, app.steps, run_config=run_config)
        step5_time = get_step5_time()

        # Verify final processing
        final_count = len(result.get_data())

        # Calculate metrics
        total_time = step1_time + step2_time + step3_time + step4_time + step5_time
        write_throughput = (
            benchmark_settings.new_posts / step4_time if step4_time > 0 else 0
        )

        measurements = {
            "step1_load_profiles_time": step1_time,
            "step2_load_posts_time": step2_time,
            "step3_transform_initial_time": step3_time,
            "step4_write_new_posts_time": step4_time,  # KEY METRIC
            "step5_transform_incremental_time": step5_time,
            "total_time": total_time,
            "write_throughput_rps": write_throughput,
            "initial_records_processed": initial_count,
            "final_records_processed": final_count,
        }

        metadata = {
            "benchmark_description": "Measures write performance with multiple input tables (PR #355 issue)",
            "steps": [
                "Load profiles (small table)",
                "Load posts (large table)",
                "Initial processing (full run)",
                "Write new posts (KEY: PR #355 reads all profiles here)",
                "Incremental processing",
            ],
            "expected_issue": "PR #355: step4_write_new_posts_time should grow with num_profiles",
            "pipeline_version": pipeline_info["version"],
            "pipeline_description": pipeline_info["description"],
            "join_keys_optimization": pipeline_info["optimization_enabled"],
        }

        output_benchmark_result(
            "multi_table_write_performance",
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
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
