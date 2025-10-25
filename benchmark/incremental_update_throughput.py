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
    USE_OFFSET_OPTIMIZATION: Enable offset optimization (default: true)

Output: JSON with benchmark results
"""

import sys
import tempfile

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table, run_steps
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import Column, Integer, String

from benchmark.common import (
    benchmark_timer,
    get_env_bool,
    get_env_int,
    output_benchmark_result,
    prepare_large_dataset,
)


def create_catalog_and_datastore(
    dbconn: DBConn, source_table: str = "perf_source", target_table: str = "perf_target"
) -> tuple[Catalog, DataStore]:
    """Create catalog and datastore for benchmarks."""
    catalog = Catalog(
        {
            source_table: Table(
                store=TableStoreDB(
                    dbconn,
                    source_table,
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            target_table: Table(
                store=TableStoreDB(
                    dbconn,
                    target_table,
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
        }
    )

    ds = DataStore(dbconn, create_meta_table=True)
    return catalog, ds


def create_pipeline(
    source_table: str = "perf_source",
    target_table: str = "perf_target",
    chunk_size: int = 100,
) -> Pipeline:
    """Create a simple copy transformation pipeline."""

    def copy_transform(df):
        return df.copy()

    return Pipeline(
        [
            BatchTransform(
                copy_transform,
                inputs=[source_table],
                outputs=[target_table],
                transform_keys=["id"],
                chunk_size=chunk_size,
            )
        ]
    )


def create_temp_sqlite_db() -> DBConn:
    """Create a temporary SQLite database connection."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_file.close()
    connection_string = f"sqlite:///{temp_file.name}"
    return DBConn(connection_string)


def main():
    """Run the incremental update throughput benchmark."""
    # Get parameters from environment
    initial_records = get_env_int("INITIAL_RECORDS", default=100)
    new_records = get_env_int("NEW_RECORDS", default=100)
    chunk_size = get_env_int("CHUNK_SIZE", default=100)
    batch_size = get_env_int("BATCH_SIZE", default=50000)
    use_offset_optimization = get_env_bool("USE_OFFSET_OPTIMIZATION", default=True)

    parameters = {
        "initial_records": initial_records,
        "new_records": new_records,
        "chunk_size": chunk_size,
        "batch_size": batch_size,
        "use_offset_optimization": use_offset_optimization,
    }

    # Create database connection
    dbconn = create_temp_sqlite_db()

    try:
        # Setup catalog, datastore, and pipeline
        catalog, ds = create_catalog_and_datastore(dbconn, "source", "target")
        pipeline = create_pipeline("source", "target", chunk_size=chunk_size)
        app = DatapipeApp(ds, catalog, pipeline)

        source = catalog.get_datatable(ds, "source")
        target = catalog.get_datatable(ds, "target")

        # Step 1: Load initial data
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_large_dataset(
                initial_records, "id", batch_size=batch_size
            ):
                source.store_chunk(data_chunk)
        step1_time = get_step1_time()

        # Step 2: Initial processing (full run)
        run_config = RunConfig(
            labels={"use_offset_optimization": use_offset_optimization}
        )

        with benchmark_timer() as get_step2_time:
            run_steps(ds, app.steps, run_config=run_config)
        step2_time = get_step2_time()

        # Verify initial processing
        initial_count = len(target.get_data())
        assert initial_count == initial_records, (
            f"Expected {initial_records}, got {initial_count}"
        )

        # Step 3: Add new records
        with benchmark_timer() as get_step3_time:
            for data_chunk in prepare_large_dataset(
                new_records, "id", start_idx=initial_records, batch_size=batch_size
            ):
                source.store_chunk(data_chunk)
        step3_time = get_step3_time()

        # Step 4: Incremental processing
        with benchmark_timer() as get_step4_time:
            run_steps(ds, app.steps, run_config=run_config)
        step4_time = get_step4_time()

        # Verify final processing
        final_count = len(target.get_data())
        expected_final = initial_records + new_records
        assert final_count == expected_final, (
            f"Expected {expected_final}, got {final_count}"
        )

        # Calculate metrics
        total_time = step1_time + step2_time + step3_time + step4_time
        incremental_throughput = new_records / step4_time if step4_time > 0 else 0
        initial_throughput = initial_records / step2_time if step2_time > 0 else 0

        # Calculate efficiency ratios
        data_ratio = new_records / initial_records if initial_records > 0 else 0
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
            "incremental_update_throughput", parameters, measurements, metadata
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
