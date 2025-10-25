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
    USE_OFFSET_OPTIMIZATION: Enable offset optimization (default: true)

Output: JSON with benchmark results
"""

import sys
import tempfile

import pandas as pd
import sqlalchemy.orm as sa_orm
from datapipe.compute import Catalog, DatapipeApp, Pipeline, run_steps
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from pydantic_settings import BaseSettings
from sqlalchemy import Column, Integer, String

from benchmark.common import (
    benchmark_timer,
    output_benchmark_result,
    prepare_large_dataset,
)


class Settings(BaseSettings):
    num_records: int
    new_records: int = 1000
    chunk_size: int = 100
    batch_size: int = 50000
    use_offset_optimization: bool = True
    dbconn_uri: str


settings = Settings()  # type: ignore


class Base(sa_orm.DeclarativeBase):
    pass


class PerfSourceTable(Base):
    __tablename__ = "perf_source"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


class PerfTargetTable(Base):
    __tablename__ = "perf_target"

    id = Column(String, primary_key=True)
    value = Column(Integer)
    category = Column(String)


def copy_transform(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


pipeline = Pipeline(
    [
        BatchTransform(
            copy_transform,
            inputs=[PerfSourceTable],
            outputs=[PerfTargetTable],
            transform_keys=["id"],
            chunk_size=100,
        )
    ]
)


def create_temp_sqlite_db() -> DBConn:
    """Create a temporary SQLite database connection."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_file.close()
    connection_string = f"sqlite:///{temp_file.name}"
    return DBConn(connection_string)


def main(settings: Settings = settings) -> None:
    """Run the incremental transform performance benchmark."""

    parameters = {
        "num_records": settings.num_records,
        "new_records": settings.new_records,
        "chunk_size": settings.chunk_size,
        "batch_size": settings.batch_size,
        "use_offset_optimization": settings.use_offset_optimization,
    }

    # Create database connection
    dbconn = create_temp_sqlite_db()

    try:
        ds = DataStore(dbconn, create_meta_table=True)

        # Setup catalog, datastore, and pipeline
        app = DatapipeApp(ds, Catalog({}), pipeline)

        source = app.ds.get_table("perf_source")
        target = app.ds.get_table("perf_target")

        # Step 1: Load initial data
        with benchmark_timer() as get_step1_time:
            for data_chunk in prepare_large_dataset(
                settings.num_records, "id", batch_size=settings.batch_size
            ):
                source.store_chunk(data_chunk)
        step1_time = get_step1_time()

        # Step 2: Initial processing (full run)
        run_config = RunConfig(
            labels={"use_offset_optimization": settings.use_offset_optimization}
        )

        with benchmark_timer() as get_step2_time:
            run_steps(ds, app.steps, run_config=run_config)
        step2_time = get_step2_time()

        # Verify initial processing
        initial_count = len(target.get_data())
        assert initial_count == settings.num_records, (
            f"Expected {settings.num_records}, got {initial_count}"
        )

        # Step 3: Add new records
        with benchmark_timer() as get_step3_time:
            for data_chunk in prepare_large_dataset(
                settings.new_records,
                "id",
                start_idx=settings.num_records,
                batch_size=settings.batch_size,
            ):
                source.store_chunk(data_chunk)
        step3_time = get_step3_time()

        # Step 4: Incremental processing
        with benchmark_timer() as get_step4_time:
            run_steps(ds, app.steps, run_config=run_config)
        step4_time = get_step4_time()

        # Verify final processing
        final_count = len(target.get_data())
        expected_final = settings.num_records + settings.new_records
        assert final_count == expected_final, (
            f"Expected {expected_final}, got {final_count}"
        )

        # Calculate metrics
        total_time = step1_time + step2_time + step3_time + step4_time
        incremental_throughput = (
            settings.new_records / step4_time if step4_time > 0 else 0
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
            "incremental_transform_performance", parameters, measurements, metadata
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
