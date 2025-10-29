"""
Common utilities for benchmarks.
"""

import json
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, Literal

import pandas as pd
import sqlalchemy as sa
from datapipe.store.database import DBConn
from pydantic_settings import BaseSettings


class DBSettings(BaseSettings):
    test_db_env: Literal["sqlite", "postgres"] = "sqlite"
    postgres_host: str | None = None
    postgres_port: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_db: str | None = None


db_settings = DBSettings()


def get_dbconn(db_settings: DBSettings = db_settings) -> DBConn:
    if db_settings.test_db_env == "sqlite":
        # remove existing SQLite file if it exists
        if os.path.exists("benchmark.db"):
            os.remove("benchmark.db")
        return DBConn("sqlite+pysqlite3:///benchmark.db")
    else:
        pg_host = db_settings.postgres_host or "localhost"
        pg_port = db_settings.postgres_port or "5432"
        pg_user = db_settings.postgres_user or "postgres"
        pg_password = db_settings.postgres_password or "password"
        pg_db = db_settings.postgres_db or "postgres"
        dbconn_str = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        db_test_schema = "benchmark"

        # Clean existing schema
        eng = sa.create_engine(dbconn_str)

        try:
            with eng.begin() as conn:
                conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {db_test_schema} CASCADE"))
        except Exception:
            pass

        with eng.begin() as conn:
            conn.execute(sa.text(f"CREATE SCHEMA {db_test_schema}"))

        return DBConn(dbconn_str, db_test_schema)


def prepare_large_dataset(
    num_records: int, primary_key: str, start_idx: int = 0, batch_size: int = 50_000
) -> Generator[pd.DataFrame, None, None]:
    """
    Generator for creating large datasets in batches for memory efficiency.

    Args:
        num_records: Total number of records to generate
        primary_key: Name of the primary key column
        start_idx: Starting index for record IDs
        batch_size: Size of each batch
    """
    for i in range(0, num_records, batch_size):
        chunk_size = min(batch_size, num_records - i)

        data = pd.DataFrame(
            {
                primary_key: [
                    f"id_{(j + start_idx):010d}" for j in range(i, i + chunk_size)
                ],
                "value": range(i, i + chunk_size),
                "category": [
                    f"cat_{(j + start_idx) % 100}" for j in range(i, i + chunk_size)
                ],
            }
        )

        yield data


def output_benchmark_result(
    benchmark_name: str,
    parameters: Dict[str, Any],
    measurements: Dict[str, float],
    metadata: Dict[str, Any] | None = None,
) -> None:
    """
    Output benchmark results as JSON to stdout.

    Includes git information and any additional metadata provided.

    Args:
        benchmark_name: Name of the benchmark
        parameters: Input parameters used for the benchmark
        measurements: Timing measurements and metrics
        metadata: Additional metadata (includes git info)
    """

    result_metadata = metadata

    result = {
        "benchmark": benchmark_name,
        "timestamp": time.time(),
        "parameters": parameters,
        "measurements": measurements,
        "metadata": result_metadata,
    }

    print(json.dumps(result, indent=2))


@contextmanager
def benchmark_timer():
    """Context manager for timing benchmark steps."""
    start = time.time()
    yield lambda: time.time() - start
