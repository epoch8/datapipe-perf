"""
Common utilities for benchmarks.
"""

import json
import os
import platform
import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator

import pandas as pd
import psutil
import sqlalchemy as sa
from datapipe.store.database import DBConn


def get_git_branch() -> str:
    """Get current git branch name."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def get_git_commit() -> str:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()[:8]  # Short hash
    except Exception:
        return "unknown"


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

    Automatically includes system information (Python version, CPU, RAM) and git info.

    Args:
        benchmark_name: Name of the benchmark
        parameters: Input parameters used for the benchmark
        measurements: Timing measurements and metrics
        metadata: Additional metadata (automatically includes git and system info)
    """

    result_metadata = {
        "git_branch": get_git_branch(),
        "git_commit": get_git_commit(),
        "system_info": get_system_info(),
        **(metadata or {}),
    }

    result = {
        "benchmark": benchmark_name,
        "timestamp": time.time(),
        "parameters": parameters,
        "measurements": measurements,
        "metadata": result_metadata,
    }

    print(json.dumps(result, indent=2))


def get_env_int(name: str, default: int | None = None) -> int:
    """Get integer environment variable with optional default."""
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise ValueError(f"Environment variable {name} is required")
        return default
    return int(value)


def get_env_str(name: str, default: str | None = None) -> str:
    """Get string environment variable with optional default."""
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise ValueError(f"Environment variable {name} is required")
        return default
    return value


def get_env_bool(name: str, default: bool = False) -> bool:
    """Get boolean environment variable with default."""
    value = os.getenv(name, "").lower()
    if value in ("true", "1", "yes", "on"):
        return True
    elif value in ("false", "0", "no", "off", ""):
        return default
    else:
        raise ValueError(f"Invalid boolean value for {name}: {value}")


def dbconn():
    if os.environ.get("TEST_DB_ENV") == "sqlite":
        DBCONNSTR = "sqlite+pysqlite3:///:memory:"
        DB_TEST_SCHEMA = None
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
        DB_TEST_SCHEMA = "test"

    if DB_TEST_SCHEMA:
        eng = sa.create_engine(DBCONNSTR)

        try:
            with eng.begin() as conn:
                conn.execute(sa.text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))
        except Exception:
            pass

        with eng.begin() as conn:
            conn.execute(sa.text(f"CREATE SCHEMA {DB_TEST_SCHEMA}"))

        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

        with eng.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))

    else:
        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)


@contextmanager
def benchmark_timer():
    """Context manager for timing benchmark steps."""
    start = time.time()
    yield lambda: time.time() - start
