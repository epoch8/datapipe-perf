import os
import signal
import time
from contextlib import contextmanager

import pandas as pd
import pytest
import sqlalchemy as sa
from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from pytest_cases import parametrize
from sqlalchemy import Column, Integer, String


class TimeoutError(Exception):
    """Raised when operation exceeds timeout"""

    pass


@pytest.fixture
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
def timeout(seconds: int):
    """
    Context manager для ограничения времени выполнения.

    Если операция не завершилась за указанное время, поднимается TimeoutError.
    """

    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")

    # Установить обработчик сигнала
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        # Отменить таймер и восстановить старый обработчик
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def prepare_large_dataset(num_records: int, primary_key: str, start_idx=0):
    batch_size = 50_000

    for i in range(0, num_records, batch_size):
        chunk_size = min(batch_size, num_records - i)

        # Генерация данных
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


@parametrize("num_records", [10_000, 100_000])
def test_performance_small_dataset(num_records, dbconn: DBConn):
    """
    Simplest case: single input table, one output table, no cross joins
    """

    catalog = Catalog(
        {
            "perf_small_source": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_small_source",
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_small_target": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_small_target",
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

    def copy_transform(df):
        result = df.copy()
        result["value"] = result["value"] * 2
        return result

    pipeline = Pipeline(
        [
            BatchTransform(
                copy_transform,
                inputs=["perf_small_source"],
                outputs=["perf_small_target"],
                transform_keys=["id"],
                # reasonable value, since 10 is too small, but 100 gives speedup from 54 to 11 seconds
                # 1000 gives time 7 seconds, doesn't worth it
                chunk_size=100,
            )
        ]
    )
    app = DatapipeApp(ds, catalog, pipeline)

    print(
        f"\n=== Testing loading data using store_chunk, table size = {num_records} ==="
    )
    start = time.time()
    source = catalog.get_datatable(ds, "perf_small_source")
    target = catalog.get_datatable(ds, "perf_small_target")
    for data_chunk in prepare_large_dataset(num_records, "id"):
        source.store_chunk(data_chunk)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing run pipeline ===")
    start = time.time()
    run_steps(ds, app.steps)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    # # Проверка корректности
    result = target.get_data().sort_values("id").reset_index(drop=True)

    assert len(result) == num_records

    print("\n=== Results ===")
    print(f"Records processed: {num_records:,}")
    print(f"v1 (pipeline run time): {v1_time:.3f}s")


@parametrize(
    "source_1_n_records, source_2_n_records",
    [
        # symmetric tables
        (100, 100),
        (1000, 1000),
        # (10_000, 10_000),
        # (100_000, 100_000),
        # (1_000_000, 1_000_000),
        # asymmetric tables
        (100, 1000),
        (100, 10_000),
        # (100, 100_000),
        # (100, 1_000_000),
        # (1000, 10_000),
        # (1000, 100_000),
        # (1000, 1_000_000),
        # (10_000, 100_000),
        # (10_000, 1_000_000),
    ],
)
def test_performance_datasets_with_inner_join(
    source_1_n_records, source_2_n_records, dbconn: DBConn
):
    catalog = Catalog(
        {
            "perf_source_1": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_1",
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_source_2": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_2",
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_target": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_target",
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

    def add_concat_transform(df1, df2):
        result = df1.copy()
        result["value"] = result["value"] + df2["value"]
        result["category"] = result["category"] + " " + result["category"]
        return result

    pipeline = Pipeline(
        [
            BatchTransform(
                add_concat_transform,
                inputs=["perf_source_1", "perf_source_2"],
                outputs=["perf_target"],
                transform_keys=["id"],
                chunk_size=100,
            )
        ]
    )
    app = DatapipeApp(ds, catalog, pipeline)

    print(
        f"\n=== Testing loading data using store_chunk, table sizes = {source_1_n_records}, {source_2_n_records} ==="
    )
    start = time.time()
    source_1 = catalog.get_datatable(ds, "perf_source_1")
    source_2 = catalog.get_datatable(ds, "perf_source_2")
    target = catalog.get_datatable(ds, "perf_target")
    for data_chunk in prepare_large_dataset(source_1_n_records, "id"):
        source_1.store_chunk(data_chunk)

    for data_chunk in prepare_large_dataset(source_2_n_records, "id"):
        source_2.store_chunk(data_chunk)

    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing run pipeline ===")
    start = time.time()
    run_steps(ds, app.steps)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    # # Проверка корректности
    result = target.get_data().sort_values("id").reset_index(drop=True)

    assert len(result) == source_1_n_records

    print("\n=== Results ===")
    print(f"Records processed: {source_1_n_records:,}, {source_2_n_records:,}")
    print(f"(pipeline run time): {v1_time:.3f}s")


@parametrize(
    "source_1_n_records, source_2_n_records",
    [
        # symmetric tables
        (100, 100),
        # (1000, 1000),
        # asymmetric tables
        (100, 10_000),
    ],
)
def test_performance_datasets_with_cross_join(
    source_1_n_records, source_2_n_records, dbconn: DBConn
):
    catalog = Catalog(
        {
            "perf_source_1": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_1",
                    data_sql_schema=[
                        Column("id_1", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_source_2": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_2",
                    data_sql_schema=[
                        Column("id_2", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_target": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_target",
                    data_sql_schema=[
                        Column("id_1", String, primary_key=True),
                        Column("id_2", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
        }
    )
    ds = DataStore(dbconn, create_meta_table=True)

    def add_concat_transform(df1, df2):
        result = pd.merge(df1, df2, how="cross")
        result["value"] = result["value_x"] + result["value_y"]
        result["category"] = result["category_x"] + " " + result["category_y"]
        return result[["id_1", "id_2", "value", "category"]]

    pipeline = Pipeline(
        [
            BatchTransform(
                add_concat_transform,
                inputs=["perf_source_1", "perf_source_2"],
                outputs=["perf_target"],
                transform_keys=["id_1", "id_2"],
                chunk_size=100,
            )
        ]
    )
    app = DatapipeApp(ds, catalog, pipeline)

    print(
        f"\n=== Testing loading data using store_chunk, table sizes = {source_1_n_records}, {source_2_n_records} ==="
    )
    start = time.time()
    source_1 = catalog.get_datatable(ds, "perf_source_1")
    source_2 = catalog.get_datatable(ds, "perf_source_2")
    target = catalog.get_datatable(ds, "perf_target")
    for data_chunk in prepare_large_dataset(source_1_n_records, "id_1"):
        source_1.store_chunk(data_chunk)

    for data_chunk in prepare_large_dataset(source_2_n_records, "id_2"):
        source_2.store_chunk(data_chunk)

    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing run pipeline ===")
    start = time.time()
    run_steps(ds, app.steps)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    # # Проверка корректности
    result = target.get_data().reset_index(drop=True)

    assert len(result) == source_1_n_records * source_2_n_records

    print("\n=== Results ===")
    print(f"Records processed: {source_1_n_records:,}, {source_2_n_records:,}")
    print(f"(pipeline run time): {v1_time:.3f}s")


@parametrize(
    "source_1_n_records, source_2_n_records",
    [
        # symmetric tables
        (10_000, 10_000),
        # (100_000, 100_000),
        # asymmetric tables
        (100, 1000),
        (100, 10_000),
        # (100, 100_000),
    ],
)
def test_incremental_updates_with_inner_joins(
    source_1_n_records, source_2_n_records, dbconn: DBConn
):
    catalog = Catalog(
        {
            "perf_source_1": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_1",
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_source_2": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_source_2",
                    data_sql_schema=[
                        Column("id", String, primary_key=True),
                        Column("value", Integer),
                        Column("category", String),
                    ],
                    create_table=True,
                )
            ),
            "perf_target": Table(
                store=TableStoreDB(
                    dbconn,
                    "perf_target",
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

    def add_concat_transform(df1, df2):
        result = pd.merge(df1, df2, how="cross")
        result["value"] = result["value_x"] + result["value_y"]
        result["category"] = result["category_x"] + " " + result["category_y"]
        return result[["id_1", "id_2", "value", "category"]]

    pipeline = Pipeline(
        [
            BatchTransform(
                add_concat_transform,
                inputs=["perf_source_1", "perf_source_2"],
                outputs=["perf_target"],
                transform_keys=["id"],
                chunk_size=100,
            )
        ]
    )
    app = DatapipeApp(ds, catalog, pipeline)

    print(
        f"\n=== Testing loading data using store_chunk, table sizes = {source_1_n_records}, {source_2_n_records} ==="
    )
    start = time.time()
    source_1 = catalog.get_datatable(ds, "perf_source_1")
    source_2 = catalog.get_datatable(ds, "perf_source_2")
    target = catalog.get_datatable(ds, "perf_target")
    for data_chunk in prepare_large_dataset(source_1_n_records, "id"):
        source_1.store_chunk(data_chunk)

    for data_chunk in prepare_large_dataset(source_2_n_records, "id"):
        source_2.store_chunk(data_chunk)

    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing run pipeline ===")
    start = time.time()
    run_steps(ds, app.steps)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing loading data using store_chunk ===")
    start = time.time()
    for data_chunk in prepare_large_dataset(
        500, "id", start_idx=source_1_n_records + 1
    ):
        source_1.store_chunk(data_chunk)

    for data_chunk in prepare_large_dataset(
        1000, "id", start_idx=source_2_n_records + 1
    ):
        source_2.store_chunk(data_chunk)

    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Testing run pipeline ===")
    start = time.time()
    run_steps(ds, app.steps)
    v1_time = time.time() - start
    print(f"v1 time: {v1_time:.3f}s")

    print("\n=== Results ===")
    print(f"Records processed: {source_1_n_records:,}, {source_2_n_records:,}")
    print(f"(pipeline run time): {v1_time:.3f}s")
