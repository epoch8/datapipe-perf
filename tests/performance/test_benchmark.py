"""
Бенчмарк тест для измерения производительности инкрементальных обновлений.

Измеряет время выполнения трансформаций на различных объемах данных.
Предназначен для запуска на разных версиях кода (ветках) для сравнения производительности.

Результаты сохраняются в: ~/.datapipe_benchmarks/

Workflow для сравнения веток:
    1. Запустить тесты на ветке master:
       git checkout master
       pytest tests/performance/test_benchmark.py -v -s

    2. Запустить тесты на ветке с оптимизацией:
       git checkout offset-optimization
       pytest tests/performance/test_benchmark.py -v -s

    3. Запустить тесты на ветке с дополнительной оптимизацией:
       git checkout offset-with-status
       pytest tests/performance/test_benchmark.py -v -s

    4. Сравнить результаты:
       pytest tests/performance/test_benchmark.py::test_compare_branches -v -s

Доступные тесты:
    - test_incremental_transform_performance: тесты на 10K, 100K, 1M записей
    - test_incremental_update_throughput: тесты с разными соотношениями initial/new
    - test_compare_branches: сравнение результатов между ветками
"""
import csv
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from pytest_cases import parametrize
from sqlalchemy import Column, Integer, String

from datapipe.compute import Catalog, Table, Pipeline, DatapipeApp, run_steps
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB


# Директория для сохранения результатов (вне проекта)
RESULTS_DIR = Path.home() / ".datapipe_benchmarks"
RESULTS_DIR.mkdir(exist_ok=True)


def get_git_branch():
    """Получить имя текущей git ветки."""
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


def save_benchmark_result(
    test_name: str,
    dataset_params: str,
    step_1_time: float,
    step_2_time: float,
    step_3_time: float,
    step_4_time: float,
    initial_records: int,
    new_records: int,
):
    """Сохранить результаты бенчмарка в CSV файл."""
    branch = get_git_branch()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    csv_file = RESULTS_DIR / f"{test_name}_results.csv"

    # Проверяем, существует ли файл
    file_exists = csv_file.exists()

    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)

        # Записываем заголовок, если файл новый
        if not file_exists:
            writer.writerow([
                "timestamp",
                "branch",
                "dataset_params",
                "initial_records",
                "new_records",
                "step_1_load_initial_sec",
                "step_2_transform_initial_sec",
                "step_3_load_incremental_sec",
                "step_4_transform_incremental_sec",
                "total_time_sec",
                "incremental_throughput_rec_per_sec",
            ])

        # Записываем данные
        total_time = step_1_time + step_2_time + step_3_time + step_4_time
        throughput = new_records / step_4_time if step_4_time > 0 else 0

        writer.writerow([
            timestamp,
            branch,
            dataset_params,
            initial_records,
            new_records,
            f"{step_1_time:.3f}",
            f"{step_2_time:.3f}",
            f"{step_3_time:.3f}",
            f"{step_4_time:.3f}",
            f"{total_time:.3f}",
            f"{throughput:.0f}",
        ])

    print(f"\n✓ Results saved to: {csv_file}")


def compare_branches(test_name: str, branches: List[str] = None):
    """
    Сравнить результаты тестов между ветками.

    Args:
        test_name: Имя теста для сравнения
        branches: Список веток для сравнения (None = все ветки)
    """
    csv_file = RESULTS_DIR / f"{test_name}_results.csv"

    if not csv_file.exists():
        print(f"No results found for test: {test_name}")
        return

    # Читаем результаты
    df = pd.read_csv(csv_file)

    # Фильтруем по веткам, если указано
    if branches:
        df = df[df["branch"].isin(branches)]

    if df.empty:
        print(f"No results found for specified branches: {branches}")
        return

    # Группируем по веткам и параметрам датасета
    print("\n" + "=" * 100)
    print(f"BENCHMARK COMPARISON: {test_name}")
    print("=" * 100)

    for dataset_params in df["dataset_params"].unique():
        df_dataset = df[df["dataset_params"] == dataset_params]

        print(f"\n📊 Dataset: {dataset_params}")
        print("-" * 100)

        # Берем последний запуск для каждой ветки
        df_latest = df_dataset.sort_values("timestamp").groupby("branch").tail(1)

        # Выводим таблицу сравнения
        comparison_data = []
        for _, row in df_latest.iterrows():
            comparison_data.append({
                "Branch": row["branch"],
                "Step 1 (load)": f"{row['step_1_load_initial_sec']:.3f}s",
                "Step 2 (transform)": f"{row['step_2_transform_initial_sec']:.3f}s",
                "Step 3 (load inc)": f"{row['step_3_load_incremental_sec']:.3f}s",
                "Step 4 (transform inc)": f"{row['step_4_transform_incremental_sec']:.3f}s",
                "Total": f"{row['total_time_sec']:.3f}s",
                "Throughput": f"{row['incremental_throughput_rec_per_sec']:.0f} rec/s",
            })

        df_comparison = pd.DataFrame(comparison_data)
        print(df_comparison.to_string(index=False))

        # Вычисляем разницу между ветками
        if len(df_latest) >= 2:
            print("\n📈 Performance difference:")

            branches_list = df_latest["branch"].tolist()
            base_branch = branches_list[0]

            for i in range(1, len(branches_list)):
                compare_branch = branches_list[i]

                base_row = df_latest[df_latest["branch"] == base_branch].iloc[0]
                compare_row = df_latest[df_latest["branch"] == compare_branch].iloc[0]

                print(f"\n  {compare_branch} vs {base_branch}:")

                # Сравниваем каждый шаг
                for col, label in [
                    ("step_1_load_initial_sec", "Step 1 (load initial)"),
                    ("step_2_transform_initial_sec", "Step 2 (transform initial)"),
                    ("step_3_load_incremental_sec", "Step 3 (load incremental)"),
                    ("step_4_transform_incremental_sec", "Step 4 (transform incremental)"),
                    ("total_time_sec", "Total time"),
                ]:
                    base_val = float(base_row[col])
                    compare_val = float(compare_row[col])

                    if base_val > 0:
                        diff_pct = ((compare_val - base_val) / base_val) * 100
                        speedup = base_val / compare_val if compare_val > 0 else 0

                        if diff_pct < 0:
                            print(f"    {label:30s}: {compare_val:.3f}s vs {base_val:.3f}s → ✓ {abs(diff_pct):.1f}% faster ({speedup:.2f}x)")
                        else:
                            print(f"    {label:30s}: {compare_val:.3f}s vs {base_val:.3f}s → ✗ {diff_pct:.1f}% slower ({speedup:.2f}x)")

                # Сравниваем throughput
                base_throughput = float(base_row["incremental_throughput_rec_per_sec"])
                compare_throughput = float(compare_row["incremental_throughput_rec_per_sec"])

                if base_throughput > 0:
                    diff_pct = ((compare_throughput - base_throughput) / base_throughput) * 100
                    print(f"    {'Incremental throughput':30s}: {compare_throughput:.0f} vs {base_throughput:.0f} rec/s → {'+' if diff_pct > 0 else ''}{diff_pct:.1f}%")

    print("\n" + "=" * 100)


def prepare_large_dataset(num_records: int, primary_key: str, start_idx=0):
    """
    Генератор для создания больших датасетов партиями.
    """
    batch_size = 50_000

    for i in range(0, num_records, batch_size):
        chunk_size = min(batch_size, num_records - i)

        data = pd.DataFrame({
            primary_key: [f"id_{(j + start_idx):010d}" for j in range(i, i + chunk_size)],
            "value": range(i, i + chunk_size),
            "category": [f"cat_{(j + start_idx) % 100}" for j in range(i, i + chunk_size)],
        })

        yield data


@parametrize('num_records', [10_000, 100_000, 1_000_000])
def test_incremental_transform_performance(num_records, dbconn: DBConn):
    """
    Бенчмарк: измеряет время инкрементальной обработки данных.

    Шаги:
    1. Загрузка начальных данных
    2. Первичная обработка (full run)
    3. Добавление новых записей
    4. Инкрементальная обработка (измеряется время)
    """
    catalog = Catalog({
        'perf_source': Table(
            store=TableStoreDB(
                dbconn,
                "perf_source",
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("value", Integer),
                    Column("category", String),
                ],
                create_table=True,
            )
        ),
        'perf_target': Table(
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
        )
    })
    ds = DataStore(dbconn, create_meta_table=True)

    def copy_transform(df):
        result = df.copy()
        result['value'] = result['value'] * 2
        return result

    pipeline = Pipeline([
        BatchTransform(
            copy_transform,
            inputs=['perf_source'],
            outputs=['perf_target'],
            transform_keys=['id'],
            chunk_size=100
        )
    ])
    app = DatapipeApp(ds, catalog, pipeline)

    # ========== Шаг 1: Загрузка начальных данных ==========
    print(f"\n=== Step 1: Loading {num_records:,} initial records ===")
    source = catalog.get_datatable(ds, 'perf_source')
    target = catalog.get_datatable(ds, 'perf_target')

    start = time.time()
    for data_chunk in prepare_large_dataset(num_records, 'id'):
        source.store_chunk(data_chunk)
    load_initial_time = time.time() - start
    print(f"Step 1 time (store_chunk): {load_initial_time:.3f}s")

    # ========== Шаг 2: Первичная обработка ==========
    print("\n=== Step 2: Initial processing (full run) ===")

    # Создаем RunConfig с включенной оптимизацией
    run_config = RunConfig(labels={"use_offset_optimization": True})

    start = time.time()
    run_steps(ds, app.steps, run_config=run_config)
    initial_transform_time = time.time() - start
    print(f"Step 2 time (transform): {initial_transform_time:.3f}s")

    initial_count = len(target.get_data())
    print(f"Records processed: {initial_count:,}")
    assert initial_count == num_records

    # ========== Шаг 3: Добавление новых записей ==========
    new_records = 1_000
    print(f"\n=== Step 3: Adding {new_records:,} new records ===")

    start = time.time()
    for data_chunk in prepare_large_dataset(new_records, 'id', start_idx=num_records):
        source.store_chunk(data_chunk)
    load_incremental_time = time.time() - start
    print(f"Step 3 time (store_chunk): {load_incremental_time:.3f}s")

    # ========== Шаг 4: Инкрементальная обработка ==========
    print("\n=== Step 4: Incremental processing ===")

    start = time.time()
    run_steps(ds, app.steps, run_config=run_config)
    incremental_transform_time = time.time() - start
    print(f"Step 4 time (transform): {incremental_transform_time:.3f}s")

    final_count = len(target.get_data())
    print(f"Final records: {final_count:,}")
    assert final_count == num_records + new_records

    # ========== Результаты ==========
    print("\n=== Results ===")
    print(f"Initial dataset: {num_records:,} records")
    print(f"New records: {new_records:,}")
    print(f"")
    print(f"Step 1 (load initial): {load_initial_time:.3f}s")
    print(f"Step 2 (transform initial): {initial_transform_time:.3f}s")
    print(f"Step 3 (load incremental): {load_incremental_time:.3f}s")
    print(f"Step 4 (transform incremental): {incremental_transform_time:.3f}s")
    print(f"")
    print(f"Total time: {load_initial_time + initial_transform_time + load_incremental_time + incremental_transform_time:.3f}s")
    print(f"Incremental throughput: {new_records/incremental_transform_time:,.0f} rec/sec")

    # Сохраняем результаты в CSV
    save_benchmark_result(
        test_name="test_incremental_transform_performance",
        dataset_params=f"{num_records}",
        step_1_time=load_initial_time,
        step_2_time=initial_transform_time,
        step_3_time=load_incremental_time,
        step_4_time=incremental_transform_time,
        initial_records=num_records,
        new_records=new_records,
    )


@parametrize('initial_records, new_records', [
    (10_000, 1000),
    (100_000, 1000),
    (1_000_000, 1000),
])
def test_incremental_update_throughput(initial_records, new_records, dbconn: DBConn):
    """
    Бенчмарк: измеряет throughput инкрементальных обновлений.

    Шаги:
    1. Загрузка начальных данных
    2. Первичная обработка (full run)
    3. Добавление новых записей
    4. Инкрементальная обработка (измеряется время)
    """
    catalog = Catalog({
        'source': Table(
            store=TableStoreDB(
                dbconn,
                "source",
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("value", Integer),
                    Column("category", String),
                ],
                create_table=True,
            )
        ),
        'target': Table(
            store=TableStoreDB(
                dbconn,
                "target",
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("value", Integer),
                    Column("category", String),
                ],
                create_table=True,
            )
        )
    })
    ds = DataStore(dbconn, create_meta_table=True)

    def transform_func(df):
        result = df.copy()
        result['value'] = result['value'] * 2
        return result

    pipeline = Pipeline([
        BatchTransform(
            transform_func,
            inputs=['source'],
            outputs=['target'],
            transform_keys=['id'],
            chunk_size=100
        )
    ])
    app = DatapipeApp(ds, catalog, pipeline)

    source = catalog.get_datatable(ds, 'source')
    target = catalog.get_datatable(ds, 'target')

    # ========== Шаг 1: Загрузка начальных данных ==========
    print(f"\n=== Step 1: Loading {initial_records:,} initial records ===")
    start = time.time()
    for data_chunk in prepare_large_dataset(initial_records, 'id'):
        source.store_chunk(data_chunk)
    load_initial_time = time.time() - start
    print(f"Step 1 time (store_chunk): {load_initial_time:.3f}s")

    # ========== Шаг 2: Первичная обработка ==========
    print("\n=== Step 2: Initial processing (full run) ===")
    run_config = RunConfig(labels={"use_offset_optimization": True})

    start = time.time()
    run_steps(ds, app.steps, run_config=run_config)
    initial_transform_time = time.time() - start
    print(f"Step 2 time (transform): {initial_transform_time:.3f}s")

    initial_count = len(target.get_data())
    assert initial_count == initial_records
    print(f"Initial records processed: {initial_count:,}")

    # ========== Шаг 3: Добавление новых записей ==========
    print(f"\n=== Step 3: Adding {new_records:,} new records ===")
    start = time.time()
    for data_chunk in prepare_large_dataset(new_records, 'id', start_idx=initial_records):
        source.store_chunk(data_chunk)
    load_incremental_time = time.time() - start
    print(f"Step 3 time (store_chunk): {load_incremental_time:.3f}s")

    # ========== Шаг 4: Инкрементальная обработка ==========
    print("\n=== Step 4: Incremental processing ===")

    start = time.time()
    run_steps(ds, app.steps, run_config=run_config)
    incremental_transform_time = time.time() - start
    print(f"Step 4 time (transform): {incremental_transform_time:.3f}s")

    final_count = len(target.get_data())
    assert final_count == initial_records + new_records
    print(f"Final records: {final_count:,}")

    # ========== Результаты ==========
    print("\n=== Results ===")
    print(f"Dataset: {initial_records:,} initial + {new_records:,} new = {final_count:,} total")
    print(f"")
    print(f"Step 1 (load initial): {load_initial_time:.3f}s")
    print(f"Step 2 (transform initial): {initial_transform_time:.3f}s")
    print(f"Step 3 (load incremental): {load_incremental_time:.3f}s")
    print(f"Step 4 (transform incremental): {incremental_transform_time:.3f}s")
    print(f"")
    print(f"Total time: {load_initial_time + initial_transform_time + load_incremental_time + incremental_transform_time:.3f}s")
    print(f"Incremental throughput: {new_records/incremental_transform_time:,.0f} rec/sec")

    # Сохраняем результаты в CSV
    save_benchmark_result(
        test_name="test_incremental_update_throughput",
        dataset_params=f"{initial_records}+{new_records}",
        step_1_time=load_initial_time,
        step_2_time=initial_transform_time,
        step_3_time=load_incremental_time,
        step_4_time=incremental_transform_time,
        initial_records=initial_records,
        new_records=new_records,
    )


def test_compare_branches():
    """
    Тест для сравнения результатов бенчмарков между ветками.

    Использование:
        pytest tests/performance/test_benchmark.py::test_compare_branches -v -s
    """
    print("\n" + "=" * 100)
    print("COMPARING BENCHMARK RESULTS")
    print("=" * 100)

    # Сравниваем результаты для обоих тестов
    compare_branches("test_incremental_transform_performance")
    compare_branches("test_incremental_update_throughput")
