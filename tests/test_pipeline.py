import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from incremental_pipeline.models import RunResult, RunStatus
from incremental_pipeline.pipeline import (
    FailurePoint,
    InjectedFailure,
    run_incremental_load,
)
from incremental_pipeline.source import SourceDataError
from incremental_pipeline.state import get_pipeline_state

INITIAL_START_AT = datetime(2026, 1, 1, tzinfo=UTC)
OVERLAP = timedelta(hours=24)


def run_products(
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
    failure_point: FailurePoint | None = None,
) -> RunResult:
    return run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name="products",
        initial_start_at=INITIAL_START_AT,
        overlap=OVERLAP,
        failure_point=failure_point,
    )


def write_source_history(path: Path) -> Path:
    source_path = path / "products.csv"
    rows = [
        {
            "product_id": "OUTSIDE",
            "updated_at": "2025-12-30T10:59:59Z",
            "source_sequence": "1",
            "name": "Old product",
            "category": "Archive",
            "price": "1.00",
            "is_active": "false",
        },
        {
            "product_id": "P001",
            "updated_at": "2026-01-01T10:00:00Z",
            "source_sequence": "1",
            "name": "Cordless drill",
            "category": "Tools",
            "price": "89.99",
            "is_active": "true",
        },
        {
            "product_id": "P001",
            "updated_at": "2026-01-01T11:00:00Z",
            "source_sequence": "2",
            "name": "Cordless drill with case",
            "category": "Tools",
            "price": "99.99",
            "is_active": "true",
        },
        {
            "product_id": "P002",
            "updated_at": "2026-01-01T10:30:00Z",
            "source_sequence": "1",
            "name": "Circular saw",
            "category": "Tools",
            "price": "129.99",
            "is_active": "true",
        },
    ]
    with source_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return source_path


def append_source_changes(source_path: Path) -> None:
    rows = [
        {
            "product_id": "P001",
            "updated_at": "2026-01-01T12:00:00Z",
            "source_sequence": "3",
            "name": "Cordless drill kit",
            "category": "Tools",
            "price": "109.99",
            "is_active": "true",
        },
        {
            "product_id": "P003",
            "updated_at": "2026-01-01T11:30:00Z",
            "source_sequence": "1",
            "name": "Impact driver",
            "category": "Tools",
            "price": "79.99",
            "is_active": "true",
        },
    ]
    with source_path.open("a", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=list(rows[0]))
        writer.writerows(rows)


def test_initial_load_commits_products_watermark_and_run_metadata(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    source_path = write_source_history(tmp_path)
    result = run_products(connection, source_path)

    assert result.status is RunStatus.SUCCESS
    assert result.previous_watermark is None
    assert result.extraction_start == INITIAL_START_AT
    assert result.resulting_watermark == datetime(
        2026, 1, 1, 11, 0, tzinfo=UTC
    )
    assert (
        result.rows_received,
        result.rows_after_deduplication,
        result.rows_inserted,
        result.rows_updated,
        result.rows_ignored,
    ) == (3, 2, 2, 0, 0)

    products = connection.execute(
        """
        SELECT product_id, name, source_sequence, last_run_id
        FROM products_current
        ORDER BY product_id
        """
    ).fetchall()
    assert products == [
        ("P001", "Cordless drill with case", 2, result.run_id),
        ("P002", "Circular saw", 1, result.run_id),
    ]

    state = get_pipeline_state(connection, "products")
    assert state is not None
    assert state.watermark == result.resulting_watermark
    assert state.last_successful_run_id == result.run_id

    run = connection.execute(
        """
        SELECT
            pipeline_name,
            status,
            previous_watermark,
            resulting_watermark,
            rows_received,
            rows_after_deduplication,
            rows_inserted,
            rows_updated,
            rows_ignored,
            error_type,
            error_message
        FROM pipeline_runs
        WHERE run_id = ?
        """,
        [result.run_id],
    ).fetchone()
    assert run == (
        "products",
        "SUCCESS",
        None,
        result.resulting_watermark,
        3,
        2,
        2,
        0,
        0,
        None,
        None,
    )
    connection.close()


def test_rerunning_source_history_does_not_mutate_products(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    source_path = write_source_history(tmp_path)
    first_result = run_products(connection, source_path)
    products_before_replay = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()

    replay_result = run_products(connection, source_path)

    products_after_replay = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()
    assert products_after_replay == products_before_replay
    assert first_result.resulting_watermark is not None
    assert replay_result.previous_watermark == first_result.resulting_watermark
    assert replay_result.resulting_watermark == first_result.resulting_watermark
    assert replay_result.extraction_start == (
        first_result.resulting_watermark - OVERLAP
    )
    assert (
        replay_result.rows_received,
        replay_result.rows_after_deduplication,
        replay_result.rows_inserted,
        replay_result.rows_updated,
        replay_result.rows_ignored,
    ) == (3, 2, 0, 0, 2)

    state = get_pipeline_state(connection, "products")
    assert state is not None
    assert state.watermark == first_result.resulting_watermark
    assert state.last_successful_run_id == replay_result.run_id
    assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone() == (
        2,
    )
    connection.close()


@pytest.mark.parametrize("failure_point", list(FailurePoint))
def test_failed_load_rolls_back_target_state_and_success_metadata(
    tmp_path: Path,
    failure_point: FailurePoint,
) -> None:
    connection = duckdb.connect(":memory:")
    source_path = write_source_history(tmp_path)
    initial_result = run_products(connection, source_path)
    products_before_failure = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()
    state_before_failure = get_pipeline_state(connection, "products")
    assert state_before_failure is not None
    append_source_changes(source_path)

    with pytest.raises(InjectedFailure):
        run_products(connection, source_path, failure_point)

    products_after_failure = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()
    assert products_after_failure == products_before_failure
    assert get_pipeline_state(connection, "products") == state_before_failure
    runs = connection.execute(
        """
        SELECT status, resulting_watermark, error_type
        FROM pipeline_runs
        ORDER BY started_at, run_id
        """
    ).fetchall()
    assert sorted(row[0] for row in runs) == ["FAILED", "SUCCESS"]
    failed_run = next(row for row in runs if row[0] == "FAILED")
    assert failed_run == ("FAILED", None, "InjectedFailure")
    assert initial_result.resulting_watermark == state_before_failure.watermark
    connection.close()


def test_validation_failure_is_recorded_before_main_transaction(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    source_path = tmp_path / "invalid-products.csv"
    row = {
        "product_id": "",
        "updated_at": "2026-01-01T10:00:00Z",
        "source_sequence": "1",
        "name": "Cordless drill",
        "category": "Tools",
        "price": "89.99",
        "is_active": "true",
    }
    with source_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)

    with pytest.raises(SourceDataError):
        run_products(connection, source_path)

    assert connection.execute("SELECT COUNT(*) FROM products_current").fetchone() == (
        0,
    )
    assert get_pipeline_state(connection, "products") is None
    failed_run = connection.execute(
        """
        SELECT status, resulting_watermark, error_type
        FROM pipeline_runs
        """
    ).fetchone()
    assert failed_run == ("FAILED", None, "SourceDataError")
    connection.close()


def test_retry_after_failure_produces_expected_state(tmp_path: Path) -> None:
    connection = duckdb.connect(":memory:")
    source_path = write_source_history(tmp_path)
    initial_result = run_products(connection, source_path)
    append_source_changes(source_path)

    with pytest.raises(InjectedFailure):
        run_products(connection, source_path, FailurePoint.AFTER_TARGET_WRITES)

    retry_result = run_products(connection, source_path)

    assert (
        retry_result.rows_received,
        retry_result.rows_after_deduplication,
        retry_result.rows_inserted,
        retry_result.rows_updated,
        retry_result.rows_ignored,
    ) == (5, 3, 1, 1, 1)
    assert retry_result.resulting_watermark == datetime(
        2026, 1, 1, 12, 0, tzinfo=UTC
    )
    products = connection.execute(
        """
        SELECT product_id, name, last_run_id
        FROM products_current
        ORDER BY product_id
        """
    ).fetchall()
    assert products == [
        ("P001", "Cordless drill kit", retry_result.run_id),
        ("P002", "Circular saw", initial_result.run_id),
        ("P003", "Impact driver", retry_result.run_id),
    ]
    state = get_pipeline_state(connection, "products")
    assert state is not None
    assert state.watermark == retry_result.resulting_watermark
    assert state.last_successful_run_id == retry_result.run_id
    run_counts = dict(
        connection.execute(
            "SELECT status, COUNT(*) FROM pipeline_runs GROUP BY status"
        ).fetchall()
    )
    assert run_counts == {"SUCCESS": 2, "FAILED": 1}
    connection.close()
