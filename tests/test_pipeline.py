import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from incremental_pipeline.models import RunStatus
from incremental_pipeline.pipeline import run_incremental_load
from incremental_pipeline.state import get_pipeline_state


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


def test_initial_load_commits_products_watermark_and_run_metadata(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    source_path = write_source_history(tmp_path)
    initial_start = datetime(2026, 1, 1, tzinfo=UTC)

    result = run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name="products",
        initial_start_at=initial_start,
        overlap=timedelta(hours=24),
    )

    assert result.status is RunStatus.SUCCESS
    assert result.previous_watermark is None
    assert result.extraction_start == initial_start
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
    initial_start = datetime(2026, 1, 1, tzinfo=UTC)
    overlap = timedelta(hours=24)
    first_result = run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name="products",
        initial_start_at=initial_start,
        overlap=overlap,
    )
    products_before_replay = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()

    replay_result = run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name="products",
        initial_start_at=initial_start,
        overlap=overlap,
    )

    products_after_replay = connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()
    assert products_after_replay == products_before_replay
    assert first_result.resulting_watermark is not None
    assert replay_result.previous_watermark == first_result.resulting_watermark
    assert replay_result.resulting_watermark == first_result.resulting_watermark
    assert replay_result.extraction_start == (
        first_result.resulting_watermark - overlap
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
