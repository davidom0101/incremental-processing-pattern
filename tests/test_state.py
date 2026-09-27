from datetime import UTC, datetime, timedelta, timezone

import duckdb

from incremental_pipeline.models import PipelineState
from incremental_pipeline.repository import initialize_database
from incremental_pipeline.state import get_pipeline_state, save_pipeline_state


def test_missing_pipeline_state_returns_none() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)

    assert get_pipeline_state(connection, "products") is None

    connection.close()


def test_pipeline_state_round_trips_in_utc() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    utc_plus_two = timezone(timedelta(hours=2))
    state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 12, 0, tzinfo=utc_plus_two),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 12, 5, tzinfo=utc_plus_two),
    )

    save_pipeline_state(connection, state)

    stored_state = get_pipeline_state(connection, "products")
    assert stored_state == PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
    )
    connection.close()


def test_saving_existing_pipeline_state_updates_one_row() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    first_state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
    )
    second_state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 2, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-2",
        updated_at=datetime(2026, 1, 2, 10, 5, tzinfo=UTC),
    )

    save_pipeline_state(connection, first_state)
    save_pipeline_state(connection, second_state)

    assert get_pipeline_state(connection, "products") == second_state
    assert connection.execute("SELECT COUNT(*) FROM pipeline_state").fetchone() == (1,)
    connection.close()


def test_state_write_can_be_rolled_back_by_caller() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
    )

    connection.execute("BEGIN TRANSACTION")
    save_pipeline_state(connection, state)
    connection.execute("ROLLBACK")

    assert get_pipeline_state(connection, "products") is None
    connection.close()
