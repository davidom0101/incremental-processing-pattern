from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

import duckdb

from incremental_pipeline.deduplication import deduplicate_records
from incremental_pipeline.loader import (
    LoadCounts,
    apply_product_load_plan,
    classify_product_records,
)
from incremental_pipeline.models import PipelineState, RunResult, RunStatus
from incremental_pipeline.repository import initialize_database
from incremental_pipeline.source import (
    DEFAULT_ALLOWED_FUTURE_SKEW,
    read_records_since,
)
from incremental_pipeline.state import get_pipeline_state, save_pipeline_state
from incremental_pipeline.windowing import calculate_extraction_start


class FailurePoint(StrEnum):
    AFTER_CLASSIFICATION = "after_classification"
    AFTER_TARGET_WRITES = "after_target_writes"
    AFTER_STATE_ADVANCEMENT = "after_state_advancement"


class InjectedFailure(RuntimeError):
    pass


def run_incremental_load(
    connection: duckdb.DuckDBPyConnection,
    source_path: str | Path,
    pipeline_name: str,
    initial_start_at: datetime,
    overlap: timedelta,
    allowed_future_skew: timedelta = DEFAULT_ALLOWED_FUTURE_SKEW,
    failure_point: FailurePoint | None = None,
) -> RunResult:
    """Run one incremental load against accumulated source history."""

    if not pipeline_name.strip():
        raise ValueError("pipeline_name must not be blank")

    initialize_database(connection)
    run_id = str(uuid4())
    run_started_at = datetime.now(UTC)
    previous_watermark: datetime | None = None
    records_received = 0
    records_after_deduplication = 0
    counts = LoadCounts(inserted=0, updated=0, ignored=0)
    transaction_active = False

    try:
        previous_state = get_pipeline_state(connection, pipeline_name)
        previous_watermark = (
            previous_state.watermark if previous_state is not None else None
        )
        extraction_start = calculate_extraction_start(
            state=previous_state,
            initial_start_at=initial_start_at,
            overlap=overlap,
        )
        records = read_records_since(
            source_path=source_path,
            updated_at_gte=extraction_start,
            run_started_at=run_started_at,
            allowed_future_skew=allowed_future_skew,
        )
        records_received = len(records)
        winners = deduplicate_records(records)
        records_after_deduplication = len(winners)
        source_watermark = max(
            (record.version.updated_at for record in records),
            default=None,
        )
        resulting_watermark = _advance_watermark(
            previous_watermark,
            source_watermark,
        )
        load_plan = classify_product_records(connection, winners)
        _inject_failure(failure_point, FailurePoint.AFTER_CLASSIFICATION)

        connection.execute("BEGIN TRANSACTION")
        transaction_active = True
        counts = apply_product_load_plan(
            connection,
            load_plan,
            run_id=run_id,
            loaded_at=run_started_at,
        )
        _inject_failure(failure_point, FailurePoint.AFTER_TARGET_WRITES)
        finished_at = datetime.now(UTC)

        if resulting_watermark is not None:
            save_pipeline_state(
                connection,
                PipelineState(
                    pipeline_name=pipeline_name,
                    watermark=resulting_watermark,
                    last_successful_run_id=run_id,
                    updated_at=finished_at,
                ),
            )
        _inject_failure(failure_point, FailurePoint.AFTER_STATE_ADVANCEMENT)

        _insert_run_record(
            connection=connection,
            run_id=run_id,
            pipeline_name=pipeline_name,
            started_at=run_started_at,
            finished_at=finished_at,
            status=RunStatus.SUCCESS,
            previous_watermark=previous_watermark,
            resulting_watermark=resulting_watermark,
            rows_received=records_received,
            rows_after_deduplication=records_after_deduplication,
            counts=counts,
        )
        connection.execute("COMMIT")
        transaction_active = False
    except Exception as error:
        if transaction_active:
            try:
                connection.execute("ROLLBACK")
            except Exception as rollback_error:
                error.add_note(f"rollback also failed: {rollback_error}")

        try:
            _insert_run_record(
                connection=connection,
                run_id=run_id,
                pipeline_name=pipeline_name,
                started_at=run_started_at,
                finished_at=datetime.now(UTC),
                status=RunStatus.FAILED,
                previous_watermark=previous_watermark,
                resulting_watermark=None,
                rows_received=records_received,
                rows_after_deduplication=records_after_deduplication,
                counts=counts,
                error=error,
            )
        except Exception as metadata_error:
            error.add_note(f"failed to record pipeline failure: {metadata_error}")
        raise

    return RunResult(
        run_id=run_id,
        status=RunStatus.SUCCESS,
        run_started_at=run_started_at,
        previous_watermark=previous_watermark,
        extraction_start=extraction_start,
        resulting_watermark=resulting_watermark,
        rows_received=records_received,
        rows_after_deduplication=records_after_deduplication,
        rows_inserted=counts.inserted,
        rows_updated=counts.updated,
        rows_ignored=counts.ignored,
    )


def _inject_failure(
    requested: FailurePoint | None,
    current: FailurePoint,
) -> None:
    if requested is current:
        raise InjectedFailure(f"failure injected at {current.value}")


def _insert_run_record(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    pipeline_name: str,
    started_at: datetime,
    finished_at: datetime,
    status: RunStatus,
    previous_watermark: datetime | None,
    resulting_watermark: datetime | None,
    rows_received: int,
    rows_after_deduplication: int,
    counts: LoadCounts,
    error: Exception | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO pipeline_runs (
            run_id,
            pipeline_name,
            started_at,
            finished_at,
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
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            pipeline_name,
            started_at,
            finished_at,
            status.value,
            previous_watermark,
            resulting_watermark,
            rows_received,
            rows_after_deduplication,
            counts.inserted,
            counts.updated,
            counts.ignored,
            type(error).__name__ if error is not None else None,
            str(error)[:500] if error is not None else None,
        ],
    )


def _advance_watermark(
    previous_watermark: datetime | None,
    source_watermark: datetime | None,
) -> datetime | None:
    if previous_watermark is None:
        return source_watermark
    if source_watermark is None:
        return previous_watermark
    return max(previous_watermark, source_watermark)
