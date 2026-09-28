from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import duckdb

from incremental_pipeline.deduplication import deduplicate_records
from incremental_pipeline.loader import load_product_records
from incremental_pipeline.models import PipelineState, RunResult, RunStatus
from incremental_pipeline.repository import initialize_database
from incremental_pipeline.source import (
    DEFAULT_ALLOWED_FUTURE_SKEW,
    read_records_since,
)
from incremental_pipeline.state import get_pipeline_state, save_pipeline_state
from incremental_pipeline.windowing import calculate_extraction_start


def run_incremental_load(
    connection: duckdb.DuckDBPyConnection,
    source_path: str | Path,
    pipeline_name: str,
    initial_start_at: datetime,
    overlap: timedelta,
    allowed_future_skew: timedelta = DEFAULT_ALLOWED_FUTURE_SKEW,
) -> RunResult:
    """Run one successful incremental load against accumulated source history."""

    if not pipeline_name.strip():
        raise ValueError("pipeline_name must not be blank")

    initialize_database(connection)
    run_id = str(uuid4())
    run_started_at = datetime.now(UTC)
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
    winners = deduplicate_records(records)
    source_watermark = max(
        (record.version.updated_at for record in records),
        default=None,
    )
    resulting_watermark = _advance_watermark(
        previous_watermark,
        source_watermark,
    )

    connection.execute("BEGIN TRANSACTION")
    try:
        counts = load_product_records(
            connection,
            winners,
            run_id=run_id,
            loaded_at=run_started_at,
        )
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            [
                run_id,
                pipeline_name,
                run_started_at,
                finished_at,
                RunStatus.SUCCESS.value,
                previous_watermark,
                resulting_watermark,
                len(records),
                len(winners),
                counts.inserted,
                counts.updated,
                counts.ignored,
            ],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    return RunResult(
        run_id=run_id,
        status=RunStatus.SUCCESS,
        run_started_at=run_started_at,
        previous_watermark=previous_watermark,
        extraction_start=extraction_start,
        resulting_watermark=resulting_watermark,
        rows_received=len(records),
        rows_after_deduplication=len(winners),
        rows_inserted=counts.inserted,
        rows_updated=counts.updated,
        rows_ignored=counts.ignored,
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
