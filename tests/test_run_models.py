from datetime import UTC, datetime

import pytest

from incremental_pipeline.models import RunResult, RunStatus


def make_run_result(**changes: object) -> RunResult:
    values = {
        "run_id": "run-2",
        "status": RunStatus.SUCCESS,
        "run_started_at": datetime(2026, 1, 2, 10, 0, tzinfo=UTC),
        "previous_watermark": datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        "extraction_start": datetime(2025, 12, 31, 10, 0, tzinfo=UTC),
        "resulting_watermark": datetime(2026, 1, 2, 9, 55, tzinfo=UTC),
        "rows_received": 12,
        "rows_after_deduplication": 10,
        "rows_inserted": 2,
        "rows_updated": 3,
        "rows_ignored": 5,
    }
    values.update(changes)
    return RunResult(**values)  # type: ignore[arg-type]


def test_deduplicated_count_cannot_exceed_received_count() -> None:
    with pytest.raises(
        ValueError,
        match="rows_after_deduplication cannot exceed rows_received",
    ):
        make_run_result(rows_received=9)


def test_successful_run_counters_must_reconcile() -> None:
    with pytest.raises(ValueError, match="successful run counters must reconcile"):
        make_run_result(rows_ignored=4)


def test_failed_run_can_stop_before_all_rows_are_classified() -> None:
    result = make_run_result(
        status=RunStatus.FAILED,
        resulting_watermark=None,
        rows_received=12,
        rows_after_deduplication=10,
        rows_inserted=0,
        rows_updated=0,
        rows_ignored=0,
    )

    assert result.status is RunStatus.FAILED


def test_failed_run_cannot_have_a_resulting_watermark() -> None:
    with pytest.raises(ValueError, match="failed runs cannot have"):
        make_run_result(status=RunStatus.FAILED)
