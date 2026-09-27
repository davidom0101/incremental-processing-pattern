from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from incremental_pipeline.models import PipelineState, RunResult, RunStatus


def test_pipeline_state_normalizes_timestamps_to_utc() -> None:
    utc_plus_two = timezone(timedelta(hours=2))

    state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 12, 0, tzinfo=utc_plus_two),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 12, 5, tzinfo=utc_plus_two),
    )

    assert state.watermark == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    assert state.updated_at == datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    assert state.watermark.tzinfo is UTC
    assert state.updated_at.tzinfo is UTC


@pytest.mark.parametrize("field_name", ["watermark", "updated_at"])
def test_pipeline_state_rejects_naive_timestamps(field_name: str) -> None:
    values = {
        "pipeline_name": "products",
        "watermark": datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        "last_successful_run_id": "run-1",
        "updated_at": datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
    }
    values[field_name] = datetime(2026, 1, 1, 10, 0)

    with pytest.raises(ValueError, match=f"{field_name} must be timezone-aware"):
        PipelineState(**values)  # type: ignore[arg-type]


def test_pipeline_state_is_immutable() -> None:
    state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
    )

    with pytest.raises(FrozenInstanceError):
        state.pipeline_name = "orders"  # type: ignore[misc]


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


def test_run_status_uses_persisted_values() -> None:
    assert RunStatus.SUCCESS.value == "SUCCESS"
    assert RunStatus.FAILED.value == "FAILED"


def test_run_result_requires_a_run_status() -> None:
    with pytest.raises(ValueError, match="status must be a RunStatus"):
        make_run_result(status="SUCCESS")


def test_run_result_normalizes_timestamps_to_utc() -> None:
    utc_plus_two = timezone(timedelta(hours=2))

    result = make_run_result(
        run_started_at=datetime(2026, 1, 2, 12, 0, tzinfo=utc_plus_two),
        previous_watermark=datetime(2026, 1, 1, 12, 0, tzinfo=utc_plus_two),
        extraction_start=datetime(2025, 12, 31, 12, 0, tzinfo=utc_plus_two),
        resulting_watermark=datetime(2026, 1, 2, 11, 55, tzinfo=utc_plus_two),
    )

    assert result.run_started_at == datetime(2026, 1, 2, 10, 0, tzinfo=UTC)
    assert result.previous_watermark == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    assert result.extraction_start == datetime(2025, 12, 31, 10, 0, tzinfo=UTC)
    assert result.resulting_watermark == datetime(2026, 1, 2, 9, 55, tzinfo=UTC)


@pytest.mark.parametrize(
    "field_name",
    [
        "run_started_at",
        "previous_watermark",
        "extraction_start",
        "resulting_watermark",
    ],
)
def test_run_result_rejects_naive_timestamps(field_name: str) -> None:
    with pytest.raises(ValueError, match=f"{field_name} must be timezone-aware"):
        make_run_result(**{field_name: datetime(2026, 1, 1, 10, 0)})


@pytest.mark.parametrize(
    "field_name",
    [
        "rows_received",
        "rows_after_deduplication",
        "rows_inserted",
        "rows_updated",
        "rows_ignored",
    ],
)
def test_run_result_rejects_negative_counters(field_name: str) -> None:
    with pytest.raises(ValueError, match=f"{field_name} must be non-negative"):
        make_run_result(**{field_name: -1})


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
