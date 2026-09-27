from datetime import UTC, datetime, timedelta, timezone

import pytest

from incremental_pipeline.models import PipelineState
from incremental_pipeline.windowing import calculate_extraction_start


def test_initial_run_uses_configured_start_in_utc() -> None:
    utc_plus_two = timezone(timedelta(hours=2))

    extraction_start = calculate_extraction_start(
        state=None,
        initial_start_at=datetime(2026, 1, 1, 12, 0, tzinfo=utc_plus_two),
        overlap=timedelta(hours=24),
    )

    assert extraction_start == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    assert extraction_start.tzinfo is UTC


def test_subsequent_run_subtracts_overlap_from_watermark() -> None:
    state = PipelineState(
        pipeline_name="products",
        watermark=datetime(2026, 1, 2, 10, 0, tzinfo=UTC),
        last_successful_run_id="run-1",
        updated_at=datetime(2026, 1, 2, 10, 5, tzinfo=UTC),
    )

    extraction_start = calculate_extraction_start(
        state=state,
        initial_start_at=datetime(2020, 1, 1, tzinfo=UTC),
        overlap=timedelta(hours=24),
    )

    assert extraction_start == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)


def test_negative_overlap_is_rejected() -> None:
    with pytest.raises(ValueError, match="overlap must be non-negative"):
        calculate_extraction_start(
            state=None,
            initial_start_at=datetime(2026, 1, 1, tzinfo=UTC),
            overlap=timedelta(seconds=-1),
        )
