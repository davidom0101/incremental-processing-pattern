from datetime import UTC, datetime

import pytest

from incremental_pipeline.models import RecordVersion


def test_later_timestamp_is_newer() -> None:
    older = RecordVersion(
        updated_at=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        source_sequence=10,
    )
    newer = RecordVersion(
        updated_at=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
        source_sequence=1,
    )

    assert newer > older


def test_source_sequence_breaks_equal_timestamp_tie() -> None:
    timestamp = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)

    older = RecordVersion(updated_at=timestamp, source_sequence=10)
    newer = RecordVersion(updated_at=timestamp, source_sequence=11)

    assert newer > older


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        RecordVersion(
            updated_at=datetime(2026, 1, 1, 10, 0),
            source_sequence=1,
        )


def test_negative_source_sequence_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        RecordVersion(
            updated_at=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            source_sequence=-1,
        )
