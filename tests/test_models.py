from datetime import UTC, datetime, timedelta, timezone

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


def test_timestamp_is_normalized_to_utc() -> None:
    utc_plus_two = timezone(timedelta(hours=2))

    version = RecordVersion(
        updated_at=datetime(2026, 1, 1, 12, 0, tzinfo=utc_plus_two),
        source_sequence=1,
    )

    assert version.updated_at == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    assert version.updated_at.tzinfo is UTC


def test_equivalent_instants_have_equal_versions() -> None:
    utc_version = RecordVersion(
        updated_at=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        source_sequence=1,
    )
    offset_version = RecordVersion(
        updated_at=datetime(
            2026,
            1,
            1,
            12,
            0,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        source_sequence=1,
    )

    assert offset_version == utc_version
