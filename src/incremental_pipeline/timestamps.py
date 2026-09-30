from datetime import UTC, datetime


def normalize_to_utc(value: datetime, field_name: str) -> datetime:
    """Reject naive values and preserve the same instant in UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")

    return value.astimezone(UTC)
