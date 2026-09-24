from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, order=True)
class RecordVersion:
    """Source-defined ordering coordinates for a record version."""

    updated_at: datetime
    source_sequence: int

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")

        if self.source_sequence < 0:
            raise ValueError("source_sequence must be non-negative")
