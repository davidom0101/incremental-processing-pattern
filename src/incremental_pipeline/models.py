from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal


@dataclass(frozen=True, order=True)
class RecordVersion:
    """Source-defined ordering coordinates for a record version."""

    updated_at: datetime
    source_sequence: int

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")

        object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))

        if self.source_sequence < 0:
            raise ValueError("source_sequence must be non-negative")


@dataclass(frozen=True)
class ProductRecord:
    """A versioned product received from the source."""

    product_id: str
    version: RecordVersion
    name: str
    category: str
    price: Decimal
    is_active: bool

    @property
    def business_payload(self) -> tuple[str, str, Decimal, bool]:
        return self.name, self.category, self.price, self.is_active
