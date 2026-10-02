from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from incremental_pipeline.timestamps import normalize_to_utc


@dataclass(frozen=True, order=True)
class RecordVersion:
    """Source-defined ordering coordinates for a record version."""

    updated_at: datetime
    source_sequence: int

    def __post_init__(self) -> None:
        """Normalize and validate the version coordinates."""

        object.__setattr__(
            self,
            "updated_at",
            normalize_to_utc(self.updated_at, "updated_at"),
        )

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
        """Return the fields used to distinguish replays from conflicts."""

        return self.name, self.category, self.price, self.is_active


@dataclass(frozen=True)
class PipelineState:
    """The last successfully committed checkpoint for a pipeline."""

    pipeline_name: str
    watermark: datetime
    last_successful_run_id: str
    updated_at: datetime

    def __post_init__(self) -> None:
        """Normalize the stored state timestamps."""

        object.__setattr__(
            self,
            "watermark",
            normalize_to_utc(self.watermark, "watermark"),
        )
        object.__setattr__(
            self,
            "updated_at",
            normalize_to_utc(self.updated_at, "updated_at"),
        )


class RunStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RunResult:
    """The observable outcome and processing counts for one pipeline run."""

    run_id: str
    status: RunStatus
    run_started_at: datetime
    previous_watermark: datetime | None
    extraction_start: datetime
    resulting_watermark: datetime | None
    rows_received: int
    rows_after_deduplication: int
    rows_inserted: int
    rows_updated: int
    rows_ignored: int

    def __post_init__(self) -> None:
        """Normalize timestamps and enforce result invariants."""

        if not isinstance(self.status, RunStatus):
            raise ValueError("status must be a RunStatus")

        object.__setattr__(
            self,
            "run_started_at",
            normalize_to_utc(self.run_started_at, "run_started_at"),
        )
        object.__setattr__(
            self,
            "extraction_start",
            normalize_to_utc(self.extraction_start, "extraction_start"),
        )

        for field_name in ("previous_watermark", "resulting_watermark"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    normalize_to_utc(value, field_name),
                )

        counters = {
            "rows_received": self.rows_received,
            "rows_after_deduplication": self.rows_after_deduplication,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_ignored": self.rows_ignored,
        }
        for field_name, value in counters.items():
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")

        if self.rows_after_deduplication > self.rows_received:
            raise ValueError("rows_after_deduplication cannot exceed rows_received")

        rows_classified = self.rows_inserted + self.rows_updated + self.rows_ignored
        # Every deduplicated winner has exactly one load outcome on success.
        if (
            self.status is RunStatus.SUCCESS
            and rows_classified != self.rows_after_deduplication
        ):
            raise ValueError(
                "successful run counters must reconcile with rows_after_deduplication"
            )

        if self.status is RunStatus.FAILED and self.resulting_watermark is not None:
            raise ValueError("failed runs cannot have a resulting watermark")
