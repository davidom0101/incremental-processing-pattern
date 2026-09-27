import csv
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from incremental_pipeline.models import ProductRecord
from incremental_pipeline.source import SourceDataError, read_records_since

FIELDNAMES = [
    "product_id",
    "updated_at",
    "source_sequence",
    "name",
    "category",
    "price",
    "is_active",
]


def product_row(**changes: str) -> dict[str, str]:
    row = {
        "product_id": "P001",
        "updated_at": "2026-01-01T10:00:00Z",
        "source_sequence": "1",
        "name": "Cordless drill",
        "category": "Tools",
        "price": "89.99",
        "is_active": "true",
    }
    row.update(changes)
    return row


def write_source(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str] = FIELDNAMES,
) -> Path:
    source_path = path / "products.csv"
    with source_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return source_path


def read_source(
    source_path: Path,
    *,
    updated_at_gte: datetime = datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
    run_started_at: datetime = datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    allowed_future_skew: timedelta = timedelta(minutes=5),
) -> list[ProductRecord]:
    return read_records_since(
        source_path=source_path,
        updated_at_gte=updated_at_gte,
        run_started_at=run_started_at,
        allowed_future_skew=allowed_future_skew,
    )


def test_reader_parses_product_record(tmp_path: Path) -> None:
    source_path = write_source(
        tmp_path,
        [
            product_row(
                updated_at="2026-01-01T12:00:00+02:00",
                price="89.9",
                is_active="FALSE",
            )
        ],
    )

    records = read_source(source_path)

    assert len(records) == 1
    assert records[0].product_id == "P001"
    assert records[0].version.updated_at == datetime(
        2026, 1, 1, 10, 0, tzinfo=UTC
    )
    assert records[0].price == Decimal("89.90")
    assert records[0].is_active is False


def test_reader_uses_an_inclusive_window_and_preserves_source_order(
    tmp_path: Path,
) -> None:
    source_path = write_source(
        tmp_path,
        [
            product_row(product_id="OLD", updated_at="2026-01-01T09:59:59Z"),
            product_row(product_id="BOUNDARY"),
            product_row(product_id="NEWER", updated_at="2026-01-01T11:00:00Z"),
        ],
    )

    records = read_source(source_path)

    assert [record.product_id for record in records] == ["BOUNDARY", "NEWER"]


def test_record_at_future_skew_boundary_is_accepted(tmp_path: Path) -> None:
    source_path = write_source(
        tmp_path,
        [product_row(updated_at="2026-01-01T12:05:00Z")],
    )

    records = read_source(source_path)

    assert len(records) == 1


def test_record_beyond_future_skew_boundary_is_rejected(tmp_path: Path) -> None:
    source_path = write_source(
        tmp_path,
        [product_row(updated_at="2026-01-01T12:05:00.000001Z")],
    )

    with pytest.raises(SourceDataError, match="exceeds allowed future skew"):
        read_source(source_path)


def test_missing_required_column_is_rejected(tmp_path: Path) -> None:
    fieldnames = [field for field in FIELDNAMES if field != "category"]
    row = product_row()
    del row["category"]
    source_path = write_source(tmp_path, [row], fieldnames)

    with pytest.raises(SourceDataError, match="missing required columns: category"):
        read_source(source_path)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("product_id", "  ", "product_id must not be blank"),
        ("updated_at", "not-a-date", "timezone-aware timestamp"),
        ("source_sequence", "-1", "must be non-negative"),
        ("price", "-0.01", "price must fit decimal"),
        ("is_active", "yes", "must be either true or false"),
    ],
)
def test_invalid_selected_record_is_rejected(
    tmp_path: Path,
    field_name: str,
    value: str,
    message: str,
) -> None:
    source_path = write_source(tmp_path, [product_row(**{field_name: value})])

    with pytest.raises(SourceDataError, match=message):
        read_source(source_path)


def test_payload_of_record_before_window_is_not_parsed(tmp_path: Path) -> None:
    source_path = write_source(
        tmp_path,
        [
            product_row(
                product_id="OLD",
                updated_at="2026-01-01T09:59:59Z",
                price="not-a-price",
            ),
            product_row(product_id="CURRENT"),
        ],
    )

    records = read_source(source_path)

    assert [record.product_id for record in records] == ["CURRENT"]
