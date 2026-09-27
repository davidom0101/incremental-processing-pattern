import csv
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from incremental_pipeline.models import ProductRecord, RecordVersion
from incremental_pipeline.timestamps import normalize_to_utc

SOURCE_FIELDS = {
    "product_id",
    "updated_at",
    "source_sequence",
    "name",
    "category",
    "price",
    "is_active",
}
DEFAULT_ALLOWED_FUTURE_SKEW = timedelta(minutes=5)
MAX_PRICE = Decimal("9999999999999999.99")
PRICE_QUANTUM = Decimal("0.01")


class SourceDataError(ValueError):
    """Raised when source history does not satisfy its data contract."""


def read_records_since(
    source_path: str | Path,
    updated_at_gte: datetime,
    *,
    run_started_at: datetime,
    allowed_future_skew: timedelta = DEFAULT_ALLOWED_FUTURE_SKEW,
) -> list[ProductRecord]:
    extraction_start = normalize_to_utc(updated_at_gte, "updated_at_gte")
    run_start = normalize_to_utc(run_started_at, "run_started_at")
    if allowed_future_skew < timedelta(0):
        raise ValueError("allowed_future_skew must be non-negative")

    latest_allowed_timestamp = run_start + allowed_future_skew
    records: list[ProductRecord] = []

    with Path(source_path).open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        _validate_header(reader.fieldnames)

        for row_number, row in enumerate(reader, start=2):
            updated_at = _parse_timestamp(row.get("updated_at"), row_number)
            if updated_at < extraction_start:
                continue
            if updated_at > latest_allowed_timestamp:
                raise SourceDataError(
                    f"row {row_number}: updated_at exceeds allowed future skew"
                )

            records.append(_parse_record(row, row_number, updated_at))

    return records


def _validate_header(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise SourceDataError("source file must contain a header row")

    missing_fields = SOURCE_FIELDS.difference(fieldnames)
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise SourceDataError(f"missing required columns: {missing}")


def _parse_record(
    row: dict[str, str | None],
    row_number: int,
    updated_at: datetime,
) -> ProductRecord:
    product_id = _required_text(row, "product_id", row_number)
    name = _required_text(row, "name", row_number)
    category = _required_text(row, "category", row_number)

    return ProductRecord(
        product_id=product_id,
        version=RecordVersion(
            updated_at=updated_at,
            source_sequence=_parse_sequence(row.get("source_sequence"), row_number),
        ),
        name=name,
        category=category,
        price=_parse_price(row.get("price"), row_number),
        is_active=_parse_boolean(row.get("is_active"), row_number),
    )


def _required_text(
    row: dict[str, str | None],
    field_name: str,
    row_number: int,
) -> str:
    value = row.get(field_name)
    if value is None or not value.strip():
        raise SourceDataError(f"row {row_number}: {field_name} must not be blank")

    return value.strip()


def _parse_timestamp(value: str | None, row_number: int) -> datetime:
    if value is None or not value.strip():
        raise SourceDataError(f"row {row_number}: updated_at must not be blank")

    try:
        timestamp_text = value.strip()
        if timestamp_text.endswith("Z"):
            timestamp_text = f"{timestamp_text[:-1]}+00:00"
        parsed = datetime.fromisoformat(timestamp_text)
        return normalize_to_utc(parsed, "updated_at")
    except ValueError as error:
        raise SourceDataError(
            f"row {row_number}: updated_at must be a timezone-aware timestamp"
        ) from error


def _parse_sequence(value: str | None, row_number: int) -> int:
    if value is None or not value.strip():
        raise SourceDataError(f"row {row_number}: source_sequence must not be blank")

    try:
        sequence = int(value)
    except ValueError as error:
        raise SourceDataError(
            f"row {row_number}: source_sequence must be an integer"
        ) from error

    if sequence < 0:
        raise SourceDataError(
            f"row {row_number}: source_sequence must be non-negative"
        )

    return sequence


def _parse_price(value: str | None, row_number: int) -> Decimal:
    if value is None or not value.strip():
        raise SourceDataError(f"row {row_number}: price must not be blank")

    try:
        price = Decimal(value)
    except InvalidOperation as error:
        raise SourceDataError(f"row {row_number}: price must be a decimal") from error

    if not price.is_finite() or price < 0 or price > MAX_PRICE:
        raise SourceDataError(
            f"row {row_number}: price must fit decimal(18, 2) and be non-negative"
        )

    try:
        normalized_price = price.quantize(PRICE_QUANTUM)
    except InvalidOperation as error:
        raise SourceDataError(
            f"row {row_number}: price must fit decimal(18, 2) and be non-negative"
        ) from error

    if normalized_price != price:
        raise SourceDataError(
            f"row {row_number}: price must have at most two decimal places"
        )

    return normalized_price


def _parse_boolean(value: str | None, row_number: int) -> bool:
    if value is None:
        raise SourceDataError(f"row {row_number}: is_active must not be blank")

    normalized_value = value.strip().lower()
    if normalized_value == "true":
        return True
    if normalized_value == "false":
        return False

    raise SourceDataError(
        f"row {row_number}: is_active must be either true or false"
    )
