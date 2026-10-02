from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import permutations

import pytest

from incremental_pipeline.deduplication import (
    ConflictingSourceVersionError,
    deduplicate_records,
)
from incremental_pipeline.models import ProductRecord, RecordVersion


def product_record(
    *,
    product_id: str = "P001",
    updated_at: datetime = datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
    source_sequence: int = 1,
    name: str = "Cordless drill",
) -> ProductRecord:
    return ProductRecord(
        product_id=product_id,
        version=RecordVersion(
            updated_at=updated_at,
            source_sequence=source_sequence,
        ),
        name=name,
        category="Tools",
        price=Decimal("89.99"),
        is_active=True,
    )


def test_exact_duplicates_collapse_to_one_record() -> None:
    record = product_record()

    assert deduplicate_records([record, record]) == [record]


def test_latest_version_wins_for_each_product() -> None:
    older = product_record()
    newer = product_record(
        updated_at=older.version.updated_at + timedelta(hours=1),
        name="Cordless drill with case",
    )
    other_product = product_record(product_id="P002")

    winners = deduplicate_records([newer, other_product, older])

    assert winners == [newer, other_product]


def test_winners_do_not_depend_on_input_order() -> None:
    records = [
        product_record(source_sequence=1),
        product_record(source_sequence=2, name="Cordless drill with case"),
        product_record(product_id="P002"),
    ]
    expected = deduplicate_records(records)

    for shuffled_records in permutations(records):
        assert deduplicate_records(shuffled_records) == expected


def test_conflicting_payloads_fail_even_when_version_is_not_the_winner() -> None:
    old_version = product_record()
    conflicting_old_version = product_record(name="Different drill")
    newer_version = product_record(
        updated_at=old_version.version.updated_at + timedelta(hours=1),
        source_sequence=2,
    )

    with pytest.raises(ConflictingSourceVersionError) as error:
        deduplicate_records([newer_version, old_version, conflicting_old_version])

    assert error.value.product_id == "P001"
    assert error.value.version == old_version.version
