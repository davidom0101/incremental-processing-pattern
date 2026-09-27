from datetime import UTC, datetime, timedelta
from decimal import Decimal

import duckdb
import pytest

from incremental_pipeline.loader import (
    LoadCounts,
    TargetVersionConflictError,
    load_product_records,
)
from incremental_pipeline.models import ProductRecord, RecordVersion
from incremental_pipeline.repository import initialize_database

INITIAL_LOAD_TIME = datetime(2026, 1, 2, 10, 0, tzinfo=UTC)
NEXT_LOAD_TIME = datetime(2026, 1, 2, 11, 0, tzinfo=UTC)


def product_record(
    product_id: str,
    *,
    updated_at: datetime = datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
    source_sequence: int = 1,
    name: str = "Cordless drill",
) -> ProductRecord:
    return ProductRecord(
        product_id=product_id,
        version=RecordVersion(updated_at, source_sequence),
        name=name,
        category="Tools",
        price=Decimal("89.99"),
        is_active=True,
    )


def product_rows(connection: duckdb.DuckDBPyConnection) -> list[tuple]:
    return connection.execute(
        "SELECT * FROM products_current ORDER BY product_id"
    ).fetchall()


def test_empty_load_has_zero_counts() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)

    counts = load_product_records(
        connection,
        [],
        run_id="run-1",
        loaded_at=INITIAL_LOAD_TIME,
    )

    assert counts == LoadCounts(inserted=0, updated=0, ignored=0)
    connection.close()


def test_new_products_are_inserted() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    records = [product_record("P001"), product_record("P002")]

    counts = load_product_records(
        connection,
        records,
        run_id="run-1",
        loaded_at=INITIAL_LOAD_TIME,
    )

    assert counts == LoadCounts(inserted=2, updated=0, ignored=0)
    assert [row[0] for row in product_rows(connection)] == ["P001", "P002"]
    connection.close()


def test_mixed_batch_updates_inserts_and_ignores_without_replay_mutation() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    original_records = [
        product_record("P001"),
        product_record("P002", source_sequence=2),
        product_record("P003"),
    ]
    load_product_records(
        connection,
        original_records,
        run_id="run-1",
        loaded_at=INITIAL_LOAD_TIME,
    )
    before = {row[0]: row for row in product_rows(connection)}
    incoming = [
        product_record(
            "P001",
            updated_at=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
            name="Cordless drill with case",
        ),
        product_record("P002", source_sequence=1),
        product_record("P003"),
        product_record("P004"),
    ]

    counts = load_product_records(
        connection,
        incoming,
        run_id="run-2",
        loaded_at=NEXT_LOAD_TIME,
    )
    after = {row[0]: row for row in product_rows(connection)}

    assert counts == LoadCounts(inserted=1, updated=1, ignored=2)
    assert after["P001"][3] == "Cordless drill with case"
    assert after["P001"][7:] == (NEXT_LOAD_TIME, "run-2")
    assert after["P002"] == before["P002"]
    assert after["P003"] == before["P003"]
    assert after["P004"][7:] == (NEXT_LOAD_TIME, "run-2")
    connection.close()


def test_equal_version_conflict_fails_before_any_mutation() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    original = product_record("P001")
    load_product_records(
        connection,
        [original],
        run_id="run-1",
        loaded_at=INITIAL_LOAD_TIME,
    )
    before = product_rows(connection)
    conflicting = product_record("P001", name="Different drill")

    with pytest.raises(TargetVersionConflictError) as error:
        load_product_records(
            connection,
            [product_record("P002"), conflicting],
            run_id="run-2",
            loaded_at=NEXT_LOAD_TIME,
        )

    assert error.value.product_id == "P001"
    assert product_rows(connection) == before
    connection.close()


def test_product_writes_can_be_rolled_back_by_caller() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)

    connection.execute("BEGIN TRANSACTION")
    load_product_records(
        connection,
        [product_record("P001")],
        run_id="run-1",
        loaded_at=INITIAL_LOAD_TIME + timedelta(minutes=1),
    )
    connection.execute("ROLLBACK")

    assert product_rows(connection) == []
    connection.close()
