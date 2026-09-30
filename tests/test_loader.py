from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import pytest

from incremental_pipeline.loader import (
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
