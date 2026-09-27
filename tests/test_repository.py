from collections.abc import Iterator
from datetime import UTC, datetime

import duckdb
import pytest

from incremental_pipeline.repository import initialize_database


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    database = duckdb.connect(":memory:")
    initialize_database(database)
    yield database
    database.close()


def test_database_initialization_creates_required_tables(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    tables = {
        row[0]
        for row in connection.execute("SHOW TABLES").fetchall()
    }

    assert tables == {"pipeline_runs", "pipeline_state", "products_current"}


def test_database_initialization_is_idempotent(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    initialize_database(connection)

    assert connection.execute("SELECT COUNT(*) FROM pipeline_state").fetchone() == (0,)


def test_product_key_must_be_unique(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    product = [
        "P001",
        datetime(2026, 1, 1, tzinfo=UTC),
        1,
        "Drill",
        "Tools",
        "89.99",
        True,
        datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        "run-1",
    ]
    connection.execute(
        "INSERT INTO products_current VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        product,
    )

    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            "INSERT INTO products_current VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            product,
        )


@pytest.mark.parametrize(
    ("source_sequence", "price"),
    [(-1, "89.99"), (1, "-0.01")],
)
def test_product_checks_reject_invalid_values(
    connection: duckdb.DuckDBPyConnection,
    source_sequence: int,
    price: str,
) -> None:
    with pytest.raises(duckdb.ConstraintException):
        connection.execute(
            """
            INSERT INTO products_current VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "P001",
                datetime(2026, 1, 1, tzinfo=UTC),
                source_sequence,
                "Drill",
                "Tools",
                price,
                True,
                datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
                "run-1",
            ],
        )


def insert_run(
    connection: duckdb.DuckDBPyConnection,
    **changes: object,
) -> None:
    values = {
        "run_id": "run-1",
        "pipeline_name": "products",
        "started_at": datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 1, 1, 10, 1, tzinfo=UTC),
        "status": "SUCCESS",
        "previous_watermark": None,
        "resulting_watermark": datetime(2026, 1, 1, 9, 0, tzinfo=UTC),
        "rows_received": 10,
        "rows_after_deduplication": 10,
        "rows_inserted": 10,
        "rows_updated": 0,
        "rows_ignored": 0,
        "error_type": None,
        "error_message": None,
    }
    values.update(changes)
    connection.execute(
        """
        INSERT INTO pipeline_runs VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        list(values.values()),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "UNKNOWN"},
        {"rows_received": -1},
        {"rows_after_deduplication": 11},
        {"rows_ignored": 1},
        {
            "finished_at": datetime(2026, 1, 1, 9, 59, tzinfo=UTC),
        },
        {
            "status": "FAILED",
            "resulting_watermark": datetime(2026, 1, 1, 9, 0, tzinfo=UTC),
        },
    ],
)
def test_run_checks_reject_invalid_values(
    connection: duckdb.DuckDBPyConnection,
    changes: dict[str, object],
) -> None:
    with pytest.raises(duckdb.ConstraintException):
        insert_run(connection, **changes)


def test_failed_run_can_store_partial_counts(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    insert_run(
        connection,
        status="FAILED",
        resulting_watermark=None,
        rows_inserted=0,
        rows_updated=0,
        rows_ignored=0,
        error_type="SourceDataError",
        error_message="invalid source row",
    )

    stored_status = connection.execute(
        "SELECT status FROM pipeline_runs WHERE run_id = 'run-1'"
    ).fetchone()
    assert stored_status == ("FAILED",)


def test_run_id_must_be_unique(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    insert_run(connection)

    with pytest.raises(duckdb.ConstraintException):
        insert_run(connection)
