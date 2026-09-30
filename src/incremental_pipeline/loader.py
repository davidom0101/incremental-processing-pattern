from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import duckdb

from incremental_pipeline.models import ProductRecord, RecordVersion
from incremental_pipeline.timestamps import normalize_to_utc


@dataclass(frozen=True)
class LoadCounts:
    inserted: int
    updated: int
    ignored: int


@dataclass(frozen=True)
class ProductLoadPlan:
    inserts: tuple[ProductRecord, ...]
    updates: tuple[ProductRecord, ...]
    ignored: int


class TargetVersionConflictError(ValueError):
    """Raised when equal source and target versions disagree on payload."""

    def __init__(self, product_id: str, version: RecordVersion) -> None:
        """Describe the source and target version conflict."""

        self.product_id = product_id
        self.version = version
        super().__init__(
            "source and target payloads conflict for "
            f"product_id={product_id!r}, "
            f"updated_at={version.updated_at.isoformat()}, "
            f"source_sequence={version.source_sequence}"
        )


def load_product_records(
    connection: duckdb.DuckDBPyConnection,
    records: Sequence[ProductRecord],
    *,
    run_id: str,
    loaded_at: datetime,
) -> LoadCounts:
    """Classify all winners, then apply inserts and updates."""

    plan = classify_product_records(connection, records)
    return apply_product_load_plan(
        connection,
        plan,
        run_id=run_id,
        loaded_at=loaded_at,
    )


def classify_product_records(
    connection: duckdb.DuckDBPyConnection,
    records: Sequence[ProductRecord],
) -> ProductLoadPlan:
    """Compare winners with current target state without mutating it."""

    target_records = _read_target_records(
        connection,
        [record.product_id for record in records],
    )
    inserts: list[ProductRecord] = []
    updates: list[ProductRecord] = []
    ignored = 0

    # Complete classification first so a conflict cannot follow partial writes.
    for record in records:
        target_record = target_records.get(record.product_id)
        if target_record is None:
            inserts.append(record)
        elif record.version > target_record.version:
            updates.append(record)
        elif record.version < target_record.version:
            # A late stale winner must never move the target backwards.
            ignored += 1
        elif record.business_payload == target_record.business_payload:
            # Leave replays untouched so their load metadata does not change.
            ignored += 1
        else:
            raise TargetVersionConflictError(record.product_id, record.version)

    return ProductLoadPlan(
        inserts=tuple(inserts),
        updates=tuple(updates),
        ignored=ignored,
    )


def apply_product_load_plan(
    connection: duckdb.DuckDBPyConnection,
    plan: ProductLoadPlan,
    *,
    run_id: str,
    loaded_at: datetime,
) -> LoadCounts:
    """Write a previously classified product load plan."""

    if not run_id.strip():
        raise ValueError("run_id must not be blank")

    loaded_at_utc = normalize_to_utc(loaded_at, "loaded_at")
    if plan.inserts:
        connection.executemany(
            """
            INSERT INTO products_current (
                product_id,
                updated_at,
                source_sequence,
                name,
                category,
                price,
                is_active,
                loaded_at,
                last_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                _write_values(record, run_id, loaded_at_utc)
                for record in plan.inserts
            ],
        )

    if plan.updates:
        connection.executemany(
            """
            UPDATE products_current
            SET
                updated_at = ?,
                source_sequence = ?,
                name = ?,
                category = ?,
                price = ?,
                is_active = ?,
                loaded_at = ?,
                last_run_id = ?
            WHERE product_id = ?
            """,
            [
                _update_values(record, run_id, loaded_at_utc)
                for record in plan.updates
            ],
        )

    return LoadCounts(
        inserted=len(plan.inserts),
        updated=len(plan.updates),
        ignored=plan.ignored,
    )


def _read_target_records(
    connection: duckdb.DuckDBPyConnection,
    product_ids: list[str],
) -> dict[str, ProductRecord]:
    """Read target records for the requested product keys."""

    if not product_ids:
        return {}

    placeholders = ", ".join("?" for _ in product_ids)
    rows = connection.execute(
        f"""
        SELECT
            product_id,
            updated_at,
            source_sequence,
            name,
            category,
            price,
            is_active
        FROM products_current
        WHERE product_id IN ({placeholders})
        """,
        product_ids,
    ).fetchall()

    return {
        row[0]: ProductRecord(
            product_id=row[0],
            version=RecordVersion(
                updated_at=row[1],
                source_sequence=row[2],
            ),
            name=row[3],
            category=row[4],
            price=row[5],
            is_active=row[6],
        )
        for row in rows
    }


def _write_values(
    record: ProductRecord,
    run_id: str,
    loaded_at: datetime,
) -> tuple[object, ...]:
    """Build values for a product insert."""

    return (
        record.product_id,
        record.version.updated_at,
        record.version.source_sequence,
        record.name,
        record.category,
        record.price,
        record.is_active,
        loaded_at,
        run_id,
    )


def _update_values(
    record: ProductRecord,
    run_id: str,
    loaded_at: datetime,
) -> tuple[object, ...]:
    """Build values for a product update."""

    # The update uses the insert payload order but places the key last for WHERE.
    return (*_write_values(record, run_id, loaded_at)[1:], record.product_id)
