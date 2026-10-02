import hashlib
import json
from datetime import datetime
from decimal import Decimal

import duckdb

from incremental_pipeline.timestamps import normalize_to_utc


def canonical_business_state(
    connection: duckdb.DuckDBPyConnection,
) -> bytes:
    """Return the target's business state as canonical JSON Lines."""

    rows = connection.execute(
        """
        SELECT
            product_id,
            updated_at,
            source_sequence,
            name,
            category,
            price,
            is_active
        FROM products_current
        """
    ).fetchall()

    # Python ordering follows the Unicode ordering required by the checksum.
    lines = (
        _canonical_product_line(row) for row in sorted(rows, key=lambda row: row[0])
    )
    return b"".join(lines)


def business_state_checksum(
    connection: duckdb.DuckDBPyConnection,
) -> str:
    """Return the SHA-256 digest of the canonical business state."""

    return hashlib.sha256(canonical_business_state(connection)).hexdigest()


def _canonical_product_line(row: tuple[object, ...]) -> bytes:
    """Encode one product as a canonical JSON Lines record."""

    product_id, updated_at, source_sequence, name, category, price, is_active = row
    if not isinstance(updated_at, datetime):
        raise TypeError("updated_at must be a datetime")
    if not isinstance(price, Decimal):
        raise TypeError("price must be a Decimal")

    # Field insertion order and JSON settings below are part of the byte contract.
    payload = {
        "product_id": product_id,
        "updated_at": normalize_to_utc(updated_at, "updated_at").strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        "source_sequence": source_sequence,
        "name": name,
        "category": category,
        "price": f"{price:.2f}",
        "is_active": is_active,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return encoded + b"\n"
