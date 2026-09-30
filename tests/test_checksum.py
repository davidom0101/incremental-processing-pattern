import hashlib

import duckdb

from incremental_pipeline.checksum import (
    business_state_checksum,
    canonical_business_state,
)
from incremental_pipeline.repository import initialize_database


def test_business_state_uses_canonical_json_lines() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)
    connection.execute(
        """
        INSERT INTO products_current VALUES
            (
                'Pé',
                '2026-01-02 03:04:05.6+00',
                2,
                'Café',
                'Kitchen',
                19.9,
                true,
                '2026-01-03 00:00:00+00',
                'run-2'
            ),
            (
                'P"1',
                '2026-01-01 00:00:00+00',
                1,
                'Line\nBreak',
                'Tools',
                5,
                false,
                '2026-01-03 00:00:00+00',
                'run-1'
            )
        """
    )
    expected = (
        '{"product_id":"P\\"1","updated_at":"2026-01-01T00:00:00.000000Z",'
        '"source_sequence":1,"name":"Line\\nBreak","category":"Tools",'
        '"price":"5.00","is_active":false}\n'
        '{"product_id":"Pé","updated_at":"2026-01-02T03:04:05.600000Z",'
        '"source_sequence":2,"name":"Café","category":"Kitchen",'
        '"price":"19.90","is_active":true}\n'
    ).encode("utf-8")

    assert canonical_business_state(connection) == expected
    assert business_state_checksum(connection) == hashlib.sha256(expected).hexdigest()
    connection.close()


def test_empty_business_state_hashes_empty_bytes() -> None:
    connection = duckdb.connect(":memory:")
    initialize_database(connection)

    assert canonical_business_state(connection) == b""
    assert business_state_checksum(connection) == hashlib.sha256(b"").hexdigest()
    connection.close()
