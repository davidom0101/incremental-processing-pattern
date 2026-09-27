from datetime import UTC, datetime
from decimal import Decimal

from incremental_pipeline.models import ProductRecord, RecordVersion


def test_business_payload_excludes_identity_and_version() -> None:
    first = ProductRecord(
        product_id="P001",
        version=RecordVersion(
            updated_at=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            source_sequence=1,
        ),
        name="Cordless drill",
        category="Tools",
        price=Decimal("89.99"),
        is_active=True,
    )
    second = ProductRecord(
        product_id="P002",
        version=RecordVersion(
            updated_at=datetime(2026, 1, 2, 10, 0, tzinfo=UTC),
            source_sequence=2,
        ),
        name="Cordless drill",
        category="Tools",
        price=Decimal("89.99"),
        is_active=True,
    )

    assert first.business_payload == second.business_payload
