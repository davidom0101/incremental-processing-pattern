from collections.abc import Iterable

from incremental_pipeline.models import ProductRecord, RecordVersion


class ConflictingSourceVersionError(ValueError):
    """Raised when one source version has more than one business payload."""

    def __init__(self, product_id: str, version: RecordVersion) -> None:
        """Describe the ambiguous source version."""

        self.product_id = product_id
        self.version = version
        super().__init__(
            "conflicting source rows for "
            f"product_id={product_id!r}, "
            f"updated_at={version.updated_at.isoformat()}, "
            f"source_sequence={version.source_sequence}"
        )


def deduplicate_records(
    records: Iterable[ProductRecord],
) -> list[ProductRecord]:
    """Reject ambiguous versions and return one latest record per product."""

    # Conflicting old versions still make the source batch ambiguous.
    records_by_version: dict[tuple[str, RecordVersion], ProductRecord] = {}
    winners_by_product: dict[str, ProductRecord] = {}

    for record in records:
        identity = (record.product_id, record.version)
        existing_version = records_by_version.get(identity)

        if existing_version is not None:
            if existing_version.business_payload != record.business_payload:
                raise ConflictingSourceVersionError(
                    product_id=record.product_id,
                    version=record.version,
                )
            # Exact repeats are safe because they carry no competing information.
            continue

        records_by_version[identity] = record
        current_winner = winners_by_product.get(record.product_id)
        if current_winner is None or record.version > current_winner.version:
            winners_by_product[record.product_id] = record

    # Stable output keeps downstream writes independent of source row order.
    return [winners_by_product[product_id] for product_id in sorted(winners_by_product)]
