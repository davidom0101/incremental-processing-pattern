import csv
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import duckdb

from incremental_pipeline.checksum import business_state_checksum
from incremental_pipeline.models import RunResult
from incremental_pipeline.pipeline import (
    FailurePoint,
    InjectedFailure,
    run_incremental_load,
)
from incremental_pipeline.state import get_pipeline_state

PIPELINE_NAME = "products"
INITIAL_START_AT = datetime(2026, 1, 1, tzinfo=UTC)
OVERLAP = timedelta(hours=24)
INITIAL_SOURCE = Path(__file__).parent / "source" / "products_initial.csv"
CHANGES_DIRECTORY = Path(__file__).parent / "source" / "changes"


def run_products(
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
    failure_point: FailurePoint | None = None,
) -> RunResult:
    """Run the product pipeline with the demo settings."""

    return run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name=PIPELINE_NAME,
        initial_start_at=INITIAL_START_AT,
        overlap=OVERLAP,
        failure_point=failure_point,
    )


def print_run(label: str, result: RunResult, checksum: str) -> None:
    """Print one demo run summary."""

    print(label)
    print(
        "  received={0.rows_received} deduplicated={0.rows_after_deduplication} "
        "inserted={0.rows_inserted} updated={0.rows_updated} "
        "ignored={0.rows_ignored}".format(result)
    )
    print(f"  watermark={result.resulting_watermark.isoformat()}")
    print(f"  checksum={checksum}")


def append_fixture(source_path: Path, fixture_path: Path) -> None:
    """Append one change fragment to the accumulated source history."""

    with fixture_path.open(newline="", encoding="utf-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        with source_path.open("a", newline="", encoding="utf-8") as source_file:
            writer = csv.DictWriter(source_file, fieldnames=reader.fieldnames)
            writer.writerows(reader)


def expect_counts(result: RunResult, expected: tuple[int, int, int, int, int]) -> None:
    """Fail when a demo run returns unexpected counters."""

    actual = (
        result.rows_received,
        result.rows_after_deduplication,
        result.rows_inserted,
        result.rows_updated,
        result.rows_ignored,
    )
    if actual != expected:
        raise RuntimeError(f"unexpected counters: expected {expected}, got {actual}")


def main() -> None:
    """Run the deterministic processing demonstration."""

    with TemporaryDirectory() as temporary_directory:
        working_directory = Path(temporary_directory)
        source_path = working_directory / "products.csv"
        # Work on a copy because later runs append to the same source history.
        copyfile(INITIAL_SOURCE, source_path)

        connection = duckdb.connect(str(working_directory / "demo.duckdb"))
        try:
            initial_result = run_products(connection, source_path)
            initial_checksum = business_state_checksum(connection)
            # Full rows expose accidental changes to operational metadata on replay.
            products_after_initial = connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall()
            print_run("Run 1: initial load", initial_result, initial_checksum)
            expect_counts(initial_result, (5, 5, 5, 0, 0))

            replay_result = run_products(connection, source_path)
            replay_checksum = business_state_checksum(connection)
            products_after_replay = connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall()
            print_run("Run 2: exact replay", replay_result, replay_checksum)
            expect_counts(replay_result, (5, 5, 0, 0, 5))

            if replay_checksum != initial_checksum:
                raise RuntimeError("replay changed the business state checksum")
            if products_after_replay != products_after_initial:
                raise RuntimeError("replay changed target rows or load metadata")

            # Each run reads all accumulated history, not only the new fragment.
            append_fixture(
                source_path,
                CHANGES_DIRECTORY / "run_03_updates.csv",
            )
            update_result = run_products(connection, source_path)
            update_checksum = business_state_checksum(connection)
            print_run("Run 3: legitimate updates", update_result, update_checksum)
            expect_counts(update_result, (7, 5, 0, 2, 3))

            append_fixture(
                source_path,
                CHANGES_DIRECTORY / "run_04_mixed.csv",
            )
            mixed_result = run_products(connection, source_path)
            mixed_checksum = business_state_checksum(connection)
            print_run("Run 4: mixed changes", mixed_result, mixed_checksum)
            # The stale row and duplicate lose during deduplication, not loading.
            expect_counts(mixed_result, (12, 7, 2, 1, 4))

            products_after_mixed_run = connection.execute(
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
                ORDER BY product_id
                """
            ).fetchall()
            # This expected table proves content independently of the checksum.
            expected_products = [
                (
                    "P001",
                    datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
                    2,
                    "Cordless drill with case",
                    "Tools",
                    Decimal("99.99"),
                    True,
                ),
                (
                    "P002",
                    datetime(2026, 1, 1, 11, 1, tzinfo=UTC),
                    2,
                    "Circular saw kit",
                    "Tools",
                    Decimal("139.99"),
                    True,
                ),
                (
                    "P003",
                    datetime(2026, 1, 1, 12, 2, tzinfo=UTC),
                    2,
                    "Desk lamp with USB",
                    "Home",
                    Decimal("42.00"),
                    True,
                ),
                (
                    "P004",
                    datetime(2026, 1, 1, 10, 3, tzinfo=UTC),
                    1,
                    "Travel mug",
                    "Kitchen",
                    Decimal("18.00"),
                    True,
                ),
                (
                    "P005",
                    datetime(2026, 1, 1, 10, 4, tzinfo=UTC),
                    1,
                    "Notebook",
                    "Stationery",
                    Decimal("6.25"),
                    True,
                ),
                (
                    "P006",
                    datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
                    1,
                    "Stainless bottle",
                    "Outdoors",
                    Decimal("24.00"),
                    True,
                ),
                (
                    "P007",
                    datetime(2026, 1, 1, 12, 1, tzinfo=UTC),
                    1,
                    "Mechanical pencil",
                    "Stationery",
                    Decimal("8.75"),
                    True,
                ),
            ]
            if products_after_mixed_run != expected_products:
                raise RuntimeError("mixed run did not produce the expected products")

            products_before_failure = connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall()
            state_before_failure = get_pipeline_state(connection, PIPELINE_NAME)
            checksum_before_failure = business_state_checksum(connection)
            successful_runs_before_failure = connection.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE status = 'SUCCESS'"
            ).fetchone()[0]
            append_fixture(
                source_path,
                CHANGES_DIRECTORY / "run_05_retry.csv",
            )

            try:
                run_products(
                    connection,
                    source_path,
                    failure_point=FailurePoint.AFTER_TARGET_WRITES,
                )
            except InjectedFailure as error:
                print("Run 5: injected failure")
                print(f"  status=FAILED error={type(error).__name__}")
            else:
                raise RuntimeError("injected failure did not stop the run")

            # Target changes, state and success metadata must roll back together.
            if connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall() != products_before_failure:
                raise RuntimeError("failed run changed the target")
            if get_pipeline_state(connection, PIPELINE_NAME) != state_before_failure:
                raise RuntimeError("failed run changed the watermark state")
            if business_state_checksum(connection) != checksum_before_failure:
                raise RuntimeError("failed run changed the business state")
            successful_runs_after_failure = connection.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE status = 'SUCCESS'"
            ).fetchone()[0]
            if successful_runs_after_failure != successful_runs_before_failure:
                raise RuntimeError("failed run left success metadata behind")
            failed_run = connection.execute(
                """
                SELECT status, resulting_watermark, error_type
                FROM pipeline_runs
                WHERE status = 'FAILED'
                """
            ).fetchone()
            if failed_run != ("FAILED", None, "InjectedFailure"):
                raise RuntimeError("failed run metadata was not recorded correctly")
            print("  target_unchanged=true watermark_unchanged=true")

            # Retry the unchanged history so recovery does not depend on repair work.
            restart_result = run_products(connection, source_path)
            restart_checksum = business_state_checksum(connection)
            print_run("Run 6: safe restart", restart_result, restart_checksum)
            expect_counts(restart_result, (14, 8, 1, 1, 6))

            restarted_products = connection.execute(
                """
                SELECT product_id, name, source_sequence, last_run_id
                FROM products_current
                WHERE product_id IN ('P002', 'P008')
                ORDER BY product_id
                """
            ).fetchall()
            if restarted_products != [
                ("P002", "Circular saw bundle", 3, restart_result.run_id),
                ("P008", "Camping lantern", 1, restart_result.run_id),
            ]:
                raise RuntimeError("restart did not apply the expected changes")

            final_state = get_pipeline_state(connection, PIPELINE_NAME)
            if final_state is None or final_state.watermark != datetime(
                2026,
                1,
                1,
                13,
                1,
                tzinfo=UTC,
            ):
                raise RuntimeError("restart did not advance the watermark")

            run_counts = dict(
                connection.execute(
                    "SELECT status, COUNT(*) FROM pipeline_runs GROUP BY status"
                ).fetchall()
            )
            if run_counts != {"SUCCESS": 5, "FAILED": 1}:
                raise RuntimeError("demo run history is incomplete")
        finally:
            connection.close()


if __name__ == "__main__":
    main()
