from datetime import UTC, datetime, timedelta
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import duckdb

from incremental_pipeline.checksum import business_state_checksum
from incremental_pipeline.models import RunResult
from incremental_pipeline.pipeline import run_incremental_load

PIPELINE_NAME = "products"
INITIAL_START_AT = datetime(2026, 1, 1, tzinfo=UTC)
OVERLAP = timedelta(hours=24)
INITIAL_SOURCE = Path(__file__).parent / "source" / "products_initial.csv"


def run_products(
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
) -> RunResult:
    return run_incremental_load(
        connection=connection,
        source_path=source_path,
        pipeline_name=PIPELINE_NAME,
        initial_start_at=INITIAL_START_AT,
        overlap=OVERLAP,
    )


def print_run(label: str, result: RunResult, checksum: str) -> None:
    print(label)
    print(
        "  received={0.rows_received} deduplicated={0.rows_after_deduplication} "
        "inserted={0.rows_inserted} updated={0.rows_updated} "
        "ignored={0.rows_ignored}".format(result)
    )
    print(f"  watermark={result.resulting_watermark.isoformat()}")
    print(f"  checksum={checksum}")


def main() -> None:
    with TemporaryDirectory() as temporary_directory:
        working_directory = Path(temporary_directory)
        source_path = working_directory / "products.csv"
        copyfile(INITIAL_SOURCE, source_path)

        connection = duckdb.connect(str(working_directory / "demo.duckdb"))
        try:
            initial_result = run_products(connection, source_path)
            initial_checksum = business_state_checksum(connection)
            products_after_initial = connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall()
            print_run("Run 1 - initial load", initial_result, initial_checksum)

            replay_result = run_products(connection, source_path)
            replay_checksum = business_state_checksum(connection)
            products_after_replay = connection.execute(
                "SELECT * FROM products_current ORDER BY product_id"
            ).fetchall()
            print_run("Run 2 - exact replay", replay_result, replay_checksum)

            if replay_checksum != initial_checksum:
                raise RuntimeError("replay changed the business-state checksum")
            if products_after_replay != products_after_initial:
                raise RuntimeError("replay changed target rows or load metadata")
        finally:
            connection.close()


if __name__ == "__main__":
    main()
