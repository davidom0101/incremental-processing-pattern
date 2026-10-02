import csv
import json
from pathlib import Path

import duckdb
from pytest import CaptureFixture

from incremental_pipeline.cli import main

SOURCE_FIELDS = [
    "product_id",
    "updated_at",
    "source_sequence",
    "name",
    "category",
    "price",
    "is_active",
]


def write_cli_source(path: Path, product_id: str) -> Path:
    source_path = path / "products.csv"
    with source_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=SOURCE_FIELDS)
        writer.writeheader()
        writer.writerow(
            {
                "product_id": product_id,
                "updated_at": "2026-01-01T10:00:00Z",
                "source_sequence": "1",
                "name": "Cordless drill",
                "category": "Tools",
                "price": "89.99",
                "is_active": "true",
            }
        )
    return source_path


def test_cli_runs_incremental_load_and_prints_result(
    tmp_path: Path,
    capsys: CaptureFixture[str],
) -> None:
    source_path = write_cli_source(tmp_path, "P001")
    database_path = tmp_path / "pipeline.duckdb"

    exit_code = main(
        [
            "--source",
            str(source_path),
            "--database",
            str(database_path),
            "--initial-start-at",
            "2026-01-01T00:00:00Z",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "SUCCESS"
    assert output["resulting_watermark"] == "2026-01-01T10:00:00Z"
    assert (
        output["rows_received"],
        output["rows_after_deduplication"],
        output["rows_inserted"],
        output["rows_updated"],
        output["rows_ignored"],
    ) == (1, 1, 1, 0, 0)
    assert len(output["business_state_checksum"]) == 64

    connection = duckdb.connect(str(database_path), read_only=True)
    assert connection.execute(
        "SELECT product_id, name FROM products_current"
    ).fetchall() == [("P001", "Cordless drill")]
    connection.close()


def test_cli_returns_failure_and_persists_failed_run(
    tmp_path: Path,
    capsys: CaptureFixture[str],
) -> None:
    source_path = write_cli_source(tmp_path, "")
    database_path = tmp_path / "pipeline.duckdb"

    exit_code = main(
        [
            "--source",
            str(source_path),
            "--database",
            str(database_path),
            "--initial-start-at",
            "2026-01-01T00:00:00Z",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "SourceDataError" in captured.err
    assert "product_id must not be blank" in captured.err

    connection = duckdb.connect(str(database_path), read_only=True)
    assert connection.execute(
        "SELECT status, error_type FROM pipeline_runs"
    ).fetchall() == [("FAILED", "SourceDataError")]
    assert connection.execute("SELECT COUNT(*) FROM products_current").fetchone() == (
        0,
    )
    connection.close()
