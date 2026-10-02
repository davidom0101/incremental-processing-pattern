import argparse
import json
import math
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from incremental_pipeline.checksum import business_state_checksum
from incremental_pipeline.models import RunResult
from incremental_pipeline.pipeline import run_incremental_load
from incremental_pipeline.source import DEFAULT_ALLOWED_FUTURE_SKEW


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser for one product pipeline run."""

    parser = argparse.ArgumentParser(
        description="Run a retry safe incremental product load.",
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Path to accumulated product source history in CSV format.",
    )
    parser.add_argument(
        "--database",
        required=True,
        type=Path,
        help="Path to the DuckDB database used for target data and state.",
    )
    parser.add_argument(
        "--pipeline-name",
        default="products",
        help="State key for this pipeline. Defaults to products.",
    )
    parser.add_argument(
        "--initial-start-at",
        required=True,
        type=_parse_timestamp,
        help="Inclusive UTC starting point used when no state exists.",
    )
    parser.add_argument(
        "--overlap-hours",
        default=24.0,
        type=_nonnegative_number,
        help="Hours to read again before the stored watermark. Defaults to 24.",
    )
    parser.add_argument(
        "--allowed-future-skew-minutes",
        default=DEFAULT_ALLOWED_FUTURE_SKEW.total_seconds() / 60,
        type=_nonnegative_number,
        help="Accepted source clock skew in minutes. Defaults to 5.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its process exit code."""

    args = build_parser().parse_args(argv)
    connection: duckdb.DuckDBPyConnection | None = None

    try:
        connection = duckdb.connect(str(args.database))
        result = run_incremental_load(
            connection=connection,
            source_path=args.source,
            pipeline_name=args.pipeline_name,
            initial_start_at=args.initial_start_at,
            overlap=timedelta(hours=args.overlap_hours),
            allowed_future_skew=timedelta(
                minutes=args.allowed_future_skew_minutes
            ),
        )
        output = _result_output(result, business_state_checksum(connection))
        print(json.dumps(output, indent=2))
        return 0
    except Exception as error:
        # Pipeline failures are audited before control returns to the CLI.
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()


def _parse_timestamp(value: str) -> datetime:
    """Parse a timezone aware command line timestamp in UTC."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "timestamp must use ISO 8601 format"
        ) from error

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _nonnegative_number(value: str) -> float:
    """Parse a nonnegative numeric command line option."""

    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a number") from error

    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("value must be a finite nonnegative number")
    return parsed


def _result_output(result: RunResult, checksum: str) -> dict[str, object]:
    """Build the stable success document printed by the CLI."""

    return {
        "run_id": result.run_id,
        "status": result.status.value,
        "run_started_at": _format_timestamp(result.run_started_at),
        "previous_watermark": _format_timestamp(result.previous_watermark),
        "extraction_start": _format_timestamp(result.extraction_start),
        "resulting_watermark": _format_timestamp(result.resulting_watermark),
        "rows_received": result.rows_received,
        "rows_after_deduplication": result.rows_after_deduplication,
        "rows_inserted": result.rows_inserted,
        "rows_updated": result.rows_updated,
        "rows_ignored": result.rows_ignored,
        "business_state_checksum": checksum,
    }


def _format_timestamp(value: datetime | None) -> str | None:
    """Format an optional timestamp as UTC ISO 8601 text."""

    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
