import duckdb

from incremental_pipeline.models import PipelineState


def get_pipeline_state(
    connection: duckdb.DuckDBPyConnection,
    pipeline_name: str,
) -> PipelineState | None:
    row = connection.execute(
        """
        SELECT pipeline_name, watermark, last_successful_run_id, updated_at
        FROM pipeline_state
        WHERE pipeline_name = ?
        """,
        [pipeline_name],
    ).fetchone()

    if row is None:
        return None

    return PipelineState(
        pipeline_name=row[0],
        watermark=row[1],
        last_successful_run_id=row[2],
        updated_at=row[3],
    )


def save_pipeline_state(
    connection: duckdb.DuckDBPyConnection,
    state: PipelineState,
) -> None:
    """Upsert state without committing the caller's transaction."""

    connection.execute(
        """
        INSERT INTO pipeline_state (
            pipeline_name,
            watermark,
            last_successful_run_id,
            updated_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT (pipeline_name) DO UPDATE SET
            watermark = EXCLUDED.watermark,
            last_successful_run_id = EXCLUDED.last_successful_run_id,
            updated_at = EXCLUDED.updated_at
        """,
        [
            state.pipeline_name,
            state.watermark,
            state.last_successful_run_id,
            state.updated_at,
        ],
    )
