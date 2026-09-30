import duckdb

# Keep cheap invariants in DuckDB so direct writes cannot bypass them.
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS products_current (
        product_id VARCHAR PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL,
        source_sequence BIGINT NOT NULL CHECK (source_sequence >= 0),
        name VARCHAR NOT NULL,
        category VARCHAR NOT NULL,
        price DECIMAL(18, 2) NOT NULL CHECK (price >= 0),
        is_active BOOLEAN NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL,
        last_run_id VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_state (
        pipeline_name VARCHAR PRIMARY KEY,
        watermark TIMESTAMPTZ NOT NULL,
        last_successful_run_id VARCHAR NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        status VARCHAR NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
        previous_watermark TIMESTAMPTZ,
        resulting_watermark TIMESTAMPTZ,
        rows_received BIGINT NOT NULL CHECK (rows_received >= 0),
        rows_after_deduplication BIGINT NOT NULL
            CHECK (rows_after_deduplication >= 0),
        rows_inserted BIGINT NOT NULL CHECK (rows_inserted >= 0),
        rows_updated BIGINT NOT NULL CHECK (rows_updated >= 0),
        rows_ignored BIGINT NOT NULL CHECK (rows_ignored >= 0),
        error_type VARCHAR,
        error_message VARCHAR,
        CHECK (finished_at >= started_at),
        CHECK (rows_after_deduplication <= rows_received),
        CHECK (
            status = 'FAILED'
            OR rows_inserted + rows_updated + rows_ignored
                = rows_after_deduplication
        ),
        CHECK (status = 'SUCCESS' OR resulting_watermark IS NULL)
    )
    """,
)


def initialize_database(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the pipeline tables when they do not exist."""

    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
