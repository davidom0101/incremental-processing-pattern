from datetime import datetime, timedelta

from incremental_pipeline.models import PipelineState
from incremental_pipeline.timestamps import normalize_to_utc


def calculate_extraction_start(
    state: PipelineState | None,
    initial_start_at: datetime,
    overlap: timedelta,
) -> datetime:
    """Calculate the inclusive source extraction boundary."""

    if overlap < timedelta(0):
        raise ValueError("overlap must be non-negative")

    if state is None:
        return normalize_to_utc(initial_start_at, "initial_start_at")

    # Read the overlap again so bounded late arrivals are still discovered.
    return state.watermark - overlap
