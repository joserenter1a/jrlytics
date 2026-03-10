from dataclasses import dataclass


@dataclass
class QueryMetrics:
    """Execution statistics captured for a single query.

    Attributes:
        execution_time_ms: Wall-clock time for DataFusion execution in
            milliseconds.
        row_count: Total number of rows in the result set.
        batch_count: Number of Arrow record batches collected.
        logical_plan: Unoptimised logical plan string, if requested.
        optimized_plan: Optimised logical plan string, if requested.
        physical_plan: Physical execution plan string, if requested.
    """

    execution_time_ms: float
    row_count: int
    batch_count: int
    logical_plan: str | None = None
    optimized_plan: str | None = None
    physical_plan: str | None = None
