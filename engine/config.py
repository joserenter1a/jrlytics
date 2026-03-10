from dataclasses import dataclass


@dataclass
class EngineConfig:
    """Configuration for the AnalyticsEngine.

    Attributes:
        target_partitions: Number of partitions used for parallel query
            execution inside DataFusion.
        batch_size: Number of rows per record batch when streaming results.
        collect_metrics: Whether to record execution timing and row counts.
        verbose_plans: Whether to print query plans to stdout during execution.
    """

    target_partitions: int = 4
    batch_size: int = 8192
    collect_metrics: bool = True
    verbose_plans: bool = False
