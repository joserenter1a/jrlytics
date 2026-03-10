import pandas
import pyarrow

from .metrics import QueryMetrics


class QueryResult:
    """Wraps a query's Arrow result table together with its execution metrics.

    Attributes:
        table: The raw PyArrow Table containing result rows.
        metrics: Timing and row-count information for the query.
    """

    def __init__(self, table: pyarrow.Table, metrics: QueryMetrics) -> None:
        """Initialise a QueryResult.

        Args:
            table: PyArrow Table containing the query output.
            metrics: Execution metrics captured during the query run.
        """
        self._table = table
        self._metrics = metrics

    @property
    def table(self) -> pyarrow.Table:
        """The query output as a PyArrow Table."""
        return self._table

    @property
    def metrics(self) -> QueryMetrics:
        """Execution metrics for this query."""
        return self._metrics

    def to_pandas(self) -> pandas.DataFrame:
        """Convert the result table to a Pandas DataFrame.

        Returns:
            A Pandas DataFrame containing all result rows and columns.
        """
        return self._table.to_pandas()

    def __repr__(self) -> str:
        return (
            f"QueryResult(rows={self._metrics.row_count}, "
            f"time_ms={self._metrics.execution_time_ms:.2f})"
        )
