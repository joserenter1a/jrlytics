import pathlib
import uuid

import pandas
import pyarrow
import pyarrow.compute
import pyarrow.parquet

from .catalog import ColumnStat
from .engine import AnalyticsEngine


class IngestionManager:
    """Converts external data files into managed parquet segments.

    Supported input formats: ``.csv``, ``.xlsx``, ``.xls``, ``.parquet``.

    Each call to :meth:`ingest_file` writes a new segment file under
    ``<data_dir>/tables/<table_name>/segment<uuid>.parquet``, computes
    per-column min/max statistics for numeric and temporal columns, and
    registers the segment in the engine's catalog.

    Attributes:
        engine: The :class:`~engine.engine.AnalyticsEngine` instance that
            owns the catalog and DataFusion session.
    """

    def __init__(self, engine: AnalyticsEngine) -> None:
        """Initialise the ingestion manager.

        Args:
            engine: Engine instance to which ingested segments will be
                registered.
        """
        self.engine = engine

    def ingest_file(self, file_path: pathlib.Path, table_name: str) -> pathlib.Path:
        """Ingest a data file as a new segment of *table_name*.

        The file is read into a Pandas DataFrame, converted to an Arrow Table,
        written as a parquet segment, and registered in the catalog.  If
        *table_name* does not yet exist it is created; otherwise the segment is
        appended with schema validation.

        Args:
            file_path: Path to the source data file.  Must have one of the
                extensions ``.csv``, ``.xlsx``, ``.xls``, or ``.parquet``.
            table_name: Logical name of the destination table.

        Returns:
            Path to the newly written parquet segment file.

        Raises:
            ValueError: If *file_path* has an unsupported extension.
        """
        match file_path.suffix.lower():
            case ".csv":
                df = pandas.read_csv(file_path)
            case ".xlsx" | ".xls":
                df = pandas.read_excel(file_path)
            case ".parquet":
                df = pandas.read_parquet(file_path)
            case _:
                raise ValueError(
                    f"Unsupported file format: '{file_path.suffix}'. "
                    "Supported formats are .csv, .xlsx, .xls, .parquet."
                )
        return self._ingest_dataframe(df, table_name)

    def _ingest_dataframe(
        self,
        df: pandas.DataFrame,
        table_name: str,
    ) -> pathlib.Path:
        """Write a DataFrame as a managed parquet segment and register it.

        Computes min/max statistics for integer, float, date, and timestamp
        columns, then either creates a new table or appends to an existing one.

        Args:
            df: DataFrame containing the data to ingest.
            table_name: Logical name of the destination table.

        Returns:
            Path to the newly written parquet segment file.
        """
        table_dir = self.engine.data_dir / "tables" / table_name
        table_dir.mkdir(parents=True, exist_ok=True)

        segment_path = table_dir / f"segment{uuid.uuid4().hex}.parquet"
        arrow_table = pyarrow.Table.from_pandas(df)
        pyarrow.parquet.write_table(arrow_table, segment_path)

        stats: list[ColumnStat] = []
        for field in arrow_table.schema:
            if (
                pyarrow.types.is_integer(field.type)
                or pyarrow.types.is_floating(field.type)
                or pyarrow.types.is_date(field.type)
                or pyarrow.types.is_timestamp(field.type)
            ):
                col = arrow_table[field.name]
                stats.append({
                    "column": field.name,
                    "min": pyarrow.compute.min(col).as_py(),
                    "max": pyarrow.compute.max(col).as_py(),
                })

        if table_name not in self.engine.catalog.get_tables():
            self.engine.create_table_from_parquet(table_name, segment_path)
        else:
            self.engine.append_segment(table_name, segment_path, stats)

        return segment_path
