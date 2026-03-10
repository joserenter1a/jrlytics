import pathlib
import sqlite3
from typing import Any

import utils

THIS_DIRECTORY = pathlib.Path(__file__).parent

# Type alias for a column-statistics entry produced by IngestionManager.
ColumnStat = dict[str, Any]  # keys: "column" (str), "min" (Any), "max" (Any)

# Type alias for a row returned by get_segment_stats.
SegmentStatRow = tuple[
    int, str, str, str, str
]  # (segment_id, file_path, column, min, max)


class CatalogManager:
    """SQLite-backed metadata store for tables, columns, segments, and stats.

    The catalog is the single source of truth for the engine. It tracks every
    table's schema, the parquet segment files that make up each table, and
    per-segment min/max statistics used for query-time partition pruning.

    Attributes:
        db_path: Filesystem path to the SQLite database file.
    """

    def __init__(self, db_path: pathlib.Path) -> None:
        """Initialise the catalog, creating the database schema if needed.

        Args:
            db_path: Path at which the SQLite database file should be created
                or opened.
        """
        self.db_path = db_path
        self._initialize()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def __internal_query(self, query: str) -> str:
        """Load an internal SQL statement from the *catalog* query directory.

        Args:
            query: Filename stem of the ``.sql`` file to load (no extension).

        Returns:
            The SQL statement as a plain string.

        Raises:
            FileNotFoundError: If the requested ``.sql`` file does not exist.
        """
        query_path = THIS_DIRECTORY / "internal_queries" / "catalog"
        return utils.fread(path=query_path / f"{query}.sql")

    def _connect(self) -> sqlite3.Connection:
        """Open and return a new SQLite connection to the catalog database.

        Returns:
            An open :class:`sqlite3.Connection` for :attr:`db_path`.
        """
        return sqlite3.connect(self.db_path)

    def _initialize(self) -> None:
        """Create catalog tables if they do not already exist.

        All ``CREATE TABLE`` statements use ``IF NOT EXISTS``, so calling this
        on an existing database is safe and idempotent.

        Raises:
            Exception: Re-raises any unexpected database error.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("tables"))
            cur.execute(self.__internal_query("columns"))
            cur.execute(self.__internal_query("segments"))
            cur.execute(self.__internal_query("segmentstable"))
            conn.commit()
        except Exception:
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------

    def create_table(self, name: str, schema: dict[str, dict[str, Any]]) -> None:
        """Register a new table and its column definitions in the catalog.

        Args:
            name: Logical table name (must be unique).
            schema: Mapping of column name to column metadata.  Each value
                must contain ``"type"`` (str) and ``"nullable"`` (bool).

                Example::

                    {
                        "id":    {"type": "int64",  "nullable": False},
                        "label": {"type": "string", "nullable": True},
                    }
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("insert_table_names"), (name,))
            table_id = cur.lastrowid
            for col, meta in schema.items():
                cur.execute(
                    self.__internal_query("insert_columns"),
                    (table_id, col, meta["type"], meta["nullable"]),
                )
            conn.commit()
        finally:
            conn.close()

    def add_segment(
        self,
        table_name: str,
        file_path: pathlib.Path,
        row_count: int,
    ) -> int:
        """Append a parquet segment record to the catalog.

        Args:
            table_name: Name of the table this segment belongs to.
            file_path: Absolute path to the parquet segment file on disk.
            row_count: Number of data rows contained in the segment.

        Returns:
            The auto-generated integer primary key of the new segment record.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("select_table_by_name"), (table_name,))
            table_id = cur.fetchone()[0]
            cur.execute(
                self.__internal_query("insert_segments"),
                (table_id, str(file_path), row_count),
            )
            segment_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()
        return segment_id

    def get_tables(self) -> list[str]:
        """Return the names of all registered tables.

        Returns:
            A list of table name strings, in insertion order.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("select_name_from_tables"))
            rows = cur.fetchall()
        finally:
            conn.close()
        return [row[0] for row in rows]

    def get_segments(self, table_name: str) -> list[str]:
        """Return the file paths of all segments belonging to *table_name*.

        Args:
            table_name: Logical name of the table whose segments to retrieve.

        Returns:
            A list of absolute file-path strings, one per segment.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("get_segments"), (table_name,))
            rows = cur.fetchall()
        finally:
            conn.close()
        return [row[0] for row in rows]

    def get_schema(self, table_name: str) -> list[dict[str, Any]]:
        """Return the column definitions for *table_name*.

        Args:
            table_name: Logical name of the table whose schema to retrieve.

        Returns:
            A list of column descriptors, each containing:

            - ``"name"`` (str): column name.
            - ``"type"`` (str): Arrow type string, e.g. ``"int64"``.
            - ``"nullable"`` (bool): whether the column accepts nulls.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(self.__internal_query("get_schema"), (table_name,))
            rows = cur.fetchall()
        finally:
            conn.close()
        return [{"name": r[0], "type": r[1], "nullable": bool(r[2])} for r in rows]

    # ------------------------------------------------------------------
    # Segment statistics
    # ------------------------------------------------------------------

    def add_segment_stats(self, segment_id: int, stats: list[ColumnStat]) -> None:
        """Persist per-column min/max statistics for a segment.

        Args:
            segment_id: Primary key of the segment record these stats belong to.
            stats: List of column statistic entries.  Each entry must contain:

                - ``"column"`` (str): column name.
                - ``"min"`` (Any): minimum observed value.
                - ``"max"`` (Any): maximum observed value.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            for stat in stats:
                cur.execute(
                    """
                    INSERT INTO segment_stats (segment_id, column_name, min_value, max_value)
                    VALUES (?, ?, ?, ?)
                    """,
                    (segment_id, stat["column"], str(stat["min"]), str(stat["max"])),
                )
            conn.commit()
        finally:
            conn.close()

    def get_segment_stats(self, table_name: str) -> list[SegmentStatRow]:
        """Retrieve all segment statistics for every segment of *table_name*.

        Args:
            table_name: Logical name of the table whose stats to retrieve.

        Returns:
            A list of 5-tuples, one per (segment, column) pair:
            ``(segment_id, file_path, column_name, min_value, max_value)``.
            Both *min_value* and *max_value* are stored and returned as strings.
        """
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    segments.id,
                    segments.file_path,
                    segment_stats.column_name,
                    segment_stats.min_value,
                    segment_stats.max_value
                FROM segments
                JOIN tables ON segments.table_id = tables.id
                JOIN segment_stats ON segment_stats.segment_id = segments.id
                WHERE tables.name = ?
                """,
                (table_name,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
        return rows
