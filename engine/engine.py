import pathlib
import re
import time
from typing import Any

import datafusion
import pyarrow
import pyarrow.dataset
import pyarrow.parquet

from .catalog import CatalogManager, ColumnStat
from .config import EngineConfig
from .metrics import QueryMetrics
from .result import QueryResult


class AnalyticsEngine:
    """Columnar query engine backed by Apache DataFusion.

    The engine manages a DataFusion :class:`~datafusion.SessionContext`, uses a
    :class:`~engine.catalog.CatalogManager` as its metadata store, and performs
    segment-level partition pruning before handing queries to DataFusion.

    On each call to :meth:`execute_sql` the DataFusion session is rebuilt from
    scratch so that only the segments relevant to the query are registered —
    either all segments (no filter) or the subset whose min/max statistics are
    compatible with the WHERE-clause predicate.

    Attributes:
        data_dir: Root directory for database files and parquet segments.
        catalog: Metadata store for tables, columns, segments, and statistics.
        config: Engine tuning parameters.
        context: Active DataFusion ``SessionContext``.
    """

    def __init__(
        self,
        data_dir: pathlib.Path,
        config: EngineConfig | None = None,
    ) -> None:
        """Initialise the engine, creating the data directory if needed.

        Args:
            data_dir: Directory under which ``metadata.db`` and the
                ``tables/`` segment tree will be stored.
            config: Optional engine configuration.  Defaults to
                :class:`~engine.config.EngineConfig` with all defaults.
        """
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True)
        self.catalog = CatalogManager(self.data_dir / "metadata.db")
        self.config = config or EngineConfig()
        self._create_session()
        self._bootstrap_tables()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _create_session(self) -> None:
        """Replace the current DataFusion session with a fresh one.

        A new :class:`~datafusion.SessionContext` is constructed using the
        partition and batch-size settings from :attr:`config`.  Any previously
        registered tables are discarded.
        """
        session_config = (
            datafusion.SessionConfig()
            .with_target_partitions(self.config.target_partitions)
            .with_batch_size(self.config.batch_size)
        )
        self.context = datafusion.SessionContext(session_config)

    def _registered_tables(self) -> set[str]:
        """Return the set of table names currently registered in the session.

        Returns:
            A set of table name strings from the DataFusion catalog.
        """
        return self.context.catalog("datafusion").schema("public").names()

    def _bootstrap_tables(self) -> None:
        """Register every known table from the catalog into the DataFusion session.

        Each table is registered by pointing DataFusion at the table's managed
        directory so that all segment files inside it are visible.  An existing
        registration is deregistered first to prevent duplicate-table errors.
        """
        for table_name in self.catalog.get_tables():
            table_dir = str(self.data_dir / "tables" / table_name)
            if table_name in self._registered_tables():
                self.context.deregister_table(table_name)
            self.context.register_parquet(table_name, table_dir)

    # ------------------------------------------------------------------
    # SQL parsing helpers
    # ------------------------------------------------------------------

    def _extract_filters(self, sql: str) -> list[tuple[str, str, str]]:
        """Parse all simple ``column OP value`` predicates from the WHERE clause.

        Handles multiple conditions joined by ``AND``.  Recognises operators
        ``>=``, ``<=``, and ``=``.  Stops parsing at ``ORDER BY``, ``GROUP BY``,
        ``HAVING``, ``LIMIT``, ``UNION``, a semicolon, or end of string.

        Args:
            sql: Raw SQL query string.

        Returns:
            A list of ``(column, operator, value)`` tuples, one per recognised
            predicate.  Returns an empty list when no WHERE clause is present
            or no predicates are parseable.
        """
        where_match = re.search(
            r"WHERE\s+(.+?)(?:\s+(?:ORDER|GROUP|HAVING|LIMIT|UNION)\b|;|$)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not where_match:
            return []
        where_clause = where_match.group(1).strip()
        conditions = re.split(r"\bAND\b", where_clause, flags=re.IGNORECASE)
        filters: list[tuple[str, str, str]] = []
        for cond in conditions:
            m = re.search(r"(\w+)\s*(>=|<=|=)\s*'?([\w.]+)'?", cond.strip())
            if m:
                filters.append((m.group(1), m.group(2), m.group(3)))
        return filters

    def _extract_table_name(self, sql: str) -> str:
        """Extract the primary table name from the FROM clause of a SQL string.

        Args:
            sql: Raw SQL query string.

        Returns:
            The table name immediately following the ``FROM`` keyword.

        Raises:
            ValueError: If no ``FROM`` clause can be found in *sql*.
        """
        match = re.search(r"FROM\s+(?:\w+\.)?(\w+)", sql, re.IGNORECASE)
        if not match:
            raise ValueError("Could not determine table name from SQL")
        return match.group(1)

    def _extract_table_names(self, sql: str) -> list[str]:
        """Extract all table names referenced in the FROM and JOIN clauses.

        Performs two regex passes over *sql*: one for the ``FROM`` clause and
        one for all JOIN variants (``INNER JOIN``, ``LEFT JOIN``, ``RIGHT JOIN``,
        ``FULL JOIN``, ``CROSS JOIN``, and their ``OUTER`` equivalents).
        Schema-qualified names such as ``schema.table`` are handled; only the
        unqualified table name is returned.  Duplicates are removed while
        preserving order — the ``FROM`` table is always first.

        This method does not parse subquery aliases.  A subquery starting with
        ``(SELECT`` will not produce a spurious table name because the regex
        requires the name to follow ``FROM`` or ``JOIN`` as a bare word token.
        If a subquery's ``FROM`` clause is matched, the extracted name will not
        correspond to any catalog table and will be silently ignored during
        segment registration.

        Args:
            sql: Raw SQL SELECT statement, potentially containing zero or more
                JOIN clauses.

        Returns:
            An ordered list of unique table name strings.  The primary
            ``FROM`` table appears first, followed by joined tables in the
            order they appear in *sql*.  Returns an empty list if no
            ``FROM`` clause is present.
        """
        from_matches = re.findall(r"FROM\s+(?:\w+\.)?(\w+)", sql, re.IGNORECASE)
        join_matches = re.findall(r"JOIN\s+(?:\w+\.)?(\w+)", sql, re.IGNORECASE)
        seen: set[str] = set()
        result: list[str] = []
        for name in from_matches + join_matches:
            key = name.lower()
            if key not in seen:
                seen.add(key)
                result.append(name)
        return result

    # ------------------------------------------------------------------
    # Segment pruning
    # ------------------------------------------------------------------

    def _prune_segments(
        self,
        table_name: str,
        filters: list[tuple[str, str, str]],
    ) -> list[str]:
        """Return segment file paths whose statistics are compatible with all filters.

        For each filter ``(column, operator, value)`` a segment is *excluded* if
        its ``[min, max]`` range for that column provably cannot satisfy the
        predicate:

        - ``>=``: prune if ``segment_max < value``
        - ``<=``: prune if ``segment_min > value``
        - ``=``:  prune if ``value < segment_min or value > segment_max``

        When multiple filters are supplied (AND semantics) the surviving sets
        are intersected — a segment must pass every filter to be included.

        Segments without statistics for a filtered column are excluded from
        that filter's candidate set (they were created via
        :meth:`create_table_from_parquet` which does not store stats).

        Args:
            table_name: Logical table name to look up in the catalog.
            filters: List of ``(column, operator, value)`` tuples.  All
                conditions are combined with AND.

        Returns:
            List of absolute file-path strings for surviving segments.
        """
        all_paths = self.catalog.get_segments(table_name)
        stats_rows = self.catalog.get_segment_stats(table_name)

        # Build {file_path: {column: (min_val, max_val)}} from stored statistics.
        segment_stats: dict[str, dict[str, tuple[str, str]]] = {}
        for _sid, file_path, col_name, min_val, max_val in stats_rows:
            segment_stats.setdefault(file_path, {})[col_name] = (min_val, max_val)

        surviving: list[str] = []
        for path in all_paths:
            include = True
            for column, operator, value in filters:
                col_stats = segment_stats.get(path, {})
                if column not in col_stats:
                    # No stats for this column — cannot prune, keep the segment.
                    continue
                min_val, max_val = col_stats[column]
                try:
                    value_cast: float | str = float(value)
                    min_cast: float | str = float(min_val)
                    max_cast: float | str = float(max_val)
                except (ValueError, TypeError):
                    value_cast = value
                    min_cast = min_val
                    max_cast = max_val

                if operator == ">=":
                    if max_cast < value_cast:
                        include = False
                elif operator == "<=":
                    if min_cast > value_cast:
                        include = False
                elif operator == "=":
                    if value_cast < min_cast or value_cast > max_cast:
                        include = False

                if not include:
                    break

            if include:
                surviving.append(path)

        return surviving

    def _register_empty_table(self, table_name: str) -> None:
        """Register a zero-row typed dataset for *table_name* in the session.

        Used when all segments for a JOIN-side table have been pruned away so
        that DataFusion can still resolve the table reference in the SQL
        (a missing registration causes a table-not-found error).  The Arrow
        schema is derived from the first segment file on disk; if the table
        has no segments at all the call is a no-op and DataFusion will raise
        its own table-not-found error at execution time.

        Args:
            table_name: Logical name of the table to register as empty.
        """
        all_paths = self.catalog.get_segments(table_name)
        if not all_paths:
            return
        pf = pyarrow.parquet.ParquetFile(all_paths[0])
        empty = pf.schema_arrow.empty_table()
        dataset = pyarrow.dataset.dataset([empty], schema=pf.schema_arrow)
        if table_name in self._registered_tables():
            self.context.deregister_table(table_name)
        self.context.register_dataset(table_name, dataset)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_parquet(self, name: str, path: pathlib.Path) -> None:
        """Register a parquet file or directory as a table in the current session.

        Args:
            name: Table name to use when querying.
            path: Path to a parquet file or directory of parquet files.
        """
        self.context.register_parquet(name, path)

    def list_tables(self) -> list[str]:
        """Return the names of all tables registered in the current session.

        Returns:
            A list of table name strings.
        """
        return list(self._registered_tables())

    def execute_sql(self, sql: str) -> QueryResult:
        """Execute a SQL query with automatic segment pruning across all tables.

        Rebuilds the DataFusion session on every call, then registers the
        pruned segment set for every table referenced in the ``FROM`` and
        ``JOIN`` clauses.  Segment pruning is applied per-table using the
        WHERE-clause predicates extracted from *sql*; segments whose min/max
        statistics cannot satisfy any predicate are excluded.  When a segment
        has no statistics for a filtered column it is retained conservatively.

        For JOIN queries, all participating tables are registered before
        execution.  If a JOIN-side table's segments are pruned to empty, a
        zero-row typed dataset is registered instead so that DataFusion can
        still resolve the table reference (the JOIN will naturally return no
        rows).  The early-exit empty-result path is only triggered when the
        primary ``FROM`` table has no surviving segments.

        Queries with no WHERE clause register all segments for every
        referenced table.

        Args:
            sql: A valid SQL SELECT statement, optionally containing one or
                more JOIN clauses (``INNER``, ``LEFT``, ``RIGHT``, ``FULL``,
                ``CROSS``, and ``OUTER`` variants are all supported).

        Returns:
            A :class:`~engine.result.QueryResult` containing the result table
            and execution metrics.

        Raises:
            ValueError: If *sql* contains no ``FROM`` clause.
        """
        table_names = self._extract_table_names(sql)
        if not table_names:
            raise ValueError("Could not determine table name from SQL")

        filters = self._extract_filters(sql)
        primary_table = table_names[0]

        # Compute per-table surviving segment lists.
        table_segments: dict[str, list[str]] = {}
        for table_name in table_names:
            if not self.catalog.get_tables() or table_name not in self.catalog.get_tables():
                # Not a catalogued table (e.g. subquery alias) — skip.
                continue
            if filters:
                table_segments[table_name] = self._prune_segments(table_name, filters)
            else:
                table_segments[table_name] = self.catalog.get_segments(table_name)

        # Early exit only when the primary table yields no segments.
        if not table_segments.get(primary_table):
            print("No matching segments after pruning")
            return QueryResult(pyarrow.table({}), QueryMetrics(0, 0, 0))

        self._create_session()

        for table_name, paths in table_segments.items():
            if table_name in self._registered_tables():
                self.context.deregister_table(table_name)

            if paths:
                dataset = pyarrow.dataset.dataset(paths, format="parquet")
                self.context.register_dataset(table_name, dataset)
            else:
                # JOIN-side table pruned to empty: register a zero-row typed
                # dataset so DataFusion can resolve the table reference.
                self._register_empty_table(table_name)

        start = time.perf_counter()
        df = self.context.sql(sql)
        batches = df.collect()
        execution_time_ms = (time.perf_counter() - start) * 1_000

        if batches:
            table = pyarrow.Table.from_batches(batches)
            row_count = table.num_rows
            batch_count = len(batches)
        else:
            table = pyarrow.table({})
            row_count = 0
            batch_count = 0

        metrics = QueryMetrics(
            execution_time_ms=execution_time_ms,
            row_count=row_count,
            batch_count=batch_count,
        )
        return QueryResult(table, metrics)

    def explain_sql(self, sql: str) -> dict[str, str]:
        """Return the logical, optimised, and physical query plans for *sql*.

        Args:
            sql: A valid SQL SELECT statement.

        Returns:
            A dictionary with three keys:

            - ``"logical"``: unoptimised logical plan.
            - ``"optimized"``: optimised logical plan.
            - ``"physical"``: physical execution plan.
        """
        self._create_session()
        self._bootstrap_tables()
        df = self.context.sql(sql)
        return {
            "logical": str(df.logical_plan()),
            "optimized": str(df.optimized_logical_plan()),
            "physical": str(df.execution_plan()),
        }

    def reset_session(self) -> None:
        """Discard the current DataFusion session and create a new empty one."""
        self._create_session()

    def create_table_from_parquet(
        self,
        table_name: str,
        file_path: pathlib.Path,
    ) -> None:
        """Create a new table entry in the catalog from a parquet file.

        Reads the Arrow schema from *file_path*, registers the table and its
        columns in the catalog, adds the file as the first segment, and
        registers the parquet file with the current DataFusion session.

        Args:
            table_name: Logical name for the new table.
            file_path: Path to an existing parquet file that becomes the
                table's first segment.
        """
        parquet_file = pyarrow.parquet.ParquetFile(file_path)
        schema = parquet_file.schema_arrow

        schema_dict: dict[str, dict[str, Any]] = {
            field.name: {"type": str(field.type), "nullable": field.nullable}
            for field in schema
        }

        self.catalog.create_table(table_name, schema_dict)
        row_count = parquet_file.metadata.num_rows
        self.catalog.add_segment(table_name, file_path, row_count)
        self.context.register_parquet(table_name, file_path)

    def append_segment(
        self,
        table_name: str,
        file_path: pathlib.Path,
        stats: list[ColumnStat],
    ) -> None:
        """Append a new segment to an existing table.

        Validates that the incoming parquet file's schema is compatible with
        the table's registered schema, then records the segment and its
        column statistics in the catalog.

        Args:
            table_name: Logical name of the table to append to.
            file_path: Path to the new parquet segment file.
            stats: Column statistics computed for this segment.  Each entry
                must contain ``"column"`` (str), ``"min"`` (Any), and
                ``"max"`` (Any).

        Raises:
            ValueError: If *file_path*'s schema is incompatible with the
                existing table schema.
        """
        parquet_file = pyarrow.parquet.ParquetFile(file_path)
        arrow_schema = parquet_file.schema_arrow
        incoming_schema: list[dict[str, Any]] = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow_schema
        ]

        self._validate_schema(table_name, incoming_schema)

        row_count = parquet_file.metadata.num_rows
        segment_id = self.catalog.add_segment(table_name, file_path, row_count)
        self.catalog.add_segment_stats(segment_id, stats)

    def _validate_schema(
        self,
        table_name: str,
        incoming_schema: list[dict[str, Any]],
    ) -> None:
        """Assert that *incoming_schema* is compatible with the stored schema.

        Args:
            table_name: Table whose catalog schema is used as the reference.
            incoming_schema: Schema derived from the incoming parquet file.
                Each entry must have ``"name"``, ``"type"``, and
                ``"nullable"`` keys.

        Raises:
            ValueError: If column counts differ, column names or types do not
                match, or a non-nullable column would receive nullable data.
        """
        existing_schema = self.catalog.get_schema(table_name)

        if len(existing_schema) != len(incoming_schema):
            raise ValueError("Schema Mismatch: different column count")

        for existing, incoming in zip(existing_schema, incoming_schema):
            if existing["name"] != incoming["name"]:
                raise ValueError(
                    f"Column Mismatch: {incoming['name']} != {existing['name']}"
                )
            if existing["type"] != incoming["type"]:
                raise ValueError(
                    f"Type mismatch for column '{existing['name']}': "
                    f"{incoming['type']} != {existing['type']}"
                )
            if not existing["nullable"] and incoming["nullable"]:
                raise ValueError(
                    f"Nullability violation on column '{existing['name']}'"
                )
