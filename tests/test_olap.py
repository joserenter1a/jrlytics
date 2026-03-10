"""
Rigorous pytest suite for the JLytics OLAP engine.

Covers:
  - Append-only segments
  - Schema validation
  - Per-segment stats
  - Query-time pruning
  - Clean session rebuild
  - Deterministic metadata-driven execution
"""

import pathlib

import pyarrow
import pyarrow.parquet
import pytest

from engine import AnalyticsEngine, EngineConfig
from engine.catalog import CatalogManager
from engine.ingestion import IngestionManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_parquet(path: pathlib.Path, data: dict) -> pathlib.Path:
    """Write a dict-of-lists as a parquet file and return the path."""
    table = pyarrow.Table.from_pydict(data)
    pyarrow.parquet.write_table(table, path)
    return path


def make_engine(tmp_path: pathlib.Path) -> AnalyticsEngine:
    return AnalyticsEngine(
        data_dir=tmp_path / "db",
        config=EngineConfig(target_partitions=2, batch_size=1024),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine(tmp_path: pathlib.Path):
    return make_engine(tmp_path)


@pytest.fixture
def ingestor(engine: AnalyticsEngine):
    return IngestionManager(engine)


@pytest.fixture
def catalog(tmp_path):
    return CatalogManager(tmp_path / "meta.db")


# ---------------------------------------------------------------------------
# CatalogManager unit tests
# ---------------------------------------------------------------------------


class TestCatalogManager:
    def test_initialize_creates_empty_tables(self, catalog: CatalogManager):
        assert catalog.get_tables() == []

    def test_create_table_and_retrieve(self, catalog: CatalogManager):
        schema = {
            "id": {"type": "int64", "nullable": False},
            "name": {"type": "string", "nullable": True},
        }
        catalog.create_table("users", schema)
        assert "users" in catalog.get_tables()

    def test_create_multiple_tables(self, catalog: CatalogManager):
        for name in ("orders", "products", "customers"):
            catalog.create_table(name, {"id": {"type": "int64", "nullable": False}})
        tables = catalog.get_tables()
        assert set(tables) == {"orders", "products", "customers"}

    def test_get_schema_returns_correct_columns(self, catalog: CatalogManager):
        schema = {
            "id": {"type": "int64", "nullable": False},
            "value": {"type": "double", "nullable": True},
        }
        catalog.create_table("t", schema)
        result = catalog.get_schema("t")
        assert len(result) == 2
        names = [c["name"] for c in result]
        assert "id" in names
        assert "value" in names

    def test_add_segment_and_retrieve(
        self, catalog: CatalogManager, tmp_path: pathlib.Path
    ):
        catalog.create_table("t", {"id": {"type": "int64", "nullable": False}})
        seg = tmp_path / "seg1.parquet"
        seg.touch()
        catalog.add_segment("t", seg, row_count=100)
        segments = catalog.get_segments("t")
        assert len(segments) == 1
        assert str(seg) in segments[0]

    def test_add_multiple_segments(
        self, catalog: CatalogManager, tmp_path: pathlib.Path
    ):
        catalog.create_table("t", {"id": {"type": "int64", "nullable": False}})
        for i in range(3):
            seg = tmp_path / f"seg{i}.parquet"
            seg.touch()
            catalog.add_segment("t", seg, row_count=10)
        assert len(catalog.get_segments("t")) == 3

    def test_add_segment_stats_and_retrieve(
        self, catalog: CatalogManager, tmp_path: pathlib.Path
    ):
        catalog.create_table("t", {"id": {"type": "int64", "nullable": False}})
        seg = tmp_path / "seg.parquet"
        seg.touch()
        segment_id = catalog.add_segment("t", seg, row_count=50)
        stats = [{"column": "id", "min": 1, "max": 50}]
        catalog.add_segment_stats(segment_id, stats)

        rows = catalog.get_segment_stats("t")
        assert len(rows) == 1
        _seg_id, _path, col, min_val, max_val = rows[0]
        assert col == "id"
        assert min_val == "1"
        assert max_val == "50"

    def test_reinitialize_is_idempotent(self, tmp_path: pathlib.Path):
        """Creating a second CatalogManager on the same DB must not raise."""
        db = tmp_path / "meta.db"
        c1 = CatalogManager(db)
        c1.create_table("t", {"id": {"type": "int64", "nullable": False}})
        c2 = CatalogManager(db)  # second init hits IF NOT EXISTS — must not raise
        assert "t" in c2.get_tables()


# ---------------------------------------------------------------------------
# Append-only segments
# ---------------------------------------------------------------------------


class TestAppendOnlySegments:
    def test_first_ingest_creates_segment_file(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src, "t")
        table_dir = engine.data_dir / "tables" / "t"
        segments = list(table_dir.glob("segment*.parquet"))
        assert len(segments) == 1

    def test_second_ingest_creates_new_file(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [4, 5, 6]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")
        segments = list((engine.data_dir / "tables" / "t").glob("segment*.parquet"))
        assert len(segments) == 2

    def test_segment_files_are_distinct(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        for i in range(4):
            src = write_parquet(tmp_path / f"src{i}.parquet", {"id": [i * 10]})
            ingestor.ingest_file(src, "t")
        segments = list((engine.data_dir / "tables" / "t").glob("segment*.parquet"))
        names = [s.name for s in segments]
        assert len(names) == len(set(names)), "Segment filenames must be unique"

    def test_original_source_file_untouched(
        self, ingestor: IngestionManager, tmp_path: pathlib.Path
    ):
        src = write_parquet(tmp_path / "src.parquet", {"id": [1, 2, 3]})
        mtime_before = src.stat().st_mtime
        ingestor.ingest_file(src, "t")
        assert src.stat().st_mtime == mtime_before

    def test_catalog_tracks_all_segments(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        for i in range(3):
            src = write_parquet(tmp_path / f"s{i}.parquet", {"id": [i]})
            ingestor.ingest_file(src, "t")
        assert len(engine.catalog.get_segments("t")) == 3

    def test_all_rows_queryable_after_multiple_ingests(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [4, 5, 6]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")
        result = engine.execute_sql("SELECT COUNT(*) as n FROM t")
        assert result.table.to_pydict()["n"][0] == 6


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestSchemaValidation:
    def _ingest_base(self, ingestor: IngestionManager, tmp_path: pathlib.Path):
        src = write_parquet(
            tmp_path / "base.parquet", {"id": [1, 2], "val": [10.0, 20.0]}
        )
        ingestor.ingest_file(src, "t")

    def test_compatible_append_succeeds(
        self, ingestor: IngestionManager, tmp_path: pathlib.Path
    ):
        self._ingest_base(ingestor, tmp_path)
        src2 = write_parquet(
            tmp_path / "b.parquet", {"id": [3, 4], "val": [30.0, 40.0]}
        )
        ingestor.ingest_file(src2, "t")  # must not raise

    def test_extra_column_raises(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        self._ingest_base(ingestor, tmp_path)
        src2 = write_parquet(
            tmp_path / "b.parquet", {"id": [3], "val": [30.0], "extra": ["x"]}
        )
        with pytest.raises(ValueError, match="column count"):
            ingestor.ingest_file(src2, "t")

    def test_missing_column_raises(
        self, ingestor: IngestionManager, tmp_path: pathlib.Path
    ):
        self._ingest_base(ingestor, tmp_path)
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [3]})
        with pytest.raises(ValueError, match="column count"):
            ingestor.ingest_file(src2, "t")

    def test_wrong_column_name_raises(
        self, ingestor: IngestionManager, tmp_path: pathlib.Path
    ):
        self._ingest_base(ingestor, tmp_path)
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [3], "wrong": [30.0]})
        with pytest.raises(ValueError, match="[Cc]olumn [Mm]ismatch"):
            ingestor.ingest_file(src2, "t")

    def test_type_mismatch_raises(
        self, ingestor: IngestionManager, tmp_path: pathlib.Path
    ):
        self._ingest_base(ingestor, tmp_path)
        # val was float64 — now send int64
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [3], "val": [30]})
        with pytest.raises(ValueError, match="[Tt]ype"):
            ingestor.ingest_file(src2, "t")

    def test_schema_mismatch_does_not_create_segment(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        self._ingest_base(ingestor, tmp_path)
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [3], "extra": ["x"]})
        with pytest.raises(ValueError):
            ingestor.ingest_file(src2, "t")
        # Only the original segment must exist
        assert len(engine.catalog.get_segments("t")) == 1


# ---------------------------------------------------------------------------
# Per-segment stats
# ---------------------------------------------------------------------------


class TestPerSegmentStats:
    def test_stats_stored_for_integer_column(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [100, 200, 300]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")

        stats = engine.catalog.get_segment_stats("t")
        # Second segment gets stats; at least one stat row must exist
        assert len(stats) >= 1
        cols = [r[2] for r in stats]
        assert "id" in cols

    def test_stats_min_max_values_correct(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"score": [1, 2, 3]})
        src2 = write_parquet(tmp_path / "b.parquet", {"score": [100, 200, 300]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")

        stats = engine.catalog.get_segment_stats("t")
        # Find the stat row for the second segment
        stat_row = next(r for r in stats if float(r[4]) == 300.0)
        assert float(stat_row[3]) == 100.0  # min
        assert float(stat_row[4]) == 300.0  # max

    def test_stats_stored_for_float_column(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"v": [1.5, 2.5]})
        src2 = write_parquet(tmp_path / "b.parquet", {"v": [10.5, 20.5]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")

        stats = engine.catalog.get_segment_stats("t")
        assert any(r[2] == "v" for r in stats)

    def test_stats_not_stored_for_string_column(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"name": ["alice", "bob"]})
        src2 = write_parquet(tmp_path / "b.parquet", {"name": ["charlie", "dave"]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")

        stats = engine.catalog.get_segment_stats("t")
        assert all(r[2] != "name" for r in stats), "String columns must not get stats"

    def test_each_segment_gets_independent_stats(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        src2 = write_parquet(tmp_path / "b.parquet", {"id": [10, 20, 30]})
        src3 = write_parquet(tmp_path / "c.parquet", {"id": [100, 200, 300]})
        ingestor.ingest_file(src1, "t")
        ingestor.ingest_file(src2, "t")
        ingestor.ingest_file(src3, "t")

        stats = engine.catalog.get_segment_stats("t")
        # Two appended segments should each have one stat row for "id"
        id_stats = [r for r in stats if r[2] == "id"]
        assert len(id_stats) == 2

        maxes = sorted([float(r[4]) for r in id_stats])
        assert maxes == [30.0, 300.0]


# ---------------------------------------------------------------------------
# Query-time pruning
# ---------------------------------------------------------------------------


class TestQueryTimePruning:
    """
    Segment 1 (no stats — first ingest): id = 1..5
    Segment 2 (stats: min=100, max=105):  id = 100..105
    Segment 3 (stats: min=200, max=205):  id = 200..205
    """

    @pytest.fixture
    def pruning_engine(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "s1.parquet", {"id": list(range(1, 6))})
        src2 = write_parquet(tmp_path / "s2.parquet", {"id": list(range(100, 106))})
        src3 = write_parquet(tmp_path / "s3.parquet", {"id": list(range(200, 206))})
        ingestor.ingest_file(src1, "events")
        ingestor.ingest_file(src2, "events")
        ingestor.ingest_file(src3, "events")
        return engine

    def test_gte_prunes_segment_below_threshold(self, pruning_engine: AnalyticsEngine):
        # segment 2 max=105 < 150 → pruned; segment 3 max=205 >= 150 → kept
        result = pruning_engine.execute_sql("SELECT id FROM events WHERE id >= 150")
        ids = result.table.to_pydict()["id"]
        assert all(i >= 150 for i in ids)
        assert any(i >= 200 for i in ids)

    def test_lte_prunes_segment_above_threshold(self, pruning_engine: AnalyticsEngine):
        # segment 3 min=200 > 110 → pruned; segment 2 min=100 <= 110 → kept
        result = pruning_engine.execute_sql("SELECT id FROM events WHERE id <= 110")
        ids = result.table.to_pydict()["id"]
        assert all(i <= 110 for i in ids)

    def test_eq_prunes_segments_outside_value(self, pruning_engine: AnalyticsEngine):
        # Only segment 3 range [200,205] contains 202
        result = pruning_engine.execute_sql("SELECT id FROM events WHERE id = 202")
        ids = result.table.to_pydict()["id"]
        assert ids == [202]

    def test_no_filter_returns_all_catalogued_segments(
        self, pruning_engine: AnalyticsEngine
    ):
        # No WHERE → all segments registered → all rows from s2 and s3 visible
        # (s1 has no stats so it comes through catalog.get_segments path)
        result = pruning_engine.execute_sql("SELECT COUNT(*) as n FROM events")
        assert result.table.to_pydict()["n"][0] == 17  # 5 + 6 + 6 rows

    def test_pruning_returns_empty_when_no_segments_match(
        self, pruning_engine: AnalyticsEngine
    ):
        # No segment has stats covering id=9999
        result = pruning_engine.execute_sql("SELECT id FROM events WHERE id = 9999")
        assert result.table.num_rows == 0

    def test_prune_segments_method_directly(self, pruning_engine: AnalyticsEngine):
        paths = pruning_engine._prune_segments("events", "id", ">=", "150")
        # Only segment 3 qualifies
        assert len(paths) == 1

    def test_prune_lte_keeps_correct_segments(self, pruning_engine: AnalyticsEngine):
        paths = pruning_engine._prune_segments("events", "id", "<=", "150")
        # segment 2 (max=105 ≤ 150) qualifies; segment 3 (min=200 > 150) is pruned
        assert len(paths) == 1

    def test_prune_eq_excludes_both_non_matching_segments(
        self, pruning_engine: AnalyticsEngine
    ):
        paths = pruning_engine._prune_segments("events", "id", "=", "202")
        assert len(paths) == 1


# ---------------------------------------------------------------------------
# Clean session rebuild
# ---------------------------------------------------------------------------


class TestCleanSessionRebuild:
    def test_execute_sql_creates_fresh_context(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src, "t")

        ctx_before = id(engine.context)
        engine.execute_sql("SELECT * FROM t")
        ctx_after = id(engine.context)
        assert ctx_before != ctx_after, "execute_sql must rebuild the session"

    def test_sequential_queries_all_succeed(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(
            tmp_path / "a.parquet", {"id": [1, 2, 3], "v": [10, 20, 30]}
        )
        ingestor.ingest_file(src, "t")

        r1 = engine.execute_sql("SELECT COUNT(*) as n FROM t")
        r2 = engine.execute_sql("SELECT SUM(v) as s FROM t")
        r3 = engine.execute_sql("SELECT MAX(id) as m FROM t")

        assert r1.table.to_pydict()["n"][0] == 3
        assert r2.table.to_pydict()["s"][0] == 60
        assert r3.table.to_pydict()["m"][0] == 3

    def test_new_segment_visible_after_ingest(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src1 = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src1, "t")
        r1 = engine.execute_sql("SELECT COUNT(*) as n FROM t")

        src2 = write_parquet(tmp_path / "b.parquet", {"id": [4, 5, 6]})
        ingestor.ingest_file(src2, "t")
        r2 = engine.execute_sql("SELECT COUNT(*) as n FROM t")

        assert r1.table.to_pydict()["n"][0] == 3
        assert r2.table.to_pydict()["n"][0] == 6

    def test_reset_session_clears_context(self, engine: AnalyticsEngine):
        ctx_before = id(engine.context)
        engine.reset_session()
        assert id(engine.context) != ctx_before


# ---------------------------------------------------------------------------
# Deterministic metadata-driven execution
# ---------------------------------------------------------------------------


class TestDeterministicMetadataExecution:
    def test_engine_restart_reregisters_tables(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src, "t")

        # Simulate restart — new engine pointing at same db
        engine2 = AnalyticsEngine(
            data_dir=engine.data_dir,
            config=EngineConfig(target_partitions=2, batch_size=1024),
        )
        result = engine2.execute_sql("SELECT COUNT(*) as n FROM t")
        assert result.table.to_pydict()["n"][0] == 3

    def test_engine_restart_sees_all_segments(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        for i in range(3):
            src = write_parquet(
                tmp_path / f"s{i}.parquet", {"id": [i * 10 + j for j in range(5)]}
            )
            ingestor.ingest_file(src, "t")

        engine2 = AnalyticsEngine(
            data_dir=engine.data_dir,
            config=EngineConfig(target_partitions=2, batch_size=1024),
        )
        result = engine2.execute_sql("SELECT COUNT(*) as n FROM t")
        assert result.table.to_pydict()["n"][0] == 15

    def test_catalog_is_single_source_of_truth(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src, "t")

        # Catalog must list the table
        assert "t" in engine.catalog.get_tables()
        # DataFusion session must also have it registered
        assert "t" in engine.list_tables()

    def test_list_tables_reflects_catalog(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        for name in ("alpha", "beta", "gamma"):
            src = write_parquet(tmp_path / f"{name}.parquet", {"id": [1]})
            ingestor.ingest_file(src, name)

        catalog_tables = set(engine.catalog.get_tables())
        session_tables = set(engine.list_tables())
        assert catalog_tables == session_tables

    def test_query_results_consistent_across_engine_instances(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"v": [10, 20, 30, 40, 50]})
        ingestor.ingest_file(src, "t")

        r1 = engine.execute_sql("SELECT SUM(v) as s FROM t")

        engine2 = AnalyticsEngine(data_dir=engine.data_dir, config=EngineConfig())
        r2 = engine2.execute_sql("SELECT SUM(v) as s FROM t")

        assert r1.table.to_pydict()["s"][0] == r2.table.to_pydict()["s"][0]


# ---------------------------------------------------------------------------
# QueryResult / QueryMetrics
# ---------------------------------------------------------------------------


class TestQueryResult:
    def test_metrics_row_count(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": list(range(42))})
        ingestor.ingest_file(src, "t")
        result = engine.execute_sql("SELECT * FROM t")
        assert result.metrics.row_count == 42

    def test_metrics_execution_time_positive(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1]})
        ingestor.ingest_file(src, "t")
        result = engine.execute_sql("SELECT * FROM t")
        assert result.metrics.execution_time_ms > 0

    def test_to_pandas_roundtrip(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        data = {"id": [1, 2, 3], "name": ["a", "b", "c"]}
        src = write_parquet(tmp_path / "a.parquet", data)
        ingestor.ingest_file(src, "t")
        result = engine.execute_sql("SELECT * FROM t ORDER BY id")
        df = result.to_pandas()
        assert list(df["id"]) == [1, 2, 3]
        assert list(df["name"]) == ["a", "b", "c"]

    def test_repr_contains_row_count(
        self,
        ingestor: IngestionManager,
        engine: AnalyticsEngine,
        tmp_path: pathlib.Path,
    ):
        src = write_parquet(tmp_path / "a.parquet", {"id": [1, 2, 3]})
        ingestor.ingest_file(src, "t")
        result = engine.execute_sql("SELECT * FROM t")
        assert "rows=3" in repr(result)
