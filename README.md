# JLytics

A mini OLAP database engine built in Python. JLytics implements segment-based columnar storage with metadata-driven query optimization on top of Apache DataFusion.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                        main.py                           │
│              (entry point / user scripts)                │
└────────────┬─────────────────────────┬───────────────────┘
             │                         │
             ▼                         ▼
┌────────────────────┐    ┌────────────────────────────┐
│  IngestionManager  │    │      AnalyticsEngine        │
│  ingestion.py      │───▶│      engine.py              │
│                    │    │                             │
│  - CSV/Excel/      │    │  - DataFusion session mgmt  │
│    Parquet input   │    │  - SQL execution            │
│  - Segment writes  │    │  - Segment pruning          │
│  - Stats compute   │    │  - Schema validation        │
└────────┬───────────┘    └────────────┬────────────────┘
         │                             │
         ▼                             ▼
┌────────────────────────────────────────────────────────┐
│                   CatalogManager                        │
│                   catalog.py                            │
│                                                         │
│  SQLite DB: jlytics_db/metadata.db                      │
│  ┌──────────┐  ┌─────────┐  ┌──────────────────────┐  │
│  │  tables  │  │ columns │  │ segments             │  │
│  └──────────┘  └─────────┘  └──────────────────────┘  │
│                              ┌──────────────────────┐  │
│                              │   segment_stats      │  │
│                              └──────────────────────┘  │
└────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────┐
│               jlytics_db/tables/{table}/                │
│         segment<uuid>.parquet  (columnar data)          │
└────────────────────────────────────────────────────────┘
```

---

## Components

### `AnalyticsEngine` (`engine/engine.py`)

The central query execution component. Wraps a DataFusion `SessionContext` and adds segment-aware query planning on top.

**Key responsibilities:**
- Bootstraps tables at startup by re-registering all known segments from the catalog
- Parses simple `WHERE` clauses (via regex) to extract filter predicates
- Performs **segment pruning**: uses per-column min/max statistics to skip segments that cannot contain matching rows before handing the query to DataFusion
- Validates incoming data schemas against existing table definitions when appending segments
- Exposes `explain_sql()` for inspecting logical, optimized, and physical query plans

**Segment pruning logic:**

| Operator | Prune condition |
|----------|----------------|
| `>=`     | Segment `max < value` |
| `<=`     | Segment `min > value` |
| `=`      | Segment `max < value OR min > value` |

### `CatalogManager` (`engine/catalog.py`)

A SQLite-backed metadata store that is the source of truth for all table and segment information.

**Schema:**

```
tables          (id, name, created_at)
  └─ columns    (table_id, column_name, data_type, nullable)
  └─ segments   (table_id, file_path, row_count, created_at)
       └─ segment_stats  (segment_id, column_name, min_value, max_value)
```

All DDL and DML statements are stored as `.sql` files under `engine/internal_queries/catalog/` and loaded at runtime, keeping SQL out of Python source.

### `IngestionManager` (`engine/ingestion.py`)

Handles getting data into the system from external files.

**Supported input formats:** `.csv`, `.xlsx`, `.xls`, `.parquet`

**Ingestion pipeline:**
1. Load file into a Pandas DataFrame
2. Convert to a PyArrow Table (columnar)
3. Write to `jlytics_db/tables/{table_name}/segment<uuid>.parquet`
4. Compute min/max statistics for numeric and temporal columns
5. Register metadata in catalog (create table on first ingest, append segment on subsequent ingests)

### `EngineConfig` (`engine/config.py`)

Dataclass for tuning engine behavior:

| Field | Default | Description |
|-------|---------|-------------|
| `target_partitions` | `4` | Parallelism level for DataFusion |
| `batch_size` | `8192` | Record batch size for streaming |
| `collect_metrics` | `True` | Capture execution timing and row counts |
| `verbose_plans` | `False` | Print query plans to stdout |

### `QueryResult` / `QueryMetrics` (`engine/result.py`, `engine/metrics.py`)

`QueryResult` wraps the PyArrow output table alongside a `QueryMetrics` object that records:
- `execution_time_ms` — wall-clock query time
- `row_count` — number of result rows
- `batch_count` — number of record batches collected
- Optional: `logical_plan`, `optimized_plan`, `physical_plan`

---

## Data Flow

### Ingestion

```
File on disk
  → IngestionManager.ingest_file(path, table_name)
  → Parse to Pandas DataFrame
  → Convert to PyArrow Table
  → Write segment parquet to jlytics_db/tables/{table}/segment<uuid>.parquet
  → Compute column statistics (min/max for numeric/temporal types)
  → CatalogManager: create_table + add_segment + add_segment_stats
  → AnalyticsEngine: register parquet with DataFusion session
```

### Query Execution

```
SQL string
  → AnalyticsEngine.execute_sql(sql)
  → Extract table name (regex on FROM clause)
  → Extract filter predicate (regex on WHERE clause)
  → CatalogManager.get_segment_stats(table)
  → Prune segments: exclude any whose [min, max] range cannot satisfy predicate
  → Recreate DataFusion session
  → Register only surviving segments
  → DataFusion.sql(query).collect()
  → Wrap in QueryResult with timing metrics
  → Return to caller
```

---

## Usage

```python
import pathlib
from engine import AnalyticsEngine, EngineConfig
from engine.ingestion import IngestionManager

THIS_DIR = pathlib.Path(__file__).parent

engine = AnalyticsEngine(
    data_dir=THIS_DIR / "jlytics_db",
    config=EngineConfig(target_partitions=4, batch_size=8192),
)
ingestor = IngestionManager(engine)

# Ingest a file (CSV, Excel, or Parquet)
ingestor.ingest_file(THIS_DIR / "data" / "my_data.parquet", "my_table")

# Execute a query
result = engine.execute_sql("SELECT col1, SUM(col2) FROM my_table GROUP BY col1")
print(result.metrics)       # execution time, row count
print(result.to_pandas())   # convert to DataFrame

# Inspect query plans
plans = engine.explain_sql("SELECT * FROM my_table WHERE col2 >= 100")
print(plans["optimized"])   # optimized logical plan
print(plans["physical"])    # physical plan
```

---

## Storage Layout

```
jlytics_db/
├── metadata.db               # SQLite catalog (tables, columns, segments, stats)
└── tables/
    └── {table_name}/
        ├── segment<uuid1>.parquet
        ├── segment<uuid2>.parquet
        └── ...
```

Each segment is an independent Parquet file. The catalog tracks the location and column statistics for every segment, enabling the engine to skip irrelevant files before a query touches them.

---

## Tech Stack

| Library | Role |
|---------|------|
| [Apache DataFusion](https://github.com/apache/datafusion) | Columnar SQL query engine |
| [PyArrow](https://arrow.apache.org/docs/python/) | In-memory columnar format and Parquet I/O |
| [Pandas](https://pandas.pydata.org/) | Multi-format file ingestion |
| [SQLite](https://www.sqlite.org/) | Catalog metadata persistence |
