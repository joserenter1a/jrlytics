# Project Architecture Plan
## Star Schema Data Warehouse & Query Benchmarking Tool

A hands-on OLAP learning project using Apache Arrow DataFusion, Parquet columnar storage, and star schema dimensional modelling. The goal is to build a mini data warehouse from a real-world dataset, run analytical SQL queries, and understand exactly why columnar storage wins at scale.

---

## 1. Project Overview & Goals

This project simulates the core patterns found in production OLAP systems — dimensional modelling, columnar storage, predicate pushdown, and vectorised query execution — in a self-contained Python environment you can run locally.

| | |
|---|---|
| **Dataset** | NYC Taxi Trip Records (TLC) — publicly available Parquet files, hundreds of millions of rows, rich with timestamps, locations, fares, and passenger data. Ideal for GROUP BY and aggregation-heavy OLAP queries. |
| **Primary Goal** | Build a star schema warehouse, write analytical queries using DataFusion SQL, and benchmark them against DuckDB and SQLite to feel columnar vs row-store performance differences firsthand. |
| **Secondary Goal** | Understand DataFusion's logical and physical query plans, and observe the effect of query optimisations like predicate pushdown and projection pruning. |

---

## 2. Technology Stack

| Library | Role |
|---|---|
| `datafusion` | Core OLAP query engine. Registers Parquet tables and executes SQL via the Python bindings (datafusion-python). |
| `pyarrow` | In-memory columnar format. Used to read, transform, and write Parquet files. All DataFusion results are returned as Arrow RecordBatches. |
| `pandas` | Lightweight use for initial data wrangling and result display only. Not used in the hot query path. |
| `duckdb` | Benchmarking comparison target. Also columnar but with a different execution engine — interesting to compare. |
| `sqlite3` | Row-store comparison target (stdlib). Demonstrates the performance gap clearly. |
| `rich` / `typer` | CLI interface. Rich for pretty benchmark tables; Typer for the command-line entry point. |
| `pytest` + `pytest-benchmark` | Formalised benchmarking harness with variance analysis and run-to-run comparison. |
| `uv` | Project and dependency management. Lock file ensures reproducible environments. |

---

## 3. Star Schema Design

The warehouse follows a classic star schema: one central fact table surrounded by smaller dimension tables. All tables are stored as partitioned Parquet files on disk.

### 3.1 Fact Table — `fact_trips`

The grain of this table is one row per taxi trip. Foreign keys reference dimension tables; all measures are numeric.

| Column | Description |
|---|---|
| `trip_id` | Surrogate key (integer, generated) |
| `vendor_id` (FK) | References dim_vendor |
| `pickup_datetime` | Timestamp — used for partitioning |
| `dropoff_datetime` | Timestamp |
| `pickup_location_id` (FK) | References dim_location |
| `dropoff_location_id` (FK) | References dim_location |
| `date_id` (FK) | References dim_date |
| `passenger_count` | Measure (int) |
| `trip_distance` | Measure (float) |
| `fare_amount` | Measure (float) |
| `tip_amount` | Measure (float) |
| `total_amount` | Measure (float) |
| `payment_type` | Low cardinality — candidate for dim_payment |

### 3.2 Dimension Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `dim_date` | date_id, year, month, day, quarter, day_of_week, is_weekend | Supports time-based GROUP BY without timestamp arithmetic |
| `dim_location` | location_id, borough, zone, service_zone | TLC taxi zone lookup — enables geographic aggregation |
| `dim_vendor` | vendor_id, vendor_name, vendor_code | Small table, rarely changes |

### 3.3 Partitioning Strategy

The fact table is partitioned by year and month using Hive-style partitioning. DataFusion will automatically prune partitions when queries include `WHERE` clauses on `pickup_datetime`, which is one of the key optimisations to benchmark.

```
data/warehouse/fact_trips/year=2023/month=01/part-0.parquet
data/warehouse/fact_trips/year=2023/month=02/part-0.parquet
```

---

## 4. Project Structure

```
star_schema_dw/
  pyproject.toml           # uv project config
  data/
    raw/                   # Downloaded source Parquet files
    warehouse/             # Transformed star schema Parquet files
      fact_trips/          # Partitioned by year/month
      dim_date/
      dim_location/
      dim_vendor/
  src/
    etl/
      extract.py           # Download raw data
      transform.py         # Build dimension + fact tables
      load.py              # Write to partitioned Parquet
    warehouse/
      context.py           # DataFusion SessionContext setup
      register.py          # Register all tables
    queries/
      analytical.py        # Named analytical queries
      plans.py             # Inspect logical/physical plans
    benchmarks/
      engines.py           # DataFusion / DuckDB / SQLite runners
      suite.py             # Benchmark definitions
    cli/
      main.py              # Typer CLI entry point
  tests/
    test_schema.py
    test_queries.py
    bench_compare.py       # pytest-benchmark suite
```

---

## 5. ETL Pipeline

### 5.1 Extract

Download monthly Parquet files from the TLC public S3 bucket or data.gov. A small script fetches 12 months of yellow cab data (~3–4 GB total) and stores them in `data/raw/`.

### 5.2 Transform

Using PyArrow compute functions (not Pandas):

- Cast and validate column types (enforce schema on read)
- Generate surrogate keys for fact rows
- Build `dim_date` by extracting date components from `pickup_datetime`
- Join with the TLC zone lookup CSV to build `dim_location`
- Filter out bad rows: negative fares, zero-distance trips, null locations

### 5.3 Load

Write each dimension table as a single Parquet file. Write the fact table as a partitioned dataset using `pyarrow.dataset.write_dataset()` with Hive partitioning. Compression: Snappy (good balance of speed vs size).

---

## 6. Analytical Query Suite

These are the queries you will benchmark across all three engines. Each one is designed to stress a different OLAP pattern.

| Query | Description |
|---|---|
| **Q1 — Monthly Revenue** | `SELECT year, month, SUM(total_amount) FROM fact_trips JOIN dim_date ... GROUP BY year, month ORDER BY year, month` |
| **Q2 — Top Pickup Zones** | Aggregate trip count and avg fare by pickup borough. Tests fan-out join on dim_location. |
| **Q3 — Tip Rate by Hour** | Extract hour from pickup_datetime, compute tip_amount / fare_amount. Tests expression evaluation. |
| **Q4 — Weekend vs Weekday** | Join to dim_date, filter on is_weekend, compare avg trip distance. Tests predicate pushdown on dim table. |
| **Q5 — Partition Pruning Test** | Same aggregation query run twice: once with no date filter, once filtered to a single month. Measures partition pruning benefit directly. |
| **Q6 — High Cardinality GroupBy** | GROUP BY zone (265 distinct values) + month. Tests hash aggregation at scale. |

---

## 7. Benchmarking Architecture

### 7.1 Engines Under Test

- **DataFusion** — columnar, vectorised, Rust execution engine
- **DuckDB** — columnar, vectorised, C++ execution engine (great comparison point)
- **SQLite** — row-store, single-threaded (control / baseline)

### 7.2 Measurement Approach

Use `pytest-benchmark` for statistical rigour. Each query runs a minimum of 5 times per engine with warm-up rounds discarded. Metrics captured: mean, median, stddev, min, max. Results are written to a JSON report and rendered as a Rich table in the CLI.

### 7.3 What to Look For

- **Columnar vs row-store gap on Q1** (large aggregation) — expect 10–100x difference
- **Partition pruning on Q5** — DataFusion should skip irrelevant row groups entirely
- **Projection pruning** — queries that select 3 of 13 columns should be faster than `SELECT *`
- **DataFusion vs DuckDB** — similar architecture, interesting to see where each wins

---

## 8. Query Plan Inspection Module

One of the most educational parts of this project. DataFusion exposes its optimisation pipeline via Python:

```python
df = ctx.sql(query)
print(df.explain())               # Logical + Physical plan
print(df.explain(analyze=True))   # With actual row counts
```

Build a module (`queries/plans.py`) that runs each benchmark query and captures both the unoptimised logical plan and the final physical plan. Log the differences side by side. Things to look for:

- **Filter pushdown** — does DataFusion move WHERE clauses closer to the scan?
- **Projection pushdown** — does it eliminate unneeded columns before joining?
- **Sort elimination** — does ORDER BY on a pre-sorted Parquet column get removed?
- **Aggregate pushdown** — partial aggregation before a shuffle?

---

## 9. CLI Interface

A Typer-based CLI ties the whole project together with four main commands:

| Command | Description |
|---|---|
| `uv run dw ingest` | Download raw data and run the full ETL pipeline. Idempotent. |
| `uv run dw query --name Q1` | Run a named analytical query against DataFusion and pretty-print the result. |
| `uv run dw benchmark --queries Q1,Q2,Q5` | Run the benchmark suite for specified queries and output a comparison table across all three engines. |
| `uv run dw plans --query Q5` | Print the logical and physical execution plan for a given query. |

---

## 10. Suggested Build Order & Learning Milestones

| Phase | Task | Core Learning |
|---|---|---|
| **Phase 1** | Download data, write ETL with PyArrow, produce the star schema Parquet files | Columnar types, Parquet format, schema enforcement |
| **Phase 2** | Set up DataFusion SessionContext, register tables, run Q1 manually | DataFusion Python API, SQL over Parquet |
| **Phase 3** | Add DuckDB and SQLite engines, run Q1 on all three, compare | Engine comparison, benchmark harness basics |
| **Phase 4** | Add remaining queries Q2–Q6, formalise with pytest-benchmark | OLAP query patterns, aggregation at scale |
| **Phase 5** | Build the plan inspection module, study Q5 partition pruning | Physical query plans, OLAP optimisations |
| **Phase 6** | Polish CLI, write a README documenting your benchmark results | Communication, synthesis of learning |

---

## 11. People to Follow & Literature to Read

### 11.1 Key People

| Person | Why They Matter |
|---|---|
| **Andy Pavlo** | CMU professor, teaches the famous 15-445 Database Systems course (free on YouTube). Specialises in OLAP systems and NVM storage. Unfiltered and direct — one of the best educators in the field. |
| **Peter Boncz** | Co-creator of MonetDB and the vectorised execution model that DuckDB and DataFusion both descend from. His research papers are foundational reading. |
| **Hannes Mühleisen** | Creator of DuckDB. Prolific on Twitter/X, writes deeply about in-process analytics, OLAP design decisions, and the tradeoffs in columnar engines. |
| **Wes McKinney** | Creator of Pandas and a co-creator of Apache Arrow. His blog and talks on the Arrow columnar format are essential context for why PyArrow exists and what problems it solves. |
| **Jorge Cardoso Leitao** | Core contributor to Arrow2 and DataFusion. Writes excellent technical deep-dives on vectorised computing and the Arrow memory model. |
| **Gil Forsyth** | Active DataFusion community member. Good for following practical DataFusion Python usage and new API developments. |

### 11.2 Essential Papers

| Title | Why Read It |
|---|---|
| **MonetDB/X100 (2005)** — Boncz, Zukowski, Nes | The paper that introduced vectorised execution. Every modern OLAP engine (DuckDB, DataFusion, Velox) traces its lineage here. Read this first. |
| **Column-Stores vs Row-Stores (2008)** — Abadi et al. | Classic VLDB paper directly comparing C-Store (columnar) vs a row store on analytical workloads. Explains compression advantages, late vs early materialisation. |
| **Design and Implementation of Modern Column-Oriented Database Systems (2012)** — Abadi, Boncz, Harizopoulos | Comprehensive survey — essentially a textbook chapter. Covers compression schemes, vectorised processing, and late materialisation in depth. |
| **Dremel (2010)** — Melnik et al. (Google) | The paper behind Parquet. Explains nested columnar encoding, repetition/definition levels. Crucial for understanding what Parquet is actually storing on disk. |
| **Apache Arrow and the 10 Things I Hate About Pandas (2017)** — Wes McKinney | Blog post, not a paper — but required reading. Explains the motivation for Arrow's in-memory columnar format and the zero-copy sharing model. |
| **DuckDB (2019)** — Raasveldt & Mühleisen | The original DuckDB SIGMOD paper. Explains the design decisions behind an in-process analytical database. Very readable. |

### 11.3 Books

| Book | Why Read It |
|---|---|
| **Designing Data-Intensive Applications** — Martin Kleppmann | Chapter on storage engines (SSTables, LSM trees, B-trees, columnar) is particularly relevant. Best overall systems book available. |
| **Database Internals** — Alex Petrov | Goes deeper on storage engine implementation, B-tree variants, and distributed consensus. Good follow-on after Kleppmann. |
| **The Data Warehouse Toolkit (3rd Ed.)** — Kimball & Ross | The bible of dimensional modelling. Covers star schemas, slowly changing dimensions, factless fact tables, and more. Essential for the modelling half of this project. |

### 11.4 Free Courses & Talks

| Resource | Notes |
|---|---|
| **CMU 15-445/645** | Andy Pavlo's database systems course. Full lecture videos on YouTube. Covers storage, indexing, query execution, and concurrency. One of the best free CS courses available. |
| **DataFusion Architecture Talks** — Andrew Lamb | Available on YouTube. Walks through the physical plan, scheduler, and Arrow integration from the perspective of a core maintainer. |
| **Voltron Data Blog** | The team behind DataFusion and Arrow. Regular deep technical posts on columnar query engines, the Arrow ecosystem, and Substrait (a portable query plan format). |

---

> **Best starting move:** read the MonetDB/X100 paper and watch Pavlo's CMU storage + execution lectures in parallel with building Phases 1 and 2. The concepts will click much faster when you can see them in running code.
