# Spark Concepts — Learning Notes

Running log of Spark concepts learned in this project, one section per step.
Code comments in the project are in Vietnamese (for the primary learner);
this doc is in English as a portable reference / portfolio artifact.

## Step 0 — SparkSession & local mode

- **`SparkSession` is the entry point to the Spark engine**, not to your data.
  Calling `SparkSession.builder...getOrCreate()` starts a JVM, a
  `SparkContext`, the scheduler, and the Spark UI web server — none of that
  touches any dataset. You can see this directly: the session prints its UI
  URL and returns instantly, with no delay proportional to any "data size,"
  because no data has been referenced yet.
- **`master("local[*]")`** tells Spark to run as if it had a cluster, but
  entirely inside this one machine's process, using every available CPU core
  as a "worker" thread. This is the reason a laptop is enough to learn real
  Spark internals (lazy evaluation, shuffles, partitioning) — the engine's
  behavior is the same; only *where* it runs differs from a real multi-node
  cluster (Databricks, EMR, on-prem YARN...).
- **`spark.sql.shuffle.partitions`**: the number of partitions Spark creates
  *every time data is shuffled* (post-`groupBy`, `join`, `orderBy`, etc.).
  The Spark default is 200, tuned for large multi-node clusters. On a local
  demo dataset (a few days of GH Archive events), 200 tiny partitions create
  pure overhead — each partition becomes its own scheduled task, and task
  startup cost can exceed the actual work per task. This project sets it to
  `8` to match single-machine, small-data scale. This is the single config
  knob we'll come back to explicitly in Step 5/6 once real shuffles appear.
- **Spark UI** (`http://localhost:4040` by default) is the window into what
  the scheduler is actually doing — jobs, stages, and (later) shuffle
  read/write sizes. It only shows something once an *action* runs; at Step 0
  there's nothing to see yet besides an idle, running application.
- **Windows-specific gotcha (Py4J + Ctrl+C):** PySpark talks to the JVM
  through Py4J over a local socket. On Windows, `Ctrl+C` sends
  `CTRL_C_EVENT` to the *entire process group*, including the JVM child
  process Py4J spawned — not just the Python process. If you interrupt a
  running script, the JVM can die before `spark.stop()` gets a chance to
  talk to it, producing a `Py4JNetworkError` / `ConnectionResetError`
  traceback. This is not a session or environment misconfiguration; it's a
  teardown race condition specific to Ctrl+C on Windows. `spark/session.py`
  wraps `spark.stop()` in `try/except` to turn that into a clear message
  instead of a scary traceback.
- **`winutils.exe` warning**: PySpark on Windows always logs a `WARN` about
  a missing `winutils.exe`/`HADOOP_HOME`. This binary is only needed for
  HDFS-specific filesystem operations; reading/writing local files and
  running local Spark jobs works fine without it. Safe to ignore for this
  project's scope.

## Step 1 — Ingestion vs. processing boundary

- `ingestion/download_gharchive.py` is **pure Python** (`requests` +
  filesystem) — deliberately *not* Spark. The boundary matters: ingestion
  (network I/O, retries, rate limits) is a fundamentally different concern
  from processing (reading/transforming data once it's already on disk).
  Keeping them as separate scripts means the raw `.json.gz` files can be
  downloaded once and reused across many different Spark experiments later
  (Bronze, Silver, Gold, and any re-runs while debugging) without
  re-hitting the network each time.
- **Idempotency**: re-running the download for an hour that's already on
  disk is a no-op (`[skip] ... đã tồn tại`), checked by file existence
  before making any HTTP request. This is a general Data Engineering
  pattern, not Spark-specific — a pipeline stage should be safe to re-run
  (after a crash, a retry, a backfill) without producing duplicate data or
  wasting bandwidth/API quota.
- **Streaming download (`stream=True`)**: GH Archive hourly files can be
  tens of MB each; loading a full response into memory before writing to
  disk doesn't scale as more hours/days are downloaded. `iter_content()`
  writes in fixed-size chunks instead.
- GH Archive's hour component in filenames is **not zero-padded**
  (`2024-01-01-5.json.gz`, not `-05.json.gz`), unlike the date portion —
  a schema/format quirk worth knowing before Step 2's schema exploration.

## Step 2 — Lazy evaluation & schema-on-read

- **Transformations build a plan; actions run it.** `spark.read.json(...)`
  and `.select(...)` are both transformations — calling them returns
  instantly regardless of file size, because Spark only records "how to
  compute this" (a query plan), not the data itself. `.show()`, `.count()`,
  and `.groupBy(...).count()` are actions — each one is a *separate* Spark
  job that re-runs the whole chain from the source file, because nothing
  was cached in between. In `spark/explore_schema.py`, three actions
  (`show`, `count`, `groupBy().count()`) reprocessed the same 169K-row file
  three independent times — a preview of why `.cache()` (Step 6) will
  matter once files get bigger.
- **Schema inference reads a sample, not the whole file** — but `df.read.json`
  still has to peek at the data to figure out field names/types, which is
  why `printSchema()` (itself not an action) can show a schema before any
  action has run.
- **`spark.read.json` infers ONE schema per file by merging fields across
  ALL records**, regardless of `type`. Since GH Archive's `payload` shape
  differs entirely per event type (a `PushEvent`'s payload looks nothing
  like an `IssuesEvent`'s), the inferred schema for `payload` is a **union
  of every event type's fields** — hundreds of nested columns, almost all
  `null` on any given row. This is the concrete "why Spark" moment for this
  dataset: a relational warehouse would force a choice (one giant sparse
  table, or many type-specific tables) upfront; Spark/Parquet let the union
  schema exist as-is until you deliberately flatten per-type in Silver.
  Full breakdown in `docs/schema_notes.md`.
- **Dot notation for nested access** (`df.select("actor.login", "repo.name")`)
  reads directly into a `struct` column without needing to flatten first —
  flattening (renaming to `actor_login`, `repo_name`) is a Silver-layer
  concern (Step 4), not required just to query nested fields.

## Coming up

- **Step 3**: Bronze layer — Parquet writes, partitioning by date,
  transformation vs. action boundary made concrete with `.write.parquet()`.
- **Step 4**: Silver — flattening + `explode()` on `payload.commits`.
- **Step 5/6**: shuffle in `groupBy` aggregations, `cache()`, and
  `spark.sql.shuffle.partitions` tuning revisited with real numbers from
  the Spark UI.
