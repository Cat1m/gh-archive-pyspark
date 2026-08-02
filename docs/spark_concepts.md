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

## Step 3 — Bronze: writes, partitioning, and the Windows write gotcha

- **`.write.parquet(...)` is the action** that finally runs the whole chain
  built by `spark.read.json(...)` and `.withColumn("event_date", ...)`. Both
  of those are transformations — the JSON files aren't actually opened,
  decompressed, or parsed until the write call executes. This is the same
  lazy-evaluation lesson from Step 2, now with a real multi-GB write instead
  of a `.show()`.
- **`partitionBy("event_date")`** writes one subdirectory per distinct date
  value (`event_date=2026-08-01/`) instead of one flat file. The payoff is
  **partition pruning**: a later read with a `WHERE event_date = ...` filter
  only touches the matching subdirectory, skipping everything else — this
  only becomes visible once Bronze accumulates multiple days (the demo
  dataset has exactly one partition, so the folder structure is the
  interesting part right now, not a measurable pruning win yet).
- **24 output part-files for 24 input files**: Spark's default parallelism
  for this local `master("local[*]")` run created one output file per input
  partition (one GH Archive hourly file → one Spark partition → one part
  file). This is a directly observable link between input file count and
  output file count that will matter later when tuning partition counts.
- **Round-trip verification matters**: re-reading the just-written Parquet
  and counting rows (4,071,556) against the number already established in
  `docs/data_profiling.md` is how you catch a silent row-loss bug (e.g. a
  bad join, a bad filter, a partition write failure) — trusting "the job
  didn't throw" is not the same as "the data is complete."

### Windows-specific: writing requires `winutils.exe` + `hadoop.dll`

Reading JSON worked fine on Windows without any extra setup (Steps 0-2).
**Writing** Parquet is different: Hadoop's local filesystem writer
(`RawLocalFileSystem`) needs to set directory permissions and list committed
task output during `FileOutputCommitter.commitJob()`, and both operations
shell out to native Windows helpers that don't ship with PySpark's pure-Java
JAR:

- `HADOOP_HOME` (env var) — Hadoop looks here for `bin/winutils.exe`, an
  executable it invokes via `ProcessBuilder` for chmod/permission-style
  operations, even on a purely local (non-HDFS) filesystem.
- `PATH` — separately, the JVM needs to load `hadoop.dll` (a JNI native
  library) to satisfy calls like `NativeIO$Windows.access0`. Setting
  `HADOOP_HOME` alone does **not** put its `bin/` on the library search
  path; without it you get `UnsatisfiedLinkError` instead of the earlier
  `FileNotFoundException`. Both env vars have to be set for a write to
  succeed.

This project fetches Hadoop 3.3.6's Windows binaries (closest available
match to PySpark 3.5.9's bundled Hadoop 3.3.4) from the community-maintained
`cdarlint/winutils` GitHub repo into `tools/hadoop/bin/` (gitignored — a
third-party binary, not something to commit), and `spark/session.py` sets
both env vars automatically at import time if the files are present. See
README.md "Setup Windows: winutils.exe" for the exact fetch commands.

## Step 4 — Silver: flatten, `explode_outer()`, and a scoping bug caught by checking numbers

- **Flatten is just `select()` with dot-notation + aliasing** — no new
  concept beyond Step 2's `df.select("actor.login", "repo.name")`, just
  applied more systematically (`actor.login` → `actor_login`, etc.) and
  combined with `to_timestamp(col, format)` to commit `created_at` from
  string to an actual timestamp type. Bronze deliberately left it as a raw
  string; Silver is where a real type gets chosen.
- **`explode()` vs `explode_outer()` — the central lesson of this step.**
  `payload.labels` (array<struct>) is only non-null for `PullRequestEvent`
  rows; every other event type has `NULL` there. Plain `explode()` **drops
  the row entirely** when the array is `NULL` or empty — using it here
  would have silently deleted all ~4.07M non-`PullRequestEvent` rows from
  Silver. `explode_outer()` keeps one row (with `NULL` in the exploded
  column) whenever the array is `NULL`/empty, and only fans a row out to N
  rows when there are N real elements. This is the concrete mechanism
  behind "1 row → N rows" for rows that actually have data, while leaving
  every other row's cardinality at exactly 1.
- **A real bug this project ran into and caught via round-trip counting**:
  `payload.labels` turned out to be populated by **both** `PullRequestEvent`
  (1,336 events) and `IssuesEvent` (530 events) — GH Archive's union schema
  shares the same field name across event types (same lesson as Step 2's
  schema-on-read finding, applied concretely). The first version of
  `spark/silver.py` exploded `payload.labels` unconditionally, silently
  mixing `IssuesEvent` labels into a column named `pr_label_name`. This
  wasn't caught by the code running successfully — it was caught by
  comparing the post-explode row count against the expected math
  (`~1,336 × 2.37 avg ≈ 3,162`, not the `4,822` the buggy version produced)
  and then explicitly checking `groupBy("type")` on the non-null label
  rows. **Row counts that don't match a predicted estimate are a real
  correctness signal** — the fix was gating the source column with
  `F.when(type == "PullRequestEvent", payload.labels).otherwise(None)`
  before exploding, so no other event type can leak into a column whose
  name implies PR-only scope.
- **Defensive `dropDuplicates` vs. fixing a found bug**: `data_profiling.md`
  already confirmed 0 duplicate `event_id` values in the raw data, so
  `dropDuplicates(["event_id", "pr_label_name"])` here is a no-op on this
  dataset — kept for idempotency if Silver is re-run, not because evidence
  showed duplicates. Note the composite key: after `explode_outer`, the
  same `event_id` legitimately repeats once per label, so deduping on
  `event_id` alone would have wrongly deleted N-1 valid label rows per PR.

## Step 5 — Gold: shuffle, finally made concrete

- **What actually shuffles, contrasted directly**: `filter()`, `select()`,
  `withColumn()` (including `F.hour(...)`) are all *narrow* transformations
  — each output row depends only on its own input row, computable entirely
  within one partition, no data movement needed. `groupBy(...).count()` and
  `orderBy(...)` are *wide* — every row sharing a key might currently sit on
  a different partition, so Spark must physically move rows across
  partitions (a shuffle) before it can combine them. This is the first time
  in the project a transformation triggers real network/disk data movement
  instead of just per-row computation.
- **`spark.sql.shuffle.partitions=8` made observable**: every `groupBy()` in
  `spark/gold.py` produces exactly 8 output partitions after its shuffle,
  regardless of the grouping key (repo name, hour-of-day, or actor login).
  Visible directly in the Spark UI's Stages tab as the reduce-side task
  count.
- **`dropDuplicates()` is a shuffle too** — it's easy to assume only
  `groupBy`/`join`/`orderBy` shuffle, but deduplication needs identical rows
  brought together to compare, which is the same underlying mechanism as
  `groupBy`. Used here to fix a grain problem: Silver's grain is
  `(event, label)` because of Step 4's `explode_outer()`, so a
  `PullRequestEvent` with 5 labels appears as 5 duplicate rows by
  `event_id`. Aggregating actor/hour statistics directly on Silver would
  have overcounted that PR's author by 5× instead of 1×.
  `events = silver.dropDuplicates(["event_id"])` restores one-row-per-event
  before any of the three Gold aggregations run.
- **Skew, with real numbers instead of a hypothetical**: `groupBy("actor_login")`
  must route all 227,280 `github-actions[bot]` rows to the *same* one of 8
  post-shuffle partitions (same key → same partition, always), while most
  other actors have single digits. That one partition ends up carrying a
  wildly disproportionate share of the shuffle — directly visible in the
  Spark UI as one task's "Shuffle Read" dwarfing its seven siblings in the
  same stage. Compare against `groupBy("hour")`: only 24 possible keys, and
  the real event counts per hour turned out fairly even (~166K–179K), so
  that stage's 8 tasks look much more balanced. Same operation, same
  partition count, very different balance — because skew is a property of
  the *key distribution in the data*, not of the `groupBy` call itself.
- **No `.cache()` yet, on purpose**: all three Gold tables are derived from
  the same `events` DataFrame, but each `.write.parquet()` independently
  re-triggers the full read-Silver → dedupe → aggregate chain — three full
  passes over Silver for three outputs. This is deliberate foreshadowing
  for Step 6, where `.cache()` will be introduced specifically to avoid
  this repeated work.

## Step 6 — cache, partition tuning, and skew, measured (not just described)

`spark/optimize_demo.py` runs 3 experiments and prints real wall-clock
numbers instead of asserting behavior abstractly — actual results from this
project's data, single local run (absolute numbers will vary by machine,
the *direction* and *ratio* is what matters):

**1. `.cache()` avoiding recomputation.** Without `.cache()`, two consecutive
actions on the same `events` DataFrame (`silver.dropDuplicates(["event_id"])`)
each re-ran the full read-Silver + shuffle-dedupe chain: `count()` took
3.47s, then `filter(...).count()` took 1.97s — both paid the full
recomputation cost (the second was faster mostly from OS-level file page
caching after the first read, not from anything Spark itself remembered).
After `events.cache()`, the *first* action still cost the full 6.33s
(caching only happens as a side effect of actually materializing the data —
`.cache()` alone, before any action, does nothing) — but the *second*
action on the cached DataFrame dropped to 0.34s, roughly **18× faster**,
because it read from in-memory cache instead of re-reading Parquet and
re-running the shuffle. `.unpersist()` releases that memory once the cached
DataFrame is no longer needed — caching is not free, it holds RAM for as
long as the reference (or explicit `unpersist()`) allows.

**2. `spark.sql.shuffle.partitions` tuning, measured.** Same
`groupBy("actor_login").count()` (305,494 distinct actors either way), same
cached input, only the partition count changed via `spark.conf.set(...)`
(no need to rebuild the SparkSession — it takes effect on the next query
executed): **200 partitions → 0.95s**, **8 partitions → 0.47s**, roughly 2×
slower with 200. On this single-machine, few-million-row dataset, 200 tiny
post-shuffle tasks cost more in scheduling/startup overhead than the actual
per-task work — the opposite would be true on a real cluster with far more
data per task, which is exactly why 200 is Spark's cluster-oriented default
and this project overrides it to 8 for local-demo scale (Step 0's config
choice, now backed by a measured comparison).

**3. Data skew, seen directly via `spark_partition_id()`.** After
`groupBy("actor_login").count()`, tagging each output row with
`F.spark_partition_id()` and summing the original event counts per
partition shows real imbalance across the 8 post-shuffle partitions:

| partition | distinct actors | total original events | that partition's biggest actor |
|---|---:|---:|---:|
| 0 | 37,737 | 467,425 | 11,764 |
| 1 | 37,820 | 473,207 | 5,941 |
| 2 | 38,298 | 492,039 | 3,289 |
| 3 | 38,310 | 481,994 | 4,240 |
| 4 | 38,265 | 473,915 | 7,609 |
| 5 | 38,379 | 469,425 | 1,452 |
| 6 | 38,674 | 505,956 | 23,248 |
| **7** | 38,011 | **707,595** | **227,280** (`github-actions[bot]`) |

The **number of distinct keys** per partition is nearly identical (~38K
everywhere — hashing spreads unique keys evenly). The **total row count**
per partition is not (467K–508K for 7 partitions, 708K for the one holding
`github-actions[bot]`) — because hash partitioning distributes *keys*
evenly, not the *rows behind each key*. Partition 7 has roughly 40-50% more
underlying work than its siblings, which shows up on the Spark UI's Stages
tab as one task in the stage taking noticeably longer than the other seven
running in parallel — the stage's wall-clock time is bounded by its
slowest task, not its average one. (Fixing this — salting the key, or
Adaptive Query Execution's skew-join handling — is out of scope for this
project's local-demo size, but the mechanism is now directly visible rather
than theoretical.)

## Step 7 — Portfolio polish: reading Gold is cheap, everything expensive already happened

- `analysis/findings.py` only reads the 3 small Gold Parquet outputs and
  does light `.show()`/`.collect()` work — no `groupBy`, no shuffle, no
  multi-GB scan. This is the payoff of the medallion split: the expensive
  steps (JSON parsing, `explode_outer`, shuffle-heavy aggregation) already
  ran once in Bronze/Silver/Gold, so anything built on top of Gold (a
  report, a dashboard query) stays cheap by construction.
- `.collect()` on `events_by_hour` (24 rows) is safe here specifically
  *because* Gold already reduced the data to a handful of rows — the same
  call on Silver or Bronze (millions of rows) would try to pull everything
  to the driver and risk OOM. Whether `.collect()` is safe depends entirely
  on how much has already been aggregated away by that point in the
  pipeline, not on the method itself.
- `quickstart.py` is intentionally *not* a real orchestrator — it's a thin
  subprocess wrapper chaining the exact same CLI scripts a user would run
  by hand, in order, stopping on the first failure (`check=True`). No
  retries, scheduling, or dependency graph — see README "Future work" for
  what a real orchestrator (Airflow/Dagster) would add.
