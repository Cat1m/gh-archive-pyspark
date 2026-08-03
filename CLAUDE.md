# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project purpose

This is a learning/portfolio project for PySpark, using real GH Archive data
(GitHub's public event stream — push, PR, issue, star, fork...) as the
teaching vehicle. The goal is to understand the **Spark engine** (lazy
evaluation, transformation vs action, shuffle, partitioning, caching, data
skew) via a local Bronze → Silver → Gold pipeline — not to build production
infrastructure or a deployable data platform. Keep this framing in mind: code
here favors pedagogical clarity (heavy inline comments explaining *why* Spark
behaves a certain way) over abstraction or production hardening.

The README (`README.md`) is written in Vietnamese and is the primary source
of truth for architecture, findings, and setup — read it first. Supporting
docs live in `docs/`: `schema_notes.md`, `data_profiling.md` (real schema
survey of GH Archive's nested `payload` — 25 top-level fields, mostly null
per row), and `spark_concepts.md`.

## Environment setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Windows only**: writing Parquet (Bronze/Silver/Gold) requires
`winutils.exe` + `hadoop.dll` in `tools/hadoop/bin/` (gitignored, third-party
binaries). Without them, `.write.parquet()` crashes with
`UnsatisfiedLinkError`/`FileNotFoundException`. Fetch once:

```bash
mkdir -p tools/hadoop/bin
curl -fSL -o tools/hadoop/bin/winutils.exe \
  "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
curl -fSL -o tools/hadoop/bin/hadoop.dll \
  "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"
```

`spark/session.py` auto-detects and wires `HADOOP_HOME`/`PATH` to this
directory on import — no manual env vars needed. Not required on Linux/Mac.

## Common commands

Full pipeline, one command (download → Bronze → Silver → Gold → findings):

```bash
python quickstart.py                                  # defaults to yesterday (UTC)
python quickstart.py --skip-download                   # reuse existing data/raw
python quickstart.py --start 2026-08-01 --end 2026-08-01
```

Run steps individually (useful to inspect each step's Spark UI/logs in isolation):

```bash
python spark/session.py                                       # SparkSession smoke test, prints Spark UI URL
python ingestion/download_gharchive.py --start YYYY-MM-DD --end YYYY-MM-DD
python spark/explore_schema.py                                 # explore nested JSON schema
python spark/bronze.py                                         # raw JSON.gz -> partitioned Parquet
python spark/silver.py                                         # flatten + explode_outer
python spark/gold.py                                            # aggregations (shuffle)
python spark/optimize_demo.py                                   # cache / partition-tuning / skew demo
python analysis/findings.py                                     # read-only analysis over Gold
```

There is no test suite, linter, or CI configured in this repo — don't assume
`pytest`/`ruff`/etc. exist; verify changes by running the relevant script
directly and checking the printed row counts / Spark UI.

## Architecture: Bronze → Silver → Gold

```
GH Archive (.json.gz, 1 file/hour)
   │  ingestion/download_gharchive.py  — pure Python, idempotent + retry
   ▼
data/raw/            raw .json.gz, untouched

   │  spark/bronze.py — spark.read.json() -> withColumn(event_date) -> write.parquet()
   ▼
data/bronze/         Parquet, partitioned by event_date, KEEPS the raw union schema as-is

   │  spark/silver.py — flatten (actor.login -> actor_login, ...) + explode_outer(payload.labels)
   │                    + to_timestamp(created_at) + defensive dropDuplicates(event_id)
   ▼
data/silver/         Parquet, flat, grain = (event, PR label if any)

   │  spark/gold.py — dropDuplicates(event_id) [restores 1-row-per-event grain]
   │                  groupBy(repo/hour/actor) -> SHUFFLE
   ▼
data/gold/           3 tables: top_repos_by_stars, events_by_hour, top_actors

   │  analysis/findings.py — read-only, no heavy Spark job
   ▼
Findings             printed to console
```

Each layer writes to its own Parquet directory (never overwrites another
layer), so any layer can be re-derived/rerun without re-downloading source
data.

### Key design decisions worth knowing before touching this code

- **Bronze does not clean or transform content** — it only adds a technical
  `event_date` column for partitioning. Any cleaning/renaming belongs in
  Silver, not Bronze. This boundary is intentional and documented in
  `spark/bronze.py`.
- **Silver's grain is (event, label), not (event)**: `explode_outer` on
  `payload.labels` means a PR with N labels produces N rows sharing the same
  `event_id`. Anything downstream that counts/aggregates per-event (Gold)
  must `dropDuplicates(["event_id"])` first, or per-event counts will be
  inflated by the label fan-out. `spark/gold.py` does this explicitly at the
  top of `main()`.
- **`payload.labels` is shared by both `PullRequestEvent` and `IssuesEvent`**
  in GH Archive's union schema. Silver deliberately scopes it to PR-only via
  `F.when(type == "PullRequestEvent", ...)` before exploding — if you add new
  event types that also populate `labels`, don't let them silently leak into
  the `pr_label_name` column.
- **`explode_outer` vs `explode`**: `payload.labels` is NULL for the vast
  majority of rows (non-PR events). Plain `explode()` would silently drop
  every row without a non-null array. Always use `explode_outer` for
  optional/sparse arrays like this.
- **Driver memory differs by script**: `spark/session.py`'s default
  (`spark.driver.memory=2g`) is fine for Step 0-2 demo scripts, but
  `spark/bronze.py`/`silver.py`/`gold.py` each create their own SparkSession
  with `driver.memory=6g` because reading Bronze's full union schema (hundreds
  of nested columns) OOMs the Py4J driver at 2g on ~4M rows. Don't reuse
  `spark.session.get_spark_session()` for these — they intentionally define
  their own `get_*_spark_session()`.
- **`spark.sql.shuffle.partitions=8`** (not the 200 default) across all
  sessions — tuned for local, small-data demo runs; the default 200 is
  designed for large clusters and adds overhead here (measured ~2x slower).
- **Reading raw files**: `spark/bronze.py` passes an explicit Python-built
  `list` of file paths to `spark.read.json()` instead of a wildcard string.
  A wildcard forces Spark to call Hadoop's `globStatus()`/`listStatus()`,
  which needs `winutils.exe` and crashes without it on Windows. Keep this
  pattern for any new raw-file-reading code.
- **`github-actions[bot]` is a real, intentional data-skew example**
  (~5.58% of all events in the sampled day) used to demonstrate uneven
  shuffle partitions in `spark/gold.py`/`optimize_demo.py` — don't "fix" this
  by filtering it out; it's the point of that Gold table.
- **`payload.commits` does not exist** in current GH Archive data (the
  original plan used it for the `explode()` example) — don't reintroduce
  assumptions about fields without checking `docs/data_profiling.md` first,
  since this source's schema has changed over time.
- **`quickstart.py` is a subprocess wrapper, not a real orchestrator** — no
  retry/scheduling/dependency graph. Each step runs as an independent Python
  subprocess (its own JVM/SparkSession). Don't treat it as production-grade;
  see README's "Future work" for the intended real orchestration path
  (Airflow/Dagster).
