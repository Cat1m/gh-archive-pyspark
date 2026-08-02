# GH Archive Schema Notes

Notes from exploring a real sample file: `data/raw/2026-08-01-10.json.gz`
(one hour, 169,582 events) via `spark/explore_schema.py`.

## Top-level fields (stable across all event types)

```
id            : string   — unique event id
type          : string   — event type discriminator (PushEvent, CreateEvent, ...)
actor         : struct   — { id, login, display_login, avatar_url, gravatar_id, url }
repo          : struct   — { id, name, url }
org           : struct   — same shape as actor; only present when the event
                            belongs to an org-owned repo
public        : boolean
created_at    : string   — ISO-8601 timestamp, as a STRING not a timestamp type
                            (Spark infers types per-field independently — this
                            needs an explicit cast in Silver, see Step 3/4)
payload       : struct   — schema differs by `type`, see below
```

- `actor.login` / `repo.name` are the two fields used most for aggregation
  in Gold (Step 5) — flat strings even though they live inside a nested
  `struct`, accessed with dot notation: `df.select("actor.login", "repo.name")`.

## The key insight: `payload` schema is a UNION across all event types

Calling `spark.read.json(path)` makes Spark infer one schema for the whole
file by sampling records and merging every field it sees into a single
struct. Since a `PushEvent`'s payload (`push_id`, `commits`, `ref`, ...) and
an `IssuesEvent`'s payload (`issue`, `action`, `label`, ...) share the same
top-level `payload` column, Spark's inferred schema for `payload` ends up
containing **every field from every event type combined** — several hundred
nested fields total (see `payload.pull_request.head.repo`,
`payload.issue.assignees`, `payload.release.assets`, etc.).

**Practical consequence**: for any single row, most of `payload.*` is
`null` — only the subset relevant to that row's `type` is populated. This is
exactly the "SQL warehouse struggles here" case referenced in the project's
brief: a traditional relational table would need one wide sparse table (this
schema) or dozens of type-specific tables decided upfront. Spark/Parquet let
you keep the union schema as-is in Bronze and only commit to a specific,
flattened shape once you filter to one `type` in Silver (Step 4).

## Event type distribution (this sample hour)

| type                          | count   |
|-------------------------------|--------:|
| PushEvent                     | 165,012 |
| CreateEvent                   |   3,125 |
| DeleteEvent                   |   1,173 |
| PullRequestEvent              |     103 |
| IssueCommentEvent              |      43 |
| IssuesEvent                   |      33 |
| PullRequestReviewCommentEvent |      29 |
| PullRequestReviewEvent        |      26 |
| WatchEvent (= star)           |      22 |
| ReleaseEvent                  |      10 |
| ForkEvent                     |       5 |
| MemberEvent                   |       1 |

`PushEvent` dominates (~97% of all events) — worth remembering when
designing Gold aggregations in Step 5: a naive `groupBy("repo_name")` over
raw events will be almost entirely driven by push activity, not stars/forks.
Also a preview of the **data skew** concept from Step 6: a handful of
extremely active bot/CI repos can push far more than a typical repo, so any
per-repo aggregation should expect a long tail.

## Fields planned for Silver flattening (Step 4)

- `actor.login` → `actor_login`
- `actor.id` → `actor_id`
- `repo.name` → `repo_name`
- `repo.id` → `repo_id`
- `created_at` (string) → `created_at` (timestamp, cast)
- ~~`payload.commits` (array, PushEvent only) → exploded to one row per
  commit~~ — **superseded**, this field does not exist in the actual data
  (GH Archive dropped it from `PushEvent.payload`). See
  `docs/data_profiling.md` §4.1: the explode example uses
  `PullRequestEvent.payload.labels` instead.
- `payload.action`, `payload.ref`, `payload.ref_type` etc. — kept only for
  the event types where they're meaningful; Silver will likely split by
  `type` rather than carrying the full sparse union forward.

## GH Archive filename quirk (carried over from Step 1)

Hour component in the source filename is **not zero-padded**:
`2026-08-01-10.json.gz` for hour 10, but `2026-08-01-5.json.gz` (not `-05`)
for hour 5.
