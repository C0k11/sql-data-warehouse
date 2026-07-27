# The same warehouse on PySpark + Delta Lake

This directory rebuilds the medallion warehouse from `scripts/` on a second
engine. It is not a rewrite for its own sake: the spec, the cleansing rules,
the 40 quality checks and the Type-2 customer dimension are all the same, which
is what makes the pair useful — the differences that remain are attributable to
the *engine*, not to someone changing their mind.

The same package runs in three places with no code changes:

| Where | How | Delta comes from |
|---|---|---|
| Laptop / CI | `python -m lakehouse.run_pipeline` | `delta-spark` (open source) |
| Databricks | `notebooks/databricks_pipeline.py` | built into the runtime |

There are no Databricks credentials anywhere in this repo or in CI. Delta Lake
is open source, so the identical package CI exercises against local storage is
the one that runs on Databricks serverless.

## Running it

```bash
pip install "pyspark==4.0.1" "delta-spark==4.0.1"
python -m lakehouse.run_pipeline --steps all   # full rebuild
python -m lakehouse.run_pipeline               # incremental (keeps SCD2 history)
python -m tests.lakehouse_checks               # SCD2 + encoding behaviour
```

Needs a JDK 17 or 21 on `PATH`. On Windows, Spark additionally requires the
Hadoop `winutils.exe` shim; WSL or a Linux container avoids that entirely.

## What the engine change removed

Two of the most expensive defects in the SQL Server build simply stop existing
here, which is worth being precise about — they were *platform* problems, not
design problems:

- **The landing zone is gone.** `BULK INSERT` decodes UTF-8 with the host code
  page on Windows and rejects the `CODEPAGE` option outright on Linux (error
  16202), so the T-SQL pipeline has to rewrite every extract as UTF-16 with LF
  endings before loading. Spark takes `encoding` as a read option and handles
  CRLF itself, so the extracts are read exactly as they sit in the repo.
- **`@@DATEFIRST` no longer matters.** `DATEPART(WEEKDAY)` shifts with the
  server setting and the login's language, so the T-SQL build computes weekends
  as a day-count modulo. Spark's `dayofweek` is fixed at 1=Sunday..7=Saturday.

What did **not** get dropped is the row-count reconciliation the landing zone
was protecting. "The loader silently dropped rows" is a failure mode of readers
in general — it caught a real defect in this dataset (three extracts ship
without a trailing newline) and it stays.

## What the engine change cost

**Delta has no unique indexes.** SQL Server enforces "at most one current row
per customer" with a filtered unique index — a constraint the engine will not
let you violate. Here the same invariant is only verified *after* the load by
`scd2_at_most_one_current`. That is a genuine reduction in strength: a loader
bug would produce a double-counted customer that lives until the gate runs,
rather than being rejected at write time.

**Audit rows are batched, not per-statement.** Each Delta append is a separate
transaction commit, so the row-at-a-time audit logging that is free on SQL
Server would add seconds to a pipeline whose real work takes seconds. Details
are buffered in the driver and flushed once per run — same content, one commit
instead of six. Failures still flush before re-raising.

**`etl.config` was not carried over.** The T-SQL build reads the data root from
a config table because `BULK INSERT` needs a literal path, which forces dynamic
SQL. Spark takes the path as an argument. Porting the table anyway would have
been cargo cult.

## Three things that exist on Databricks but not in Apache Spark

Each of these resolves fine on Databricks and fails in CI, so each was found by
running the pipeline rather than by reading it:

| Written first | Why it fails on open-source Spark | What it became |
|---|---|---|
| `try_to_date(s, 'yyyyMMdd')` | Databricks-only builtin; absent from Spark's `FunctionRegistry` | `CAST(try_to_timestamp(...) AS DATE)` |
| `customer_sk BIGINT GENERATED ALWAYS AS IDENTITY` | identity columns need Unity Catalog; OSS Delta raises `UNSUPPORTED_FEATURE.TABLE_OPERATION` | `max(customer_sk) + row_number()`, same stability guarantee under the single-writer assumption |
| `SparkSession.builder` without `enableHiveSupport()` | works on Databricks (Unity Catalog persists metadata) but leaves a local run with an in-memory catalog | Derby metastore under the state dir |

That last one is the nastiest of the three, because nothing fails. The pipeline
reports success, the Delta files are written correctly, and then the *next*
process sees an empty catalog sitting on top of a full warehouse directory.

## Dialect differences that change data

These are the ones that produce *silently different output* rather than an
error. Each is commented at the site in the code.

| | SQL Server | Spark | Consequence if translated literally |
|---|---|---|---|
| `ORDER BY x DESC` | NULLs last | NULLs **first** | Dedupe keeps a different row when the newest `create_date` is NULL. Fixed with explicit `NULLS LAST`. |
| `INT / INT` | truncates | promotes to `DOUBLE` | Derived price carries a fraction, so `sales = qty × price` fails on rounding alone. Fixed with `CAST(... AS INT)`. |
| `x != TRIM(x)` | always false (ANSI padding) | works | The T-SQL check needs `DATALENGTH`; here the direct comparison is correct. |
| `TRY_CONVERT(DATE, '5489', 112)` | year 5489 | a bare date parse over-accepts too | Same trap, same guard: length must be 8 and the year must be plausible. |
| unparseable date | `TRY_CONVERT` returns NULL | `to_date` **raises** — ANSI mode is on by default in Spark 4.0 | Every malformed `sls_order_dt` aborts the load instead of becoming NULL. Fixed with `try_to_timestamp`. |

One difference that is **not** on this list, because checking it showed the
opposite of what I first assumed: Spark's lateral column alias resolution does
*not* let a `SELECT` alias shadow a same-named source column. Spark resolves a
local column reference first and only falls back to the alias when the name is
otherwise unresolvable, so `PARTITION BY prd_key` binds to the source column
exactly as it does in T-SQL. The table qualifier in `silver.py` is there for
readability, not to prevent a bug.

## SCD2: hand-rolled vs MERGE

The T-SQL build deliberately avoids `MERGE` — SQL Server's implementation has a
long history of concurrency and duplicate-action bugs — and hand-rolls
expire-then-insert as two set-based statements.

Here `MERGE INTO` is the right call: it is a core Delta primitive, and
`WHEN NOT MATCHED BY SOURCE` expresses "this customer disappeared from the
source" as one clause instead of a `LEFT JOIN`. Insertion stays a separate
statement, because one source row needs to both close the old version and open
a new one, which a single `MERGE` cannot express without duplicating the source.

The change-detection hash is SHA-256 over the same attributes in the same
order, but **the digests are not comparable across engines**: T-SQL hashes the
UTF-16 bytes of `NVARCHAR`, Spark hashes UTF-8. Each side is internally
consistent, which is all change detection requires.

## Proving the migration did not change the data

`tools/parity.py` fingerprints every silver and gold table on either engine and
diffs the two, so "the Spark build is equivalent" is a test result rather than a
claim:

```bash
python tools/parity.py --engine sqlserver --out tests/parity_baseline_sqlserver.json
python tools/parity.py --engine spark     --out build/parity_spark.json
python tools/parity.py --compare tests/parity_baseline_sqlserver.json build/parity_spark.json
```

Only portable aggregates are compared. Row hashes are excluded (different
digest bytes per engine), `LEN` is avoided because SQL Server ignores trailing
spaces while `length` does not, and string `min`/`max` are skipped because the
SQL Server instance is case-insensitive and Spark is not. Technical columns —
engine-assigned surrogate keys, load timestamps, `attr_hash` — are excluded by
design.

The T-SQL baseline is checked in, so CI can verify parity without standing up
SQL Server in the Spark job.

## Results (measured, not estimated)

Full rebuild under WSL2 (Ubuntu 24.04, JDK 21, `local[*]`, 32 threads visible):

| | |
|---|---|
| Source rows ingested | **116,294** across 6 extracts |
| File-to-bronze reconciliation | **6/6** tables match |
| Quality gate | **39 pass, 1 warn, 0 fail** |
| Full rebuild | **47.4s / 47.5s** on consecutive runs |
| Cross-engine parity | **296 metrics across 11 tables — IDENTICAL** |
| Behavioural tests | non-ASCII round-trip, SCD2 versioning, SCD2 idempotence — all pass |

The single warning is `dim_products_category_coverage`: 7 products in category
`CO_PE` have no matching ERP category row. **The T-SQL build reports the same
warning on the same check** — the gate is not tuned per engine, so a real
divergence would show up as a different count, not as a different opinion.

For contrast, the T-SQL build rebuilds the same warehouse in roughly 6 seconds.
Spark is about 8× slower here, and that is the expected answer rather than a
defect: 116k rows is far below the scale at which distributed execution earns
back its own startup cost. The reason to run this on Spark is the platform it
unlocks — Delta time travel, `MERGE`, and a path to Databricks — not throughput
at this size. Claiming otherwise would be the kind of thing this repo's quality
gate exists to prevent.
