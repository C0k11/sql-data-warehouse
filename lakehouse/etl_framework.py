"""Pipeline observability: run + per-table audit logs, persisted as Delta.

Differs deliberately from the SQL Server implementation in one place. There,
every table load INSERTs its own audit row as it finishes — cheap on an OLTP
engine. On Spark each append is a separate transaction that commits a new file,
so row-at-a-time logging would add seconds of commit overhead to a pipeline
whose actual work takes seconds. Details are therefore buffered in the driver
and flushed once per run, which keeps the audit trail identical in content
while costing one commit instead of six.

Failures still land in the log: the context manager flushes on the way out of
an exception before re-raising.
"""
from __future__ import annotations

import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .session import qualify

RUN_SCHEMA = (
    "run_id BIGINT, layer STRING, started_at TIMESTAMP, finished_at TIMESTAMP, "
    "status STRING, error_message STRING"
)
DETAIL_SCHEMA = (
    "run_id BIGINT, table_name STRING, rows_loaded BIGINT, duration_ms BIGINT, "
    "status STRING, error_message STRING, loaded_at TIMESTAMP"
)
QUALITY_SCHEMA = (
    "run_id BIGINT, layer STRING, table_name STRING, check_name STRING, "
    "severity STRING, violations BIGINT, status STRING, checked_at TIMESTAMP"
)


def create_framework_tables(spark: SparkSession) -> None:
    for name, schema in (
        ("load_run", RUN_SCHEMA),
        ("load_run_detail", DETAIL_SCHEMA),
        ("quality_result", QUALITY_SCHEMA),
    ):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {qualify('etl', name)} ({schema}) USING DELTA")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def next_run_id(spark: SparkSession) -> int:
    """Allocate a run id.

    Delta has no sequences, and an IDENTITY column would force an insert before
    the run's outcome is known. max+1 is sufficient here: the pipeline is a
    single-writer batch job. Concurrent writers would need a different scheme,
    which is called out in the README rather than pretended away.
    """
    row = spark.sql(f"SELECT COALESCE(MAX(run_id), 0) + 1 AS n FROM {qualify('etl', 'load_run')}").first()
    return int(row["n"])


class LoadRun:
    """Buffers per-table audit rows for one layer, flushes them on exit."""

    def __init__(self, spark: SparkSession, layer: str, run_id: int):
        self.spark = spark
        self.layer = layer
        self.run_id = run_id
        self.started_at = _now()
        self._details: list[tuple] = []

    def record(self, table_name: str, rows_loaded: int | None, duration_ms: int,
               status: str = "success", error_message: str | None = None) -> None:
        self._details.append(
            (self.run_id, table_name, rows_loaded, duration_ms, status, error_message, _now())
        )
        rows = "-" if rows_loaded is None else f"{rows_loaded:,}"
        print(f"    {table_name:<28} {rows:>10} rows  {duration_ms:>6} ms")

    @contextmanager
    def step(self, table_name: str):
        """Time one table load; on failure log it as failed and re-raise."""
        t0 = time.perf_counter()
        box: dict[str, int | None] = {"rows": None}
        try:
            yield box
        except Exception as exc:
            self.record(table_name, None, int((time.perf_counter() - t0) * 1000),
                        status="failed", error_message=f"{type(exc).__name__}: {exc}")
            raise
        self.record(table_name, box["rows"], int((time.perf_counter() - t0) * 1000))

    def _flush(self, status: str, error_message: str | None) -> None:
        if self._details:
            self.spark.createDataFrame(self._details, DETAIL_SCHEMA) \
                .write.mode("append").saveAsTable(qualify("etl", "load_run_detail"))
        self.spark.createDataFrame(
            [(self.run_id, self.layer, self.started_at, _now(), status, error_message)],
            RUN_SCHEMA,
        ).write.mode("append").saveAsTable(qualify("etl", "load_run"))


@contextmanager
def load_run(spark: SparkSession, layer: str, run_id: int):
    """Run one layer under audit. Logs and re-raises on failure."""
    run = LoadRun(spark, layer, run_id)
    print(f"=== Loading {layer} layer (run {run_id}) ===")
    try:
        yield run
    except Exception as exc:
        run._flush("failed", f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}")
        raise
    run._flush("success", None)
    print(f"=== {layer} layer loaded ===")
