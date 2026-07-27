"""Run the declarative checks, persist the outcomes, gate the pipeline."""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .quality_checks import Check, checks
from .session import catalog_name, qualify


class QualityGateFailed(RuntimeError):
    pass


def run_quality_checks(spark: SparkSession, run_id: int, layer: str | None = None) -> list[tuple]:
    """Execute every check for `layer` (all layers when None). Raise on failure."""
    selected: list[Check] = [c for c in checks(catalog_name()) if layer in (None, c.layer)]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows, failed, warned, passed = [], 0, 0, 0

    for c in selected:
        violations = spark.sql(f"SELECT count(*) AS n FROM ({c.sql}) q").first()["n"]
        if violations == 0:
            status, passed = "pass", passed + 1
        elif c.severity == "warn":
            status, warned = "warn", warned + 1
            print(f"    WARN  {c.name}: {violations:,} violations")
        else:
            status, failed = "fail", failed + 1
            print(f"    FAIL  {c.name}: {violations:,} violations")
        rows.append((run_id, c.layer, c.table, c.name, c.severity, int(violations), status, now))

    spark.createDataFrame(
        rows,
        "run_id BIGINT, layer STRING, table_name STRING, check_name STRING, "
        "severity STRING, violations BIGINT, status STRING, checked_at TIMESTAMP",
    ).write.mode("append").saveAsTable(qualify("etl", "quality_result"))

    scope = layer or "all layers"
    print(f"    === {scope}: {passed} passed, {warned} warned, {failed} failed ===")
    if failed:
        raise QualityGateFailed(
            f"Data quality gate failed: {failed} error-severity check(s) violated."
        )
    return rows
