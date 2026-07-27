"""Orchestrator: build the medallion lakehouse end to end.

Runs bronze -> reconcile -> silver -> silver gate -> gold -> gold gate. Any
failure exits nonzero, so this is CI-friendly. The same module runs unchanged
on Databricks (see notebooks/databricks_pipeline.py, which imports it).

Usage:
    python -m lakehouse.run_pipeline                  # incremental (default)
    python -m lakehouse.run_pipeline --steps all      # DESTRUCTIVE: drops SCD2 history
    python -m lakehouse.run_pipeline --layer silver   # stop after the silver gate
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from . import bronze, gold, silver
from .etl_framework import create_framework_tables, load_run, next_run_id
from .quality import QualityGateFailed, run_quality_checks
from .session import ROOT, catalog_name, ensure_schemas, get_spark, qualify


def drop_everything(spark: SparkSession) -> None:
    """Full reset. Erases SCD2 history, hence never the default."""
    cat = catalog_name()
    print("--> DESTRUCTIVE rebuild: dropping all schemas")
    for schema in ("bronze", "silver", "gold", "etl"):
        spark.sql(f"DROP SCHEMA IF EXISTS {cat}.{schema} CASCADE")


def print_summary(spark: SparkSession, run_ids: list[int]) -> None:
    if not run_ids:
        return
    ids = ", ".join(str(i) for i in run_ids)
    print("\n=== Load summary ===")
    for r in spark.sql(f"""
        SELECT d.table_name, d.rows_loaded, d.duration_ms, d.status
        FROM {qualify('etl', 'load_run_detail')} d
        WHERE d.run_id IN ({ids}) ORDER BY d.loaded_at
    """).collect():
        rows = "-" if r["rows_loaded"] is None else f"{r['rows_loaded']:,}"
        print(f"  {r['table_name']:<44} {rows:>10} rows  {r['duration_ms']:>6} ms  {r['status']}")

    res = spark.sql(f"""
        SELECT status, count(*) AS n FROM {qualify('etl', 'quality_result')}
        WHERE run_id IN ({ids}) GROUP BY status
    """).collect()
    if res:
        summary = ", ".join(f"{r['n']} {r['status']}" for r in sorted(res, key=lambda x: x["status"]))
        print(f"\n=== Quality checks: {summary} ===")
        for r in spark.sql(f"""
            SELECT check_name, violations, status FROM {qualify('etl', 'quality_result')}
            WHERE run_id IN ({ids}) AND status != 'pass' ORDER BY status, check_name
        """).collect():
            print(f"  {r['status'].upper():<5} {r['check_name']} ({r['violations']:,} violations)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", default="load", choices=["load", "all"],
                        help="all = drop everything first (DESTRUCTIVE: erases SCD2 history)")
    parser.add_argument("--layer", default="gold", choices=["bronze", "silver", "gold"],
                        help="highest layer to build")
    parser.add_argument("--datasets", default=str(ROOT / "datasets"),
                        help="directory holding source_crm/ and source_erp/")
    parser.add_argument("--skip-bronze", action="store_true",
                        help="re-derive silver and gold from the existing bronze tables "
                             "instead of reloading the files (tests that mutate bronze "
                             "need this, or the reload would erase the mutation)")
    args = parser.parse_args(argv)

    datasets = Path(args.datasets)
    if not (datasets / "source_crm").is_dir():
        print(f"No source_crm/ under {datasets}", file=sys.stderr)
        return 2

    t0 = time.perf_counter()
    spark = get_spark()
    print(f"--> Spark {spark.version} | catalog {catalog_name()} | datasets {datasets}")

    run_ids: list[int] = []
    try:
        if args.steps == "all":
            drop_everything(spark)
        ensure_schemas(spark)
        create_framework_tables(spark)

        if args.skip_bronze:
            print("--> skipping bronze (reusing existing tables)")
        else:
            rid = next_run_id(spark)
            run_ids.append(rid)
            with load_run(spark, "bronze", rid) as run:
                bronze.load_bronze(spark, datasets, run)
            bronze.reconcile(spark, datasets)
        if args.layer == "bronze":
            print_summary(spark, run_ids)
            print(f"\nPipeline finished: SUCCESS in {time.perf_counter() - t0:.1f}s")
            return 0

        rid = next_run_id(spark)
        run_ids.append(rid)
        with load_run(spark, "silver", rid) as run:
            silver.load_silver(spark, run)
        print("--> silver quality gate")
        run_quality_checks(spark, rid, layer="silver")
        if args.layer == "silver":
            print_summary(spark, run_ids)
            print(f"\nPipeline finished: SUCCESS in {time.perf_counter() - t0:.1f}s")
            return 0

        # Gate silver BEFORE gold mutates the persistent SCD2 dimension: a bad
        # silver load would otherwise write a spurious version of every row.
        rid = next_run_id(spark)
        run_ids.append(rid)
        with load_run(spark, "gold", rid) as run:
            gold.load_gold(spark, run)
        print("--> gold quality gate")
        run_quality_checks(spark, rid, layer="gold")

        print_summary(spark, run_ids)
        print(f"\nPipeline finished: SUCCESS in {time.perf_counter() - t0:.1f}s")
        return 0
    except Exception as exc:
        print(f"\nPipeline FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        try:
            print_summary(spark, run_ids)
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
