"""Bronze: raw 1:1 landing of the source extracts as Delta tables.

The SQL Server build needs a landing zone that rewrites every extract as UTF-16
with LF endings, because BULK INSERT decodes UTF-8 with the host code page on
Windows and rejects the CODEPAGE option outright on Linux. Spark takes the
encoding as a read option and handles CRLF itself, so that whole stage
disappears here - the extracts are read exactly as they sit in the repo.

What does NOT disappear is the reconciliation the landing zone was protecting:
row counts are still verified against the raw files, because "the loader
silently dropped rows" is a failure mode of readers in general, not of BULK
INSERT in particular. It has already caught one real defect in this dataset
(three files ship without a trailing newline).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from .config import SOURCES
from .etl_framework import LoadRun
from .session import qualify

CORRUPT_COL = "_corrupt_record"


def load_bronze(spark: SparkSession, datasets: Path, run: LoadRun) -> None:
    for src in SOURCES:
        target = qualify("bronze", src.table)
        with run.step(target) as box:
            path = (datasets / src.rel_path).as_posix()
            df = (
                spark.read.format("csv")
                .schema(f"{src.schema}, {CORRUPT_COL} STRING")
                .option("header", "true")
                .option("encoding", "UTF-8")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", CORRUPT_COL)
                .load(path)
            )
            # PERMISSIVE turns unparseable rows into all-NULL rows instead of
            # failing, which row-count reconciliation cannot detect. Surface
            # them rather than letting them fold into the "n/a" buckets later.
            #
            # Every query below pairs the corrupt column with a real one on
            # purpose. Spark rejects any query over a raw CSV/JSON source whose
            # required schema is *only* the corrupt-record column
            # (UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN), so a bare
            # .select(CORRUPT_COL) or .count() here would replace this
            # diagnostic with an opaque AnalysisException at the exact moment
            # it is needed.
            anchor = src.schema.split(",")[0].split()[0]
            bad = df.filter(f"{CORRUPT_COL} IS NOT NULL").select(anchor, CORRUPT_COL)
            sample = bad.take(3)
            if sample:
                n_bad = bad.count()
                raise RuntimeError(
                    f"{src.rel_path}: {n_bad} unparseable row(s); first: "
                    + " | ".join(str(r[CORRUPT_COL]) for r in sample)
                )
            df = df.drop(CORRUPT_COL)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
            box["rows"] = spark.table(target).count()


def reconcile(spark: SparkSession, datasets: Path) -> None:
    """Fail if any bronze table's row count differs from its raw source file."""
    print("--> reconciling source files vs bronze")
    mismatches = []
    for src in SOURCES:
        raw = (datasets / src.rel_path).read_bytes().decode("utf-8-sig")
        expected = len(raw.splitlines()) - 1  # minus header
        actual = spark.table(qualify("bronze", src.table)).count()
        flag = "OK " if expected == actual else "MISMATCH"
        print(f"    {flag} {src.table:<20} file={expected:>7,}  bronze={actual:>7,}")
        if expected != actual:
            mismatches.append(f"{src.table} (file={expected}, bronze={actual})")
    if mismatches:
        raise RuntimeError("Bronze reconciliation failed: " + "; ".join(mismatches))
