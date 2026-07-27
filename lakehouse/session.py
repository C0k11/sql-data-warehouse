"""SparkSession factory: one code path for a laptop and for Databricks.

On Databricks the session already exists and Unity Catalog manages storage, so
`get_spark()` just grabs it. Locally we build an equivalent session with the
open-source Delta Lake packages, which gives the same SQL surface (MERGE,
identity columns, time travel) against plain files under `db/lakehouse`.

Everything downstream addresses tables as `<catalog>.<schema>.<table>`, so the
only environment-specific value is the catalog name.
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent

# Unity Catalog exposes `workspace` on Databricks Free Edition; the local
# session uses Spark's built-in catalog. Override with LAKEHOUSE_CATALOG.
DEFAULT_DATABRICKS_CATALOG = "workspace"
DEFAULT_LOCAL_CATALOG = "spark_catalog"

SCHEMAS = ("bronze", "silver", "gold", "etl")


def is_databricks() -> bool:
    """True when running inside a Databricks runtime (notebook or job)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _check_java_home() -> None:
    """Fail loudly on a stale JAVA_HOME instead of Spark's opaque launcher error.

    PySpark prefers JAVA_HOME over the JVM on PATH, so a JAVA_HOME left over
    from a deleted install makes the driver die with nothing but
    "The system cannot find the path specified."
    """
    java_home = os.environ.get("JAVA_HOME")
    if java_home and not (Path(java_home) / "bin" / "java.exe").exists() \
            and not (Path(java_home) / "bin" / "java").exists():
        raise RuntimeError(
            f"JAVA_HOME points at a directory with no java executable: {java_home}\n"
            "Unset it or point it at a real JDK 17/21 install."
        )


def catalog_name() -> str:
    env = os.environ.get("LAKEHOUSE_CATALOG")
    if env:
        return env
    return DEFAULT_DATABRICKS_CATALOG if is_databricks() else DEFAULT_LOCAL_CATALOG


def get_spark(app_name: str = "medallion-lakehouse") -> SparkSession:
    """Return a Delta-enabled SparkSession for the current environment."""
    if is_databricks():
        return SparkSession.builder.getOrCreate()

    _check_java_home()
    from delta import configure_spark_with_delta_pip

    # All local state (Delta tables, Hive metastore, resolved jars) lives under
    # one overridable root. Point LAKEHOUSE_STATE_DIR at a native Linux path
    # when running under WSL: Delta's transaction log needs atomic rename, and
    # a Windows drive mounted through DrvFs is not a filesystem to bet that on.
    state = Path(os.environ.get("LAKEHOUSE_STATE_DIR", ROOT / "db"))
    warehouse = state / "lakehouse"
    ivy = Path(os.environ.get("SPARK_IVY_DIR", state / "ivy"))
    for d in (warehouse, ivy):
        d.mkdir(parents=True, exist_ok=True)

    # enableHiveSupport is what makes the catalog survive the process. Without
    # it Spark defaults to an in-memory catalog: the Delta files are written
    # correctly but every table definition dies with the driver, so a second
    # process (the parity fingerprinter, the test suite) sees an empty catalog
    # sitting on top of a full warehouse directory. Databricks has Unity
    # Catalog for this; locally it is a Derby metastore under the state dir.
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .enableHiveSupport()
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse.as_posix())
        .config("spark.jars.ivy", ivy.as_posix())
        # Needs the spark.hadoop. prefix to reach the Hive conf; without it the
        # setting is ignored and Derby drops metastore_db in the working dir.
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={(state / 'metastore_db').as_posix()};create=true")
        # 116k rows: the default 200 shuffle partitions would emit hundreds of
        # tiny files and dominate the runtime with task scheduling overhead.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Match Databricks: unqualified writes go to Delta, not Parquet.
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def qualify(schema: str, table: str) -> str:
    """Fully-qualified table name for the active catalog."""
    return f"{catalog_name()}.{schema}.{table}"


def ensure_schemas(spark: SparkSession) -> None:
    cat = catalog_name()
    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema}")
