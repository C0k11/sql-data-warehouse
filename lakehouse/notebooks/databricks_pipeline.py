# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion lakehouse - Databricks run
# MAGIC
# MAGIC Runs the exact same `lakehouse` package that runs locally and in CI. The
# MAGIC only Databricks-specific work happens in this notebook: staging the CSV
# MAGIC extracts into a Unity Catalog Volume, because Spark on serverless reads
# MAGIC Volumes natively while Workspace files are not a Spark-readable path.
# MAGIC
# MAGIC **Setup:** clone this repo as a Git folder (Workspace -> Create -> Git folder),
# MAGIC then open this notebook from inside it.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog")
dbutils.widgets.text("volume_schema", "landing", "Schema holding the volume")
dbutils.widgets.text("volume", "raw", "Volume name")

CATALOG = dbutils.widgets.get("catalog")
VOL_SCHEMA = dbutils.widgets.get("volume_schema")
VOLUME = dbutils.widgets.get("volume")
VOLUME_PATH = f"/Volumes/{CATALOG}/{VOL_SCHEMA}/{VOLUME}"

import os
import sys

# The Git folder holding this notebook is the repo root's parent of `lakehouse`.
REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get()
)))
REPO_FS = f"/Workspace{REPO}"
sys.path.insert(0, REPO_FS)
os.environ["LAKEHOUSE_CATALOG"] = CATALOG

print(f"repo    : {REPO_FS}")
print(f"catalog : {CATALOG}")
print(f"volume  : {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. Stage the extracts into a Volume
# MAGIC
# MAGIC Workspace files are not a Spark-readable path on serverless compute, so the
# MAGIC six CSVs are copied into a managed Volume once. Re-running is harmless.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{VOL_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOL_SCHEMA}.{VOLUME}")

import shutil
from pathlib import Path

staged = 0
for rel in ("source_crm", "source_erp"):
    src_dir = Path(REPO_FS) / "datasets" / rel
    dst_dir = Path(VOLUME_PATH) / rel
    dst_dir.mkdir(parents=True, exist_ok=True)
    for csv in sorted(src_dir.glob("*.csv")):
        shutil.copyfile(csv, dst_dir / csv.name)
        staged += 1
        print(f"  staged {rel}/{csv.name}  ({csv.stat().st_size:,} bytes)")
print(f"--> {staged} files in {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md ## 3. Build the warehouse
# MAGIC
# MAGIC `--steps all` drops and rebuilds everything, including SCD2 history. Use the
# MAGIC default (`load`) on subsequent runs so the dimension keeps versioning.

# COMMAND ----------

from lakehouse.run_pipeline import main

exit_code = main(["--datasets", VOLUME_PATH, "--steps", "all"])
assert exit_code == 0, f"pipeline failed with exit code {exit_code}"

# COMMAND ----------

# MAGIC %md ## 4. Verify SCD2 versioning
# MAGIC
# MAGIC Mutate one customer in bronze, re-run silver + gold, and confirm the
# MAGIC dimension grew a second version with contiguous validity windows.

# COMMAND ----------

spark.sql(f"""
UPDATE {CATALOG}.bronze.crm_cust_info
SET cst_marital_status = CASE WHEN cst_marital_status = 'S' THEN 'M' ELSE 'S' END
WHERE cst_id = 11000
""")

# --skip-bronze is load-bearing: without it the default run reloads bronze from
# the CSVs in the Volume, overwriting the UPDATE above before silver ever reads
# it. The snapshot would come out unchanged, the hash would match, and the
# MERGE would expire nothing - the test would silently "pass" by doing nothing.
main(["--datasets", VOLUME_PATH, "--skip-bronze"])

display(spark.sql(f"""
SELECT customer_sk, customer_id, marital_status, valid_from, valid_to, is_current
FROM {CATALOG}.gold.dim_customers_scd2
WHERE customer_id = 11000 ORDER BY valid_from
"""))

# COMMAND ----------

# MAGIC %md ## 5. Audit trail and quality results

# COMMAND ----------

display(spark.sql(f"""
SELECT r.run_id, r.layer, r.status, d.table_name, d.rows_loaded, d.duration_ms
FROM {CATALOG}.etl.load_run r
JOIN {CATALOG}.etl.load_run_detail d USING (run_id)
ORDER BY r.run_id DESC, d.loaded_at
LIMIT 40
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT status, count(*) AS checks
FROM {CATALOG}.etl.quality_result
WHERE run_id = (SELECT max(run_id) FROM {CATALOG}.etl.quality_result)
GROUP BY status ORDER BY status
"""))

# COMMAND ----------

# MAGIC %md ## 6. Point-in-time (as-of) join
# MAGIC
# MAGIC The reason the dimension keeps history: attribute each sale to the customer
# MAGIC as they were on the order date, not as they are today.

# COMMAND ----------

display(spark.sql(f"""
SELECT f.order_date, count(*) AS orders, sum(f.sales_amount) AS revenue,
       d.country, d.marital_status
FROM {CATALOG}.gold.fact_sales f
JOIN {CATALOG}.gold.dim_customers_scd2 d
  ON d.customer_id = f.customer_id
 AND CAST(f.order_date AS TIMESTAMP) >= d.valid_from
 AND CAST(f.order_date AS TIMESTAMP) <  d.valid_to
GROUP BY f.order_date, d.country, d.marital_status
ORDER BY f.order_date DESC
LIMIT 20
"""))
