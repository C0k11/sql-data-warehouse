"""Gold: star schema with a Type-2 customer dimension.

Two deliberate divergences from the SQL Server build, both because the engine
changed rather than because the design did:

1. SCD2 uses MERGE. The T-SQL version avoids MERGE on purpose - SQL Server's
   implementation has a long history of concurrency and duplicate-action bugs -
   and hand-rolls expire-then-insert instead. Delta's MERGE is a core, heavily
   exercised primitive, and WHEN NOT MATCHED BY SOURCE expresses "this customer
   disappeared from the source" as one clause instead of a LEFT JOIN.

2. The uniqueness guarantee moves. SQL Server enforces "at most one current row
   per customer" with a filtered unique index, which is a hard constraint the
   engine will not let you violate. Delta has no unique indexes, so that
   invariant is only checked after the fact by the quality gate. This is a real
   loss of strength, not a translation detail, and it is stated as such in the
   README instead of being papered over.

The change-detection hash is SHA-256 over the same attributes in the same
order, but the digests are NOT comparable across engines: T-SQL hashes the
UTF-16 bytes of NVARCHAR while Spark hashes UTF-8. Each side is internally
consistent, which is all change detection needs.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .etl_framework import LoadRun
from .session import catalog_name, qualify

SCD2_DDL = """
CREATE TABLE IF NOT EXISTS {t} (
    -- Assigned explicitly, not GENERATED ALWAYS AS IDENTITY: identity columns
    -- are a Databricks/Unity Catalog feature and open-source Delta rejects them
    -- outright (UNSUPPORTED_FEATURE.TABLE_OPERATION), so the table would create
    -- on Databricks and fail in CI. Keys stay stable because they are only ever
    -- appended above the current maximum -- the same guarantee IDENTITY gives,
    -- and the same single-writer assumption run_id allocation already makes.
    customer_sk     BIGINT    NOT NULL,
    customer_id     INT       NOT NULL,
    customer_number STRING    NOT NULL,
    first_name      STRING,
    last_name       STRING,
    country         STRING    NOT NULL,
    marital_status  STRING    NOT NULL,
    gender          STRING    NOT NULL,
    birthdate       DATE,
    create_date     DATE,
    attr_hash       STRING    NOT NULL,
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP NOT NULL,
    is_current      BOOLEAN   NOT NULL
) USING DELTA
"""

# Integrated current-state snapshot: CRM is master, ERP enriches.
SRC_SNAPSHOT = """
SELECT
    ci.cst_id                                  AS customer_id,
    ci.cst_key                                 AS customer_number,
    ci.cst_firstname                           AS first_name,
    ci.cst_lastname                            AS last_name,
    coalesce(la.cntry, 'n/a')                  AS country,
    ci.cst_marital_status                      AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
         ELSE coalesce(ca.gen, 'n/a') END      AS gender,
    ca.bdate                                   AS birthdate,
    ci.cst_create_date                         AS create_date,
    sha2(concat_ws('|',
        ci.cst_key,
        coalesce(ci.cst_firstname, ''),
        coalesce(ci.cst_lastname, ''),
        coalesce(la.cntry, 'n/a'),
        ci.cst_marital_status,
        CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ELSE coalesce(ca.gen, 'n/a') END,
        coalesce(CAST(ca.bdate AS STRING), ''),
        coalesce(CAST(ci.cst_create_date AS STRING), '')
    ), 256)                                    AS attr_hash
FROM {s}.crm_cust_info ci
LEFT JOIN {s}.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN {s}.erp_loc_a101  la ON ci.cst_key = la.cid
"""


def _load_dim_date(spark: SparkSession, cat: str, run: LoadRun) -> None:
    target = f"{cat}.gold.dim_date"
    with run.step(target) as box:
        span = spark.sql(f"""
            SELECT min(d) AS d_min, max(d) AS d_max FROM (
                SELECT explode(array(sls_order_dt, sls_ship_dt, sls_due_dt)) AS d
                FROM {cat}.silver.crm_sales_details
            ) WHERE d IS NOT NULL
        """).first()
        if span["d_min"] is None:
            raise RuntimeError(
                "gold: silver.crm_sales_details has no parseable dates - load silver first."
            )
        spark.sql(f"""
CREATE OR REPLACE TABLE {target} USING DELTA AS
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date)                                 AS year,
    quarter(full_date)                              AS quarter,
    month(full_date)                                AS month,
    date_format(full_date, 'MMMM')                  AS month_name,
    day(full_date)                                  AS day,
    date_format(full_date, 'EEEE')                  AS weekday_name,
    -- Spark's dayofweek is fixed at 1=Sunday..7=Saturday. The SQL Server build
    -- has to compute this as a day-count modulo because DATEPART(WEEKDAY) there
    -- shifts with @@DATEFIRST and the login's language.
    dayofweek(full_date) IN (1, 7)                  AS is_weekend
FROM (
    SELECT explode(sequence(DATE'{span['d_min']}', DATE'{span['d_max']}', INTERVAL 1 DAY)) AS full_date
)
""")
        box["rows"] = spark.table(target).count()


def _load_scd2(spark: SparkSession, cat: str, run: LoadRun) -> None:
    target = f"{cat}.gold.dim_customers_scd2"
    with run.step(target) as box:
        spark.sql(SCD2_DDL.format(t=target))
        spark.sql(SRC_SNAPSHOT.format(s=f"{cat}.silver")).createOrReplaceTempView("scd2_src")

        # A single load timestamp for the whole run keeps validity windows
        # contiguous: an expired row's valid_to must equal its successor's
        # valid_from, or as-of joins fall into gaps.
        now = spark.sql("SELECT current_timestamp() AS t").first()["t"]

        before_total = spark.table(target).count()
        before_current = spark.sql(
            f"SELECT count(*) AS n FROM {target} WHERE is_current"
        ).first()["n"]

        # 1) expire: attributes changed, or the customer left the source
        spark.sql(f"""
MERGE INTO {target} AS d
USING scd2_src AS s
   ON d.customer_id = s.customer_id AND d.is_current
 WHEN MATCHED AND d.attr_hash != s.attr_hash
      THEN UPDATE SET d.valid_to = TIMESTAMP'{now}', d.is_current = false
 WHEN NOT MATCHED BY SOURCE AND d.is_current
      THEN UPDATE SET d.valid_to = TIMESTAMP'{now}', d.is_current = false
""")

        # 2) insert a current row for new and changed customers.
        # The first-ever version is backdated so facts that predate the
        # warehouse still land inside a version during as-of joins; later
        # versions start at the load that observed the change.
        max_sk = spark.sql(
            f"SELECT COALESCE(MAX(customer_sk), 0) AS m FROM {target}"
        ).first()["m"]
        spark.sql(f"""
INSERT INTO {target}
    (customer_sk, customer_id, customer_number, first_name, last_name, country,
     marital_status, gender, birthdate, create_date, attr_hash,
     valid_from, valid_to, is_current)
SELECT
    {max_sk} + row_number() OVER (ORDER BY s.customer_id),
    s.customer_id, s.customer_number, s.first_name, s.last_name, s.country,
    s.marital_status, s.gender, s.birthdate, s.create_date, s.attr_hash,
    CASE WHEN EXISTS (SELECT 1 FROM {target} d2 WHERE d2.customer_id = s.customer_id)
         THEN TIMESTAMP'{now}' ELSE TIMESTAMP'1900-01-01 00:00:00' END,
    TIMESTAMP'9999-12-31 00:00:00',
    true
FROM scd2_src s
WHERE NOT EXISTS (
    SELECT 1 FROM {target} d WHERE d.customer_id = s.customer_id AND d.is_current
)
""")
        after_total = spark.table(target).count()
        after_current = spark.sql(
            f"SELECT count(*) AS n FROM {target} WHERE is_current"
        ).first()["n"]
        expired = spark.sql(
            f"SELECT count(*) AS n FROM {target} WHERE valid_to = TIMESTAMP'{now}'"
        ).first()["n"]
        # Counted as a delta, not as "rows stamped with this run's timestamp":
        # a customer's FIRST version is backdated to 1900-01-01 so that facts
        # predating the warehouse still land inside a version during as-of
        # joins. A valid_from = now predicate would therefore report 0 inserted
        # on the very first load, when in fact every row was new.
        inserted = after_total - before_total
        print(f"    >> SCD2: expired {expired:,}, inserted {inserted:,}, "
              f"current {before_current:,} -> {after_current:,}")
        # rows_loaded means "versions written by this run", matching what the
        # T-SQL loader records via @@ROWCOUNT. Recording the whole table size
        # here would turn the audit trail into a cumulative row count and make
        # this the only table whose audit row means something different.
        box["rows"] = inserted


def _create_views(spark: SparkSession, cat: str) -> None:
    spark.sql(f"""
CREATE OR REPLACE VIEW {cat}.gold.dim_customers AS
SELECT customer_sk AS customer_key, customer_id, customer_number, first_name,
       last_name, country, marital_status, gender, birthdate, create_date
FROM {cat}.gold.dim_customers_scd2
WHERE is_current
""")
    spark.sql(f"""
CREATE OR REPLACE VIEW {cat}.gold.dim_products AS
SELECT
    row_number() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,        -- NULL for cat_id CO_PE (absent from ERP)
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM {cat}.silver.crm_prd_info pn
LEFT JOIN {cat}.silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL             -- latest version of each product only
""")
    spark.sql(f"""
CREATE OR REPLACE VIEW {cat}.gold.fact_sales AS
SELECT
    sd.sls_ord_num                                    AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_cust_id                                    AS customer_id,
    CAST(date_format(sd.sls_order_dt, 'yyyyMMdd') AS INT) AS order_date_key,
    sd.sls_order_dt                                   AS order_date,
    sd.sls_ship_dt                                    AS shipping_date,
    sd.sls_due_dt                                     AS due_date,
    sd.sls_sales                                      AS sales_amount,
    sd.sls_quantity                                   AS quantity,
    sd.sls_price                                      AS price
FROM {cat}.silver.crm_sales_details sd
LEFT JOIN {cat}.gold.dim_products  pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN {cat}.gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
""")


def load_gold(spark: SparkSession, run: LoadRun) -> None:
    cat = catalog_name()
    _load_dim_date(spark, cat, run)
    _load_scd2(spark, cat, run)
    _create_views(spark, cat)
