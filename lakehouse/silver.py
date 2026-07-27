"""Silver: cleansed, standardized, typed tables.

Every rule here is a direct translation of silver.load_silver in the SQL Server
build, kept as SQL so the two can be read side by side. Three places where a
literal translation would have produced *different data* are called out inline:
NULL ordering, integer division, and the guarded yyyymmdd parse.

Each table is rebuilt with CREATE OR REPLACE TABLE ... AS SELECT, which Delta
commits atomically — the SQL Server equivalent (TRUNCATE then INSERT) leaves
the table empty for the duration of the load.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import MAX_YEAR, MIN_YEAR
from .etl_framework import LoadRun
from .session import qualify


def _try_yyyymmdd(col: str) -> str:
    """Guarded integer-yyyymmdd -> DATE.

    The guard is not optional. Handed the 4-digit value 5489, a bare date parse
    reads it as the *year* 5489 and succeeds — in the SQL Server build that one
    row inflated the generated date dimension to 1.27 million rows before the
    length check was added. Same trap here, same guard.
    """
    s = f"CAST({col} AS STRING)"
    # try_to_timestamp, not try_to_date: try_to_date is a Databricks-only
    # builtin and is absent from Apache Spark's FunctionRegistry, so it would
    # resolve on Databricks and die with UNRESOLVED_ROUTINE in CI. A plain
    # to_date is not an option either — Spark 4.0 enables ANSI mode by default,
    # so an unparseable value raises instead of yielding NULL.
    parsed = f"CAST(try_to_timestamp({s}, 'yyyyMMdd') AS DATE)"
    return (
        f"CASE WHEN {col} IS NULL OR {col} <= 0 THEN NULL "
        f"     WHEN length({s}) != 8 THEN NULL "
        f"     WHEN year({parsed}) NOT BETWEEN {MIN_YEAR} AND {MAX_YEAR} THEN NULL "
        f"     ELSE {parsed} END"
    )


def _statements(cat: str) -> dict[str, str]:
    b = f"{cat}.bronze"
    s = f"{cat}.silver"
    return {
        # ------------------------------------------------------------------
        # crm_cust_info: drop blank ids, keep the newest row per id, decode codes
        # ------------------------------------------------------------------
        "crm_cust_info": f"""
CREATE OR REPLACE TABLE {s}.crm_cust_info USING DELTA AS
SELECT
    cst_id,
    cst_key,
    nullif(trim(cst_firstname), '')                       AS cst_firstname,
    nullif(trim(cst_lastname),  '')                       AS cst_lastname,
    CASE upper(trim(cst_marital_status))
         WHEN 'S' THEN 'Single' WHEN 'M' THEN 'Married' ELSE 'n/a' END
                                                          AS cst_marital_status,
    CASE upper(trim(cst_gndr))
         WHEN 'F' THEN 'Female' WHEN 'M' THEN 'Male' ELSE 'n/a' END
                                                          AS cst_gndr,
    cst_create_date,
    current_timestamp()                                   AS dwh_create_date
FROM (
    SELECT *,
           -- NULLS LAST is load-bearing, not decoration: SQL Server sorts NULL
           -- as the lowest value (so DESC puts it last) while Spark defaults to
           -- NULLS FIRST on DESC. Without it, a customer whose newest row has a
           -- NULL create_date would win dedupe here and lose it there.
           row_number() OVER (
               PARTITION BY cst_id ORDER BY cst_create_date DESC NULLS LAST
           ) AS rn
    FROM {b}.crm_cust_info
    WHERE cst_id IS NOT NULL
)
WHERE rn = 1
""",
        # ------------------------------------------------------------------
        # crm_prd_info: split the composite key, rebuild validity windows
        # ------------------------------------------------------------------
        "crm_prd_info": f"""
CREATE OR REPLACE TABLE {s}.crm_prd_info USING DELTA AS
SELECT
    p.prd_id,
    replace(substring(p.prd_key, 1, 5), '-', '_')         AS cat_id,
    substring(p.prd_key, 7)                               AS prd_key,
    p.prd_nm,
    coalesce(p.prd_cost, 0)                               AS prd_cost,
    CASE upper(trim(p.prd_line))
         WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales' WHEN 'T' THEN 'Touring' ELSE 'n/a' END
                                                          AS prd_line,
    p.prd_start_dt,
    -- source end dates are unreliable (200 rows end before they start), so the
    -- validity window is rebuilt from the version sequence itself.
    -- p.prd_key is qualified for readability, not for correctness: Spark
    -- resolves a local column reference BEFORE a lateral column alias, so an
    -- unqualified prd_key would bind to the source column here anyway, exactly
    -- as it does in the T-SQL original. The qualifier just makes it obvious
    -- which prd_key is meant when two of them are in scope.
    date_sub(
        lead(p.prd_start_dt) OVER (PARTITION BY p.prd_key ORDER BY p.prd_start_dt),
        1
    )                                                     AS prd_end_dt,
    current_timestamp()                                   AS dwh_create_date
FROM {b}.crm_prd_info p
""",
        # ------------------------------------------------------------------
        # crm_sales_details: guarded date parse + chained measure repair
        # ------------------------------------------------------------------
        "crm_sales_details": f"""
CREATE OR REPLACE TABLE {s}.crm_sales_details USING DELTA AS
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    {_try_yyyymmdd('sls_order_dt')}                       AS sls_order_dt,
    {_try_yyyymmdd('sls_ship_dt')}                        AS sls_ship_dt,
    {_try_yyyymmdd('sls_due_dt')}                         AS sls_due_dt,
    -- chained repair: price is settled first, then sales is recomputed from it,
    -- so sales = quantity * price holds BY CONSTRUCTION. Repairing each from
    -- the other's raw value breaks the identity (sales=100 qty=3 price=NULL
    -- would yield price 33 while keeping sales 100).
    CASE WHEN sls_sales > 0 AND sls_sales = sls_quantity * price_fixed
              THEN sls_sales
         ELSE sls_quantity * price_fixed END              AS sls_sales,
    sls_quantity,
    price_fixed                                           AS sls_price,
    current_timestamp()                                   AS dwh_create_date
FROM (
    SELECT *,
           CASE WHEN sls_price > 0 THEN sls_price
                -- CAST(... AS INT), not plain division: both operands are INT
                -- so SQL Server truncates, while Spark's / promotes to DOUBLE.
                -- Left as-is, every derived price would carry a fraction and
                -- the sales-math quality check would fail on rounding alone.
                WHEN sls_sales > 0 AND sls_quantity > 0
                     THEN CAST(sls_sales / sls_quantity AS INT)
           END AS price_fixed
    FROM {b}.crm_sales_details
)
""",
        # ------------------------------------------------------------------
        # erp_cust_az12: strip the NAS prefix, drop future birthdates
        # ------------------------------------------------------------------
        "erp_cust_az12": f"""
CREATE OR REPLACE TABLE {s}.erp_cust_az12 USING DELTA AS
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN substring(cid, 4) ELSE cid END AS cid,
    CASE WHEN bdate > current_date() THEN NULL ELSE bdate END     AS bdate,
    CASE WHEN upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN upper(trim(gen)) IN ('M', 'MALE')   THEN 'Male'
         ELSE 'n/a' END                                           AS gen,
    current_timestamp()                                           AS dwh_create_date
FROM {b}.erp_cust_az12
""",
        # ------------------------------------------------------------------
        # erp_loc_a101: normalize the key to match cst_key, normalize countries
        # ------------------------------------------------------------------
        "erp_loc_a101": f"""
CREATE OR REPLACE TABLE {s}.erp_loc_a101 USING DELTA AS
SELECT
    replace(cid, '-', '')                                 AS cid,
    CASE WHEN trim(cntry) = 'DE'              THEN 'Germany'
         WHEN trim(cntry) IN ('US', 'USA')    THEN 'United States'
         WHEN cntry IS NULL OR trim(cntry) = '' THEN 'n/a'
         ELSE trim(cntry) END                             AS cntry,
    current_timestamp()                                   AS dwh_create_date
FROM {b}.erp_loc_a101
""",
        # ------------------------------------------------------------------
        # erp_px_cat_g1v2: trim passthrough
        # ------------------------------------------------------------------
        "erp_px_cat_g1v2": f"""
CREATE OR REPLACE TABLE {s}.erp_px_cat_g1v2 USING DELTA AS
SELECT trim(id) AS id, trim(cat) AS cat, trim(subcat) AS subcat,
       trim(maintenance) AS maintenance, current_timestamp() AS dwh_create_date
FROM {b}.erp_px_cat_g1v2
""",
    }


def load_silver(spark: SparkSession, run: LoadRun) -> None:
    from .session import catalog_name

    for table, sql in _statements(catalog_name()).items():
        target = qualify("silver", table)
        with run.step(target) as box:
            spark.sql(sql)
            box["rows"] = spark.table(target).count()
