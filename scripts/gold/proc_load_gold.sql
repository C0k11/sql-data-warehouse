/*
===============================================================================
Proc: gold.load_gold - populate dim_date and apply SCD2 to dim_customers_scd2
===============================================================================
SCD2 is applied as expire-then-insert (two set-based statements) instead of
MERGE: same effect, no MERGE concurrency/duplicate-action pitfalls, and each
step is independently testable. Change detection = SHA2_256 hash over tracked
attributes (a NULL-safe, column-order-stable CONCAT_WS).

Repeated runs with unchanged source are no-ops (idempotent).
Usage: EXEC gold.load_gold;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE gold.load_gold
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id INT, @t0 DATETIME2(3), @now DATETIME2(3) = SYSDATETIME();
    DECLARE @expired INT, @inserted INT;
    INSERT INTO etl.load_run (layer) VALUES ('gold');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        PRINT '=== Loading Gold Layer (run ' + CAST(@run_id AS VARCHAR) + ') ===';

        ------------------------------------------------------------------
        -- gold.dim_date: cover the full span of fact dates, rebuild-safe
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        DECLARE @d_min DATE, @d_max DATE;
        SELECT @d_min = MIN(v), @d_max = MAX(v)
        FROM silver.crm_sales_details
        CROSS APPLY (VALUES (sls_order_dt), (sls_ship_dt), (sls_due_dt)) AS x(v)
        WHERE v IS NOT NULL;

        IF @d_min IS NULL
            THROW 50003, 'gold.load_gold: silver.crm_sales_details has no parseable dates - load silver first.', 1;

        TRUNCATE TABLE gold.dim_date;
        ;WITH seq AS (
            SELECT TOP (DATEDIFF(DAY, @d_min, @d_max) + 1)
                   DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, @d_min) AS d
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO gold.dim_date
            (date_key, full_date, year, quarter, month, month_name, day, weekday_name, is_weekend)
        SELECT
            CONVERT(INT, CONVERT(VARCHAR(8), d, 112)),
            d,
            YEAR(d),
            DATEPART(QUARTER, d),
            MONTH(d),
            DATENAME(MONTH, d),
            DAY(d),
            DATENAME(WEEKDAY, d),
            -- day-count modulo, immune to @@DATEFIRST/login language
            -- (1900-01-01 was a Monday: offset 5 = Saturday, 6 = Sunday)
            CASE WHEN DATEDIFF(DAY, '19000101', d) % 7 IN (5, 6) THEN 1 ELSE 0 END
        FROM seq;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'gold.dim_date', @@ROWCOUNT, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- gold.dim_customers_scd2: integrate sources, hash, expire, insert
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();

        -- Integrated current-state customer snapshot (CRM master + ERP enrich)
        SELECT
            ci.cst_id                              AS customer_id,
            ci.cst_key                             AS customer_number,
            ci.cst_firstname                       AS first_name,
            ci.cst_lastname                        AS last_name,
            ISNULL(la.cntry, 'n/a')                AS country,
            ci.cst_marital_status                  AS marital_status,
            CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM is master for gender
                 ELSE COALESCE(ca.gen, 'n/a')
            END                                    AS gender,
            ca.bdate                               AS birthdate,
            ci.cst_create_date                     AS create_date,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT_WS('|',
                ci.cst_key,
                ISNULL(ci.cst_firstname, ''),
                ISNULL(ci.cst_lastname, ''),
                ISNULL(la.cntry, 'n/a'),
                ci.cst_marital_status,
                CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ELSE COALESCE(ca.gen, 'n/a') END,
                ISNULL(CONVERT(VARCHAR(10), ca.bdate, 120), ''),
                ISNULL(CONVERT(VARCHAR(10), ci.cst_create_date, 120), '')
            )))                                    AS attr_hash
        INTO #src
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101  la ON ci.cst_key = la.cid;

        -- 1) expire current rows whose attributes changed or that left the source
        UPDATE d
        SET d.valid_to = @now, d.is_current = 0
        FROM gold.dim_customers_scd2 d
        LEFT JOIN #src s ON d.customer_id = s.customer_id
        WHERE d.is_current = 1
          AND (s.customer_id IS NULL OR d.attr_hash <> s.attr_hash);
        SET @expired = @@ROWCOUNT;

        -- 2) insert a fresh current row for new customers and changed customers
        INSERT INTO gold.dim_customers_scd2 (
            customer_id, customer_number, first_name, last_name, country,
            marital_status, gender, birthdate, create_date, attr_hash,
            valid_from, valid_to, is_current
        )
        SELECT
            s.customer_id, s.customer_number, s.first_name, s.last_name, s.country,
            s.marital_status, s.gender, s.birthdate, s.create_date, s.attr_hash,
            -- first-ever version is backdated so facts that predate the
            -- warehouse still hit a version in as-of (point-in-time) joins;
            -- subsequent versions start at the load that observed the change
            CASE WHEN EXISTS (
                     SELECT 1 FROM gold.dim_customers_scd2 d2
                     WHERE d2.customer_id = s.customer_id
                 ) THEN @now
                 ELSE '1900-01-01'
            END,
            '9999-12-31', 1
        FROM #src s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_customers_scd2 d
            WHERE d.customer_id = s.customer_id AND d.is_current = 1
        );
        SET @inserted = @@ROWCOUNT;

        DROP TABLE #src;

        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'gold.dim_customers_scd2 (expired=' + CAST(@expired AS VARCHAR) + ')',
                @inserted, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));
        PRINT CONCAT('>> SCD2 customers: expired ', @expired, ', inserted ', @inserted);

        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'success'
        WHERE run_id = @run_id;
        PRINT '=== Gold Layer loaded ===';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'failed', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id;
        THROW;
    END CATCH
END;
GO

PRINT 'gold.load_gold created.';
GO
