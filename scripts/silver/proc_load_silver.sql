/*
===============================================================================
Proc: silver.load_silver - cleanse + standardize bronze into silver
===============================================================================
Cleansing rules (each backed by profiling of the raw files, see tools/):
  crm_cust_info   : drop 4 blank ids, dedupe 5 duplicate ids (6 surplus rows)
                    keeping the most recent cst_create_date, TRIM names
                    (39 untrimmed), decode marital/gender codes, blanks -> 'n/a'
  crm_prd_info    : split prd_key into cat_id + product key, cost NULL -> 0,
                    decode product line (380/397 untrimmed), repair the 200
                    rows where end < start by recomputing end = next start - 1
  crm_sales_details: integer yyyymmdd -> DATE via etl.try_yyyymmdd (19 malformed),
                    chained sales/price repair so sales = quantity * price holds
                    BY CONSTRUCTION; rows where neither measure is usable stay
                    NULL (monitored by a warn-level check) instead of being
                    fabricated as 0
  erp_cust_az12   : strip 'NAS' prefix, future birthdates -> NULL (16),
                    normalize 9 gender spellings
  erp_loc_a101    : strip '-' from cid, normalize 10+ country spellings
  erp_px_cat_g1v2 : TRIM passthrough

Every table load is timed + logged to etl.load_run_detail; errors re-thrown.
Usage: EXEC silver.load_silver;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id INT, @t0 DATETIME2(3), @rows INT;
    INSERT INTO etl.load_run (layer) VALUES ('silver');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        PRINT '=== Loading Silver Layer (run ' + CAST(@run_id AS VARCHAR) + ') ===';

        ------------------------------------------------------------------
        -- silver.crm_cust_info
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            NULLIF(TRIM(cst_firstname), '')                       AS cst_firstname,
            NULLIF(TRIM(cst_lastname), '')                        AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status))
                 WHEN 'S' THEN 'Single'
                 WHEN 'M' THEN 'Married'
                 ELSE 'n/a'
            END                                                   AS cst_marital_status,
            CASE UPPER(TRIM(cst_gndr))
                 WHEN 'F' THEN 'Female'
                 WHEN 'M' THEN 'Male'
                 ELSE 'n/a'
            END                                                   AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.crm_cust_info', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- silver.crm_prd_info
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line,
            prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')           AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key))                   AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0)                                   AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other Sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'n/a'
            END                                                   AS prd_line,
            prd_start_dt,
            -- source end dates are unreliable (200 rows end < start):
            -- rebuild validity windows from the version sequence itself
            DATEADD(DAY, -1,
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key ORDER BY prd_start_dt
                ))                                                AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.crm_prd_info', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- silver.crm_sales_details
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.crm_sales_details;
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- guarded parse: 0 / wrong length / 4-digit "years" / implausible -> NULL
            etl.try_yyyymmdd(sls_order_dt) AS sls_order_dt,
            etl.try_yyyymmdd(sls_ship_dt)  AS sls_ship_dt,
            etl.try_yyyymmdd(sls_due_dt)   AS sls_due_dt,
            -- chained repair: fix price first, then recompute sales from it,
            -- so sales = quantity * price can never be violated in silver.
            -- Repairing each from the other's RAW value breaks the identity
            -- (e.g. sales=100 qty=3 price=NULL -> price 33 but sales kept 100).
            CASE
                WHEN sls_sales > 0 AND sls_sales = sls_quantity * p.price_fixed
                    THEN sls_sales
                ELSE sls_quantity * p.price_fixed   -- NULL when price is unusable
            END AS sls_sales,
            sls_quantity,
            p.price_fixed AS sls_price
        FROM bronze.crm_sales_details
        CROSS APPLY (SELECT
            CASE
                WHEN sls_price > 0 THEN sls_price
                WHEN sls_sales > 0 AND sls_quantity > 0
                    THEN sls_sales / sls_quantity   -- derive from valid measures only
            END) AS p(price_fixed);
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.crm_sales_details', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- silver.erp_cust_az12
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > CAST(GETDATE() AS DATE) THEN NULL ELSE bdate END      AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.erp_cust_az12', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- silver.erp_loc_a101
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE'                THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA')      THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.erp_loc_a101', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        ------------------------------------------------------------------
        -- silver.erp_px_cat_g1v2
        ------------------------------------------------------------------
        SET @t0 = SYSDATETIME();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT TRIM(id), TRIM(cat), TRIM(subcat), TRIM(maintenance)
        FROM bronze.erp_px_cat_g1v2;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms)
        VALUES (@run_id, 'silver.erp_px_cat_g1v2', @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()));

        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'success'
        WHERE run_id = @run_id;
        PRINT '=== Silver Layer loaded ===';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'failed', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id;
        THROW;
    END CATCH
END;
GO

PRINT 'silver.load_silver created.';
GO
