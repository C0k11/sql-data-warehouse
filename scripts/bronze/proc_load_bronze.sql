/*
===============================================================================
Proc: bronze.load_bronze - full reload of all bronze tables from CSV files
===============================================================================
Differences vs. a naive loader:
    - File root comes from etl.config('data_root'), so the same proc works on
      any machine / CI container without editing code (BULK INSERT requires a
      literal path, hence dynamic SQL).
    - Every table load is timed and logged to etl.load_run / load_run_detail.
    - Errors are logged AND re-thrown: a failed load fails the pipeline
      instead of printing and pretending success.

Usage: EXEC bronze.load_bronze;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_one_csv
    @table_name  SYSNAME,          -- e.g. 'bronze.crm_cust_info'
    @rel_path    NVARCHAR(200),    -- e.g. 'source_crm/cust_info.csv'
    @run_id      INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @data_root NVARCHAR(1000) =
        (SELECT config_value FROM etl.config WHERE config_key = 'data_root');
    DECLARE @file NVARCHAR(1200) = CONCAT(@data_root, '/', @rel_path);
    DECLARE @sql  NVARCHAR(MAX);
    DECLARE @t0   DATETIME2(3) = SYSDATETIME();
    DECLARE @rows INT;

    -- Landing files are UTF-16 (see the orchestrator): widechar is the only
    -- Unicode input mode BULK INSERT supports on BOTH platforms - Windows
    -- decodes UTF-8 with the legacy ACP unless CODEPAGE 65001 is forced, and
    -- Linux rejects the CODEPAGE option outright (error 16202) while
    -- defaulting to CP437. ROWTERMINATOR is the WIDE line feed (0x0A00);
    -- FORMAT='CSV' is omitted (no quoted fields in these extracts).
    BEGIN TRY
        SET @sql = CONCAT(
            N'TRUNCATE TABLE ', @table_name, N';',
            N'BULK INSERT ', @table_name,
            N' FROM ''', REPLACE(@file, '''', ''''''), N'''',
            N' WITH (DATAFILETYPE = ''widechar'', FIRSTROW = 2,',
            N' FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A00'', KEEPNULLS, TABLOCK);'
        );
        EXEC sp_executesql @sql;

        SET @sql = CONCAT(N'SELECT @c = COUNT(*) FROM ', @table_name, N';');
        EXEC sp_executesql @sql, N'@c INT OUTPUT', @c = @rows OUTPUT;

        INSERT INTO etl.load_run_detail (run_id, table_name, rows_loaded, duration_ms, status)
        VALUES (@run_id, @table_name, @rows, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()), 'success');
        PRINT CONCAT('>> ', @table_name, ': ', @rows, ' rows in ',
                     DATEDIFF(MILLISECOND, @t0, SYSDATETIME()), ' ms');
    END TRY
    BEGIN CATCH
        INSERT INTO etl.load_run_detail (run_id, table_name, duration_ms, status, error_message)
        VALUES (@run_id, @table_name, DATEDIFF(MILLISECOND, @t0, SYSDATETIME()), 'failed', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id INT;
    INSERT INTO etl.load_run (layer) VALUES ('bronze');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        PRINT '=== Loading Bronze Layer (run ' + CAST(@run_id AS VARCHAR) + ') ===';

        EXEC bronze.load_one_csv 'bronze.crm_cust_info',      'source_crm/cust_info.csv',     @run_id;
        EXEC bronze.load_one_csv 'bronze.crm_prd_info',       'source_crm/prd_info.csv',      @run_id;
        EXEC bronze.load_one_csv 'bronze.crm_sales_details',  'source_crm/sales_details.csv', @run_id;
        EXEC bronze.load_one_csv 'bronze.erp_cust_az12',      'source_erp/CUST_AZ12.csv',     @run_id;
        EXEC bronze.load_one_csv 'bronze.erp_loc_a101',       'source_erp/LOC_A101.csv',      @run_id;
        EXEC bronze.load_one_csv 'bronze.erp_px_cat_g1v2',    'source_erp/PX_CAT_G1V2.csv',   @run_id;

        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'success'
        WHERE run_id = @run_id;
        PRINT '=== Bronze Layer loaded ===';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_run
        SET finished_at = SYSDATETIME(), status = 'failed', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id;
        THROW;   -- fail loudly: the orchestrator must see a nonzero outcome
    END CATCH
END;
GO

PRINT 'bronze.load_bronze created.';
GO
