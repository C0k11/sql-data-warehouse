/*
===============================================================================
Init: Create DataWarehouse database, layer schemas, and ETL framework objects
===============================================================================
Creates (idempotent, WARNING: drops and recreates the database):
    - Database [DataWarehouse] with data files under $(DATA_DIR)
    - Schemas: bronze, silver, gold, etl
    - etl.config            key/value config (e.g. data_root for BULK INSERT)
    - etl.load_run          one row per pipeline run of a layer
    - etl.load_run_detail   one row per table loaded within a run
    - etl.quality_check     data quality check definitions (seeded in tests/)
    - etl.quality_result    outcome of each check per run

Usage:
    Preferred: py pipeline/run_pipeline.py  (supplies DATA_DIR/DATA_ROOT and
    normalizes source files into the landing zone first).
    Pure sqlcmd: the two variables are REQUIRED (no :setvar defaults — sqlcmd
    gives :setvar precedence over -v, which would silently ignore your paths):
        sqlcmd -S localhost\SQLEXPRESS -E -C -i scripts/init_database.sql ^
               -v DATA_DIR="<dir for mdf/ldf>" DATA_ROOT="<dir with the CSVs>"
    DATA_ROOT should point at line-ending-normalized copies of the CSVs, not
    the raw extracts (see README, "Ingestion robustness").
===============================================================================
*/

USE master;
GO

IF DB_ID('DataWarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

DECLARE @sql NVARCHAR(MAX) = N'
CREATE DATABASE DataWarehouse
ON PRIMARY (
    NAME = N''DataWarehouse'',
    FILENAME = N''$(DATA_DIR)' + N'/DataWarehouse.mdf'',   -- fwd slash: works on Windows and Linux
    SIZE = 256MB, FILEGROWTH = 64MB
)
LOG ON (
    NAME = N''DataWarehouse_log'',
    FILENAME = N''$(DATA_DIR)' + N'/DataWarehouse_log.ldf'',
    SIZE = 64MB, FILEGROWTH = 64MB
);';
EXEC sp_executesql @sql;
GO

-- Analytics workload: simple recovery keeps the log small on full reloads
ALTER DATABASE DataWarehouse SET RECOVERY SIMPLE;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
CREATE SCHEMA etl;
GO

-- =============================================================================
-- ETL framework
-- =============================================================================
CREATE TABLE etl.config (
    config_key   NVARCHAR(100)  NOT NULL PRIMARY KEY,
    config_value NVARCHAR(1000) NOT NULL,
    updated_at   DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME()
);
GO

INSERT INTO etl.config (config_key, config_value)
VALUES ('data_root', N'$(DATA_ROOT)');
GO

CREATE TABLE etl.load_run (
    run_id        INT IDENTITY(1,1) PRIMARY KEY,
    layer         NVARCHAR(20)  NOT NULL,             -- bronze | silver | gold
    started_at    DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    finished_at   DATETIME2(3)  NULL,
    status        NVARCHAR(20)  NOT NULL DEFAULT 'running',  -- running | success | failed
    error_message NVARCHAR(4000) NULL
);
GO

CREATE TABLE etl.load_run_detail (
    detail_id    INT IDENTITY(1,1) PRIMARY KEY,
    run_id       INT           NOT NULL REFERENCES etl.load_run(run_id),
    table_name   NVARCHAR(200) NOT NULL,
    rows_loaded  INT           NULL,
    duration_ms  INT           NULL,
    status       NVARCHAR(20)  NOT NULL DEFAULT 'success',
    error_message NVARCHAR(4000) NULL,
    loaded_at    DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE TABLE etl.quality_check (
    check_id    INT IDENTITY(1,1) PRIMARY KEY,
    layer       NVARCHAR(20)  NOT NULL,               -- silver | gold
    table_name  NVARCHAR(200) NOT NULL,
    check_name  NVARCHAR(200) NOT NULL UNIQUE,
    severity    NVARCHAR(10)  NOT NULL DEFAULT 'error',  -- error | warn
    check_sql   NVARCHAR(MAX) NOT NULL,               -- SELECT returning VIOLATING rows
    is_enabled  BIT           NOT NULL DEFAULT 1
);
GO

GO

-- Parse integer yyyymmdd into DATE. NULL for 0/negative, wrong length
-- (TRY_CONVERT alone reads 4-digit ints as a year!), or implausible years.
CREATE FUNCTION etl.try_yyyymmdd (@v INT)
RETURNS DATE
AS
BEGIN
    RETURN CASE
        WHEN @v IS NULL OR @v <= 0 THEN NULL
        WHEN LEN(CAST(@v AS VARCHAR(12))) != 8 THEN NULL
        WHEN TRY_CONVERT(DATE, CAST(@v AS VARCHAR(8)), 112)
             NOT BETWEEN '1990-01-01' AND '2049-12-31' THEN NULL
        ELSE TRY_CONVERT(DATE, CAST(@v AS VARCHAR(8)), 112)
    END;
END;
GO

CREATE TABLE etl.quality_result (
    result_id   INT IDENTITY(1,1) PRIMARY KEY,
    run_id      INT           NULL,
    check_id    INT           NOT NULL REFERENCES etl.quality_check(check_id),
    violations  INT           NOT NULL,
    status      NVARCHAR(10)  NOT NULL,               -- pass | fail | warn
    checked_at  DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

PRINT 'DataWarehouse initialized: schemas bronze/silver/gold/etl + ETL framework ready.';
GO
