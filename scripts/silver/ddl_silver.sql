/*
===============================================================================
DDL: Silver layer - cleansed, standardized, typed tables
===============================================================================
Every table carries dwh_create_date (load audit timestamp).
Cleansing rules live in silver.load_silver; this file is structure only.
===============================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
    cst_id             INT           NOT NULL,
    cst_key            NVARCHAR(50)  NOT NULL,
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(20)  NOT NULL,   -- Single | Married | n/a
    cst_gndr           NVARCHAR(20)  NOT NULL,   -- Male | Female | n/a
    cst_create_date    DATE,
    dwh_create_date    DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
    prd_id          INT           NOT NULL,
    cat_id          NVARCHAR(50)  NOT NULL,      -- derived: prd_key[1:5], '-'->'_'
    prd_key         NVARCHAR(50)  NOT NULL,      -- derived: prd_key[7:]
    prd_nm          NVARCHAR(100) NOT NULL,
    prd_cost        INT           NOT NULL,      -- NULL -> 0
    prd_line        NVARCHAR(20)  NOT NULL,      -- Mountain | Road | Other Sales | Touring | n/a
    prd_start_dt    DATE          NOT NULL,
    prd_end_dt      DATE,                        -- recomputed: next start - 1 day
    dwh_create_date DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50)  NOT NULL,
    sls_prd_key     NVARCHAR(50)  NOT NULL,
    sls_cust_id     INT           NOT NULL,
    sls_order_dt    DATE,                        -- NULL when source was 0/malformed
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,                         -- repaired: qty * |price| when invalid
    sls_quantity    INT,
    sls_price       INT,                         -- repaired: sales / qty when invalid
    dwh_create_date DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50)  NOT NULL,      -- 'NAS' prefix stripped
    bdate           DATE,                        -- future dates -> NULL
    gen             NVARCHAR(20)  NOT NULL,      -- Male | Female | n/a
    dwh_create_date DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50)  NOT NULL,      -- '-' removed to match cst_key
    cntry           NVARCHAR(50)  NOT NULL,      -- normalized country names
    dwh_create_date DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50)  NOT NULL,
    cat             NVARCHAR(50)  NOT NULL,
    subcat          NVARCHAR(50)  NOT NULL,
    maintenance     NVARCHAR(50)  NOT NULL,
    dwh_create_date DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME()
);
GO

PRINT 'Silver DDL complete.';
GO
