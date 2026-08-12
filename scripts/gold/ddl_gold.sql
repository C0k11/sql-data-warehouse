/*
===============================================================================
DDL: Gold layer - star schema for analytics
===============================================================================
Physical tables:
    gold.dim_date            calendar dimension (generated, not in tutorial)
    gold.dim_customers_scd2  Type-2 slowly changing customer dimension with
                             stable surrogate keys and validity windows
Views:
    gold.dim_customers  current customer slice of the SCD2 table
    gold.dim_products   current products (versioned source, latest version)
    gold.fact_sales     sales facts wired to dimension surrogate keys

Design note: the customer dimension is materialized (stable customer_key
across loads - safe for BI extracts); products stay a view with ROW_NUMBER
keys, acceptable because nothing persists product_key outside the warehouse.
The tradeoff is documented in docs/data_catalog.md.
===============================================================================
*/

USE DataWarehouse;
GO

------------------------------------------------------------------
-- gold.dim_date
------------------------------------------------------------------
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO
CREATE TABLE gold.dim_date (
    date_key     INT          NOT NULL PRIMARY KEY,   -- yyyymmdd
    full_date    DATE         NOT NULL UNIQUE,
    year         SMALLINT     NOT NULL,
    quarter      TINYINT      NOT NULL,
    month        TINYINT      NOT NULL,
    month_name   NVARCHAR(15) NOT NULL,
    day          TINYINT      NOT NULL,
    weekday_name NVARCHAR(15) NOT NULL,
    is_weekend   BIT          NOT NULL
);
GO

------------------------------------------------------------------
-- gold.dim_customers_scd2
------------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers_scd2', 'U') IS NOT NULL
    DROP TABLE gold.dim_customers_scd2;
GO
CREATE TABLE gold.dim_customers_scd2 (
    customer_sk     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    customer_id     INT               NOT NULL,       -- natural key (CRM cst_id)
    customer_number NVARCHAR(50)      NOT NULL,
    first_name      NVARCHAR(50),
    last_name       NVARCHAR(50),
    country         NVARCHAR(50)      NOT NULL,
    marital_status  NVARCHAR(20)      NOT NULL,
    gender          NVARCHAR(20)      NOT NULL,
    birthdate       DATE,
    create_date     DATE,
    attr_hash       VARBINARY(32)     NOT NULL,       -- SHA2_256 of tracked attributes
    valid_from      DATETIME2(3)      NOT NULL,
    valid_to        DATETIME2(3)      NOT NULL DEFAULT '9999-12-31',
    is_current      BIT               NOT NULL DEFAULT 1
);
GO
CREATE UNIQUE INDEX uq_dim_customers_scd2_current
    ON gold.dim_customers_scd2 (customer_id)
    WHERE is_current = 1;                             -- exactly one current row per customer
GO
CREATE INDEX ix_dim_customers_scd2_nk
    ON gold.dim_customers_scd2 (customer_id, valid_from);
GO

------------------------------------------------------------------
-- gold.dim_customers (current slice, stable surrogate keys)
------------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
    customer_sk     AS customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date
FROM gold.dim_customers_scd2
WHERE is_current = 1;
GO

------------------------------------------------------------------
-- gold.dim_products (current product versions)
------------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
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
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;            -- latest version of each product only
GO

------------------------------------------------------------------
-- gold.fact_sales
------------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num                                   AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_cust_id                                   AS customer_id,   -- natural key: enables as-of joins to the SCD2 dimension
    CONVERT(INT, CONVERT(VARCHAR(8), sd.sls_order_dt, 112)) AS order_date_key,
    sd.sls_order_dt                                  AS order_date,
    sd.sls_ship_dt                                   AS shipping_date,
    sd.sls_due_dt                                    AS due_date,
    sd.sls_sales                                     AS sales_amount,
    sd.sls_quantity                                  AS quantity,
    sd.sls_price                                     AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

PRINT 'Gold DDL complete.';
GO
