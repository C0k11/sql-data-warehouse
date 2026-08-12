/*
===============================================================================
Seed: etl.quality_check - data quality check definitions
===============================================================================
Each check_sql SELECTs VIOLATING rows; the runner counts them.
0 rows = pass. severity 'error' fails the pipeline, 'warn' is reported only.
Rerunnable: wipes and reseeds the definitions.
===============================================================================
*/

USE DataWarehouse;
GO

DELETE FROM etl.quality_result;
DELETE FROM etl.quality_check;
GO

INSERT INTO etl.quality_check (layer, table_name, check_name, severity, check_sql) VALUES
-- ---------------------------------------------------------------- silver.crm_cust_info
('silver', 'silver.crm_cust_info', 'cust_id_unique', 'error',
 N'SELECT cst_id FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1'),
-- DATALENGTH, not != TRIM(x): SQL Server ignores trailing spaces in =/!=
-- comparisons (ANSI padding), so x != TRIM(x) can never catch trailing blanks
('silver', 'silver.crm_cust_info', 'cust_names_trimmed', 'error',
 N'SELECT 1 AS v FROM silver.crm_cust_info WHERE DATALENGTH(cst_firstname) != DATALENGTH(TRIM(cst_firstname)) OR DATALENGTH(cst_lastname) != DATALENGTH(TRIM(cst_lastname))'),
('silver', 'silver.crm_cust_info', 'cust_marital_status_domain', 'error',
 N'SELECT 1 AS v FROM silver.crm_cust_info WHERE cst_marital_status NOT IN (''Single'', ''Married'', ''n/a'')'),
('silver', 'silver.crm_cust_info', 'cust_gender_domain', 'error',
 N'SELECT 1 AS v FROM silver.crm_cust_info WHERE cst_gndr NOT IN (''Male'', ''Female'', ''n/a'')'),

-- ---------------------------------------------------------------- silver.crm_prd_info
('silver', 'silver.crm_prd_info', 'prd_id_unique', 'error',
 N'SELECT prd_id FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1'),
('silver', 'silver.crm_prd_info', 'prd_cost_non_negative', 'error',
 N'SELECT 1 AS v FROM silver.crm_prd_info WHERE prd_cost < 0'),
('silver', 'silver.crm_prd_info', 'prd_line_domain', 'error',
 N'SELECT 1 AS v FROM silver.crm_prd_info WHERE prd_line NOT IN (''Mountain'', ''Road'', ''Other Sales'', ''Touring'', ''n/a'')'),
('silver', 'silver.crm_prd_info', 'prd_validity_ordered', 'error',
 N'SELECT 1 AS v FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt'),
('silver', 'silver.crm_prd_info', 'prd_versions_no_overlap', 'error',
 N'SELECT 1 AS v FROM silver.crm_prd_info a JOIN silver.crm_prd_info b ON a.prd_key = b.prd_key AND a.prd_id != b.prd_id AND a.prd_start_dt <= ISNULL(b.prd_end_dt, ''9999-12-31'') AND b.prd_start_dt <= ISNULL(a.prd_end_dt, ''9999-12-31'')'),

-- ---------------------------------------------------------------- silver.crm_sales_details
('silver', 'silver.crm_sales_details', 'sales_math_consistent', 'error',
 N'SELECT 1 AS v FROM silver.crm_sales_details WHERE sls_sales != sls_quantity * sls_price'),
('silver', 'silver.crm_sales_details', 'sales_measures_not_null', 'warn',
 N'SELECT 1 AS v FROM silver.crm_sales_details WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL'),
('silver', 'silver.crm_sales_details', 'sales_measures_positive', 'error',
 N'SELECT 1 AS v FROM silver.crm_sales_details WHERE sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0'),
('silver', 'silver.crm_sales_details', 'sales_dates_ordered', 'error',
 N'SELECT 1 AS v FROM silver.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt'),
('silver', 'silver.crm_sales_details', 'sales_order_date_in_range', 'warn',
 N'SELECT 1 AS v FROM silver.crm_sales_details WHERE sls_order_dt < ''2000-01-01'' OR sls_order_dt > ''2030-12-31'''),
('silver', 'silver.crm_sales_details', 'sales_product_exists', 'error',
 N'SELECT 1 AS v FROM silver.crm_sales_details sd WHERE NOT EXISTS (SELECT 1 FROM silver.crm_prd_info p WHERE p.prd_key = sd.sls_prd_key)'),
('silver', 'silver.crm_sales_details', 'sales_customer_exists', 'error',
 N'SELECT 1 AS v FROM silver.crm_sales_details sd WHERE NOT EXISTS (SELECT 1 FROM silver.crm_cust_info c WHERE c.cst_id = sd.sls_cust_id)'),

-- ---------------------------------------------------------------- silver.erp_cust_az12
('silver', 'silver.erp_cust_az12', 'az12_no_future_birthdate', 'error',
 N'SELECT 1 AS v FROM silver.erp_cust_az12 WHERE bdate > GETDATE()'),
('silver', 'silver.erp_cust_az12', 'az12_gender_domain', 'error',
 N'SELECT 1 AS v FROM silver.erp_cust_az12 WHERE gen NOT IN (''Male'', ''Female'', ''n/a'')'),
('silver', 'silver.erp_cust_az12', 'az12_cid_matches_crm', 'warn',
 N'SELECT 1 AS v FROM silver.erp_cust_az12 e WHERE NOT EXISTS (SELECT 1 FROM silver.crm_cust_info c WHERE c.cst_key = e.cid)'),

-- ---------------------------------------------------------------- silver.erp_loc_a101
('silver', 'silver.erp_loc_a101', 'loc_country_normalized', 'error',
 N'SELECT 1 AS v FROM silver.erp_loc_a101 WHERE cntry IN (''DE'', ''US'', ''USA'', '''') OR DATALENGTH(cntry) != DATALENGTH(TRIM(cntry)) OR cntry IS NULL'),
('silver', 'silver.erp_loc_a101', 'loc_cid_matches_crm', 'warn',
 N'SELECT 1 AS v FROM silver.erp_loc_a101 e WHERE NOT EXISTS (SELECT 1 FROM silver.crm_cust_info c WHERE c.cst_key = e.cid)'),
('silver', 'silver.erp_cust_az12', 'az12_cid_unique', 'error',
 N'SELECT cid FROM silver.erp_cust_az12 GROUP BY cid HAVING COUNT(*) > 1'),
('silver', 'silver.erp_loc_a101', 'loc_cid_unique', 'error',
 N'SELECT cid FROM silver.erp_loc_a101 GROUP BY cid HAVING COUNT(*) > 1'),

-- ---------------------------------------------------------------- silver.erp_px_cat_g1v2
('silver', 'silver.erp_px_cat_g1v2', 'px_id_unique', 'error',
 N'SELECT id FROM silver.erp_px_cat_g1v2 GROUP BY id HAVING COUNT(*) > 1'),

-- ---------------------------------------------------------------- file-format artifacts
-- CR/LF/TAB fragments surviving into silver mean the ingestion layer broke;
-- domain checks alone cannot catch these (polluted values fold into 'n/a')
('silver', 'silver.erp_cust_az12', 'az12_gen_no_control_chars', 'error',
 N'SELECT 1 AS v FROM silver.erp_cust_az12 WHERE gen LIKE ''%'' + CHAR(13) + ''%'' OR gen LIKE ''%'' + CHAR(10) + ''%'' OR gen LIKE ''%'' + CHAR(9) + ''%'''),
('silver', 'silver.erp_loc_a101', 'loc_cntry_no_control_chars', 'error',
 N'SELECT 1 AS v FROM silver.erp_loc_a101 WHERE cntry LIKE ''%'' + CHAR(13) + ''%'' OR cntry LIKE ''%'' + CHAR(10) + ''%'' OR cntry LIKE ''%'' + CHAR(9) + ''%'''),
('silver', 'silver.crm_cust_info', 'cust_names_no_control_chars', 'error',
 N'SELECT 1 AS v FROM silver.crm_cust_info CROSS APPLY (VALUES (cst_firstname), (cst_lastname)) AS n(s) WHERE n.s LIKE ''%'' + CHAR(13) + ''%'' OR n.s LIKE ''%'' + CHAR(10) + ''%'' OR n.s LIKE ''%'' + CHAR(9) + ''%'''),

-- ---------------------------------------------------------------- fold-rate monitors
-- the domain checks above are satisfied BY CONSTRUCTION (silver folds unknown
-- values to 'n/a'), so they cannot detect mass corruption that folds an entire
-- column; these rate ceilings (profiled natural rate + headroom) can
('silver', 'silver.crm_cust_info', 'cust_gender_na_rate_under_35pct', 'warn',
 N'SELECT 1 AS v WHERE (SELECT AVG(CASE WHEN cst_gndr = ''n/a'' THEN 1.0 ELSE 0.0 END) FROM silver.crm_cust_info) > 0.35'),
('silver', 'silver.crm_cust_info', 'cust_marital_na_rate_under_5pct', 'warn',
 N'SELECT 1 AS v WHERE (SELECT AVG(CASE WHEN cst_marital_status = ''n/a'' THEN 1.0 ELSE 0.0 END) FROM silver.crm_cust_info) > 0.05'),
('silver', 'silver.crm_prd_info', 'prd_line_na_rate_under_10pct', 'warn',
 N'SELECT 1 AS v WHERE (SELECT AVG(CASE WHEN prd_line = ''n/a'' THEN 1.0 ELSE 0.0 END) FROM silver.crm_prd_info) > 0.10'),
('silver', 'silver.erp_cust_az12', 'az12_gender_na_rate_under_15pct', 'warn',
 N'SELECT 1 AS v WHERE (SELECT AVG(CASE WHEN gen = ''n/a'' THEN 1.0 ELSE 0.0 END) FROM silver.erp_cust_az12) > 0.15'),

-- ---------------------------------------------------------------- gold
('gold', 'gold.dim_customers', 'dim_customers_key_unique', 'error',
 N'SELECT customer_key FROM gold.dim_customers GROUP BY customer_key HAVING COUNT(*) > 1'),
-- product_key is ROW_NUMBER (unique by construction); the real fanout risk is
-- two CURRENT versions of the same product_number double-counting fact rows
('gold', 'gold.dim_products', 'dim_products_number_unique', 'error',
 N'SELECT product_number FROM gold.dim_products GROUP BY product_number HAVING COUNT(*) > 1'),
('gold', 'gold.dim_products', 'dim_products_category_coverage', 'warn',
 N'SELECT 1 AS v FROM gold.dim_products WHERE category IS NULL'),
('gold', 'gold.fact_sales', 'fact_no_orphan_dimensions', 'error',
 N'SELECT 1 AS v FROM gold.fact_sales WHERE product_key IS NULL OR customer_key IS NULL'),
('gold', 'gold.fact_sales', 'fact_date_key_in_dim_date', 'error',
 N'SELECT 1 AS v FROM gold.fact_sales f WHERE f.order_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM gold.dim_date d WHERE d.date_key = f.order_date_key)'),
-- AT MOST one current row: zero current rows is legitimate (customer deleted
-- from the source gets expired with no replacement - by loader design)
('gold', 'gold.dim_customers_scd2', 'scd2_at_most_one_current', 'error',
 N'SELECT customer_id FROM gold.dim_customers_scd2 GROUP BY customer_id HAVING SUM(CAST(is_current AS INT)) > 1'),
('gold', 'gold.dim_customers_scd2', 'scd2_deleted_customers_visible', 'warn',
 N'SELECT customer_id FROM gold.dim_customers_scd2 GROUP BY customer_id HAVING SUM(CAST(is_current AS INT)) = 0'),
('gold', 'gold.dim_customers_scd2', 'scd2_windows_sane', 'error',
 N'SELECT 1 AS v FROM gold.dim_customers_scd2 WHERE valid_from > valid_to'),
('gold', 'gold.dim_customers_scd2', 'scd2_no_overlapping_versions', 'error',
 N'SELECT 1 AS v FROM gold.dim_customers_scd2 a JOIN gold.dim_customers_scd2 b ON a.customer_id = b.customer_id AND a.customer_sk != b.customer_sk AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to');
GO

DECLARE @n INT = (SELECT COUNT(*) FROM etl.quality_check);
PRINT CONCAT('Seeded ', @n, ' quality checks.');
GO
