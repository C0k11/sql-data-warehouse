"""Declarative data-quality checks. Each SQL selects VIOLATING rows; 0 = pass.

The SQL Server build stores these definitions in a table and drives them with a
cursor, because T-SQL has no good way to keep a list of queries under version
control. Here they live in Python, so they diff in review, and only the
*results* are persisted to Delta. Same declarative model, better home.

Dialect notes for anyone comparing the two files:
  - `x != trim(x)` works directly. The T-SQL version has to use DATALENGTH
    because SQL Server ignores trailing spaces in = / != comparisons (ANSI
    padding), so `x != TRIM(x)` there can never be true.
  - Control characters use rlike instead of three LIKE ... CHAR(n) predicates.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    layer: str
    table: str
    name: str
    severity: str   # 'error' fails the pipeline, 'warn' is reported only
    sql: str


def checks(cat: str) -> tuple[Check, ...]:
    s = f"{cat}.silver"
    g = f"{cat}.gold"
    return (
        # ------------------------------------------------ silver.crm_cust_info
        Check("silver", "crm_cust_info", "cust_id_unique", "error",
              f"SELECT cst_id FROM {s}.crm_cust_info GROUP BY cst_id HAVING count(*) > 1"),
        Check("silver", "crm_cust_info", "cust_names_trimmed", "error",
              f"SELECT 1 FROM {s}.crm_cust_info "
              f"WHERE cst_firstname != trim(cst_firstname) OR cst_lastname != trim(cst_lastname)"),
        Check("silver", "crm_cust_info", "cust_marital_status_domain", "error",
              f"SELECT 1 FROM {s}.crm_cust_info "
              f"WHERE cst_marital_status NOT IN ('Single','Married','n/a')"),
        Check("silver", "crm_cust_info", "cust_gender_domain", "error",
              f"SELECT 1 FROM {s}.crm_cust_info WHERE cst_gndr NOT IN ('Male','Female','n/a')"),

        # ------------------------------------------------- silver.crm_prd_info
        Check("silver", "crm_prd_info", "prd_id_unique", "error",
              f"SELECT prd_id FROM {s}.crm_prd_info GROUP BY prd_id HAVING count(*) > 1"),
        Check("silver", "crm_prd_info", "prd_cost_non_negative", "error",
              f"SELECT 1 FROM {s}.crm_prd_info WHERE prd_cost < 0"),
        Check("silver", "crm_prd_info", "prd_line_domain", "error",
              f"SELECT 1 FROM {s}.crm_prd_info "
              f"WHERE prd_line NOT IN ('Mountain','Road','Other Sales','Touring','n/a')"),
        Check("silver", "crm_prd_info", "prd_validity_ordered", "error",
              f"SELECT 1 FROM {s}.crm_prd_info WHERE prd_end_dt < prd_start_dt"),
        Check("silver", "crm_prd_info", "prd_versions_no_overlap", "error",
              f"SELECT 1 FROM {s}.crm_prd_info a JOIN {s}.crm_prd_info b "
              f"ON a.prd_key = b.prd_key AND a.prd_id != b.prd_id "
              f"AND a.prd_start_dt <= coalesce(b.prd_end_dt, DATE'9999-12-31') "
              f"AND b.prd_start_dt <= coalesce(a.prd_end_dt, DATE'9999-12-31')"),

        # -------------------------------------------- silver.crm_sales_details
        Check("silver", "crm_sales_details", "sales_math_consistent", "error",
              f"SELECT 1 FROM {s}.crm_sales_details WHERE sls_sales != sls_quantity * sls_price"),
        Check("silver", "crm_sales_details", "sales_measures_not_null", "warn",
              f"SELECT 1 FROM {s}.crm_sales_details "
              f"WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL"),
        Check("silver", "crm_sales_details", "sales_measures_positive", "error",
              f"SELECT 1 FROM {s}.crm_sales_details "
              f"WHERE sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0"),
        Check("silver", "crm_sales_details", "sales_dates_ordered", "error",
              f"SELECT 1 FROM {s}.crm_sales_details "
              f"WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt"),
        Check("silver", "crm_sales_details", "sales_order_date_in_range", "warn",
              f"SELECT 1 FROM {s}.crm_sales_details "
              f"WHERE sls_order_dt < DATE'2000-01-01' OR sls_order_dt > DATE'2030-12-31'"),
        Check("silver", "crm_sales_details", "sales_product_exists", "error",
              f"SELECT 1 FROM {s}.crm_sales_details sd WHERE NOT EXISTS "
              f"(SELECT 1 FROM {s}.crm_prd_info p WHERE p.prd_key = sd.sls_prd_key)"),
        Check("silver", "crm_sales_details", "sales_customer_exists", "error",
              f"SELECT 1 FROM {s}.crm_sales_details sd WHERE NOT EXISTS "
              f"(SELECT 1 FROM {s}.crm_cust_info c WHERE c.cst_id = sd.sls_cust_id)"),

        # ------------------------------------------------ silver.erp_cust_az12
        Check("silver", "erp_cust_az12", "az12_no_future_birthdate", "error",
              f"SELECT 1 FROM {s}.erp_cust_az12 WHERE bdate > current_date()"),
        Check("silver", "erp_cust_az12", "az12_gender_domain", "error",
              f"SELECT 1 FROM {s}.erp_cust_az12 WHERE gen NOT IN ('Male','Female','n/a')"),
        Check("silver", "erp_cust_az12", "az12_cid_unique", "error",
              f"SELECT cid FROM {s}.erp_cust_az12 GROUP BY cid HAVING count(*) > 1"),
        Check("silver", "erp_cust_az12", "az12_cid_matches_crm", "warn",
              f"SELECT 1 FROM {s}.erp_cust_az12 e WHERE NOT EXISTS "
              f"(SELECT 1 FROM {s}.crm_cust_info c WHERE c.cst_key = e.cid)"),

        # ------------------------------------------------- silver.erp_loc_a101
        Check("silver", "erp_loc_a101", "loc_country_normalized", "error",
              f"SELECT 1 FROM {s}.erp_loc_a101 "
              f"WHERE cntry IN ('DE','US','USA','') OR cntry != trim(cntry) OR cntry IS NULL"),
        Check("silver", "erp_loc_a101", "loc_cid_unique", "error",
              f"SELECT cid FROM {s}.erp_loc_a101 GROUP BY cid HAVING count(*) > 1"),
        Check("silver", "erp_loc_a101", "loc_cid_matches_crm", "warn",
              f"SELECT 1 FROM {s}.erp_loc_a101 e WHERE NOT EXISTS "
              f"(SELECT 1 FROM {s}.crm_cust_info c WHERE c.cst_key = e.cid)"),

        # ---------------------------------------------- silver.erp_px_cat_g1v2
        Check("silver", "erp_px_cat_g1v2", "px_id_unique", "error",
              f"SELECT id FROM {s}.erp_px_cat_g1v2 GROUP BY id HAVING count(*) > 1"),

        # ------------------------------------------------ file-format artifacts
        # CR/LF/TAB fragments surviving into silver mean ingestion broke. Domain
        # checks alone cannot catch these: polluted values fold into 'n/a'.
        Check("silver", "erp_cust_az12", "az12_gen_no_control_chars", "error",
              f"SELECT 1 FROM {s}.erp_cust_az12 WHERE gen rlike '[\\r\\n\\t]'"),
        Check("silver", "erp_loc_a101", "loc_cntry_no_control_chars", "error",
              f"SELECT 1 FROM {s}.erp_loc_a101 WHERE cntry rlike '[\\r\\n\\t]'"),
        Check("silver", "crm_cust_info", "cust_names_no_control_chars", "error",
              f"SELECT 1 FROM (SELECT explode(array(cst_firstname, cst_lastname)) AS v "
              f"FROM {s}.crm_cust_info) WHERE v rlike '[\\r\\n\\t]'"),

        # ---------------------------------------------------- fold-rate monitors
        # The domain checks above are satisfied BY CONSTRUCTION (silver folds
        # unknown values to 'n/a'), so they cannot detect mass corruption that
        # folds an entire column. These ceilings (profiled rate + headroom) can.
        Check("silver", "crm_cust_info", "cust_gender_na_rate_under_35pct", "warn",
              f"SELECT 1 WHERE (SELECT avg(CASE WHEN cst_gndr = 'n/a' THEN 1.0 ELSE 0.0 END) "
              f"FROM {s}.crm_cust_info) > 0.35"),
        Check("silver", "crm_cust_info", "cust_marital_na_rate_under_5pct", "warn",
              f"SELECT 1 WHERE (SELECT avg(CASE WHEN cst_marital_status = 'n/a' THEN 1.0 ELSE 0.0 END) "
              f"FROM {s}.crm_cust_info) > 0.05"),
        Check("silver", "crm_prd_info", "prd_line_na_rate_under_10pct", "warn",
              f"SELECT 1 WHERE (SELECT avg(CASE WHEN prd_line = 'n/a' THEN 1.0 ELSE 0.0 END) "
              f"FROM {s}.crm_prd_info) > 0.10"),
        Check("silver", "erp_cust_az12", "az12_gender_na_rate_under_15pct", "warn",
              f"SELECT 1 WHERE (SELECT avg(CASE WHEN gen = 'n/a' THEN 1.0 ELSE 0.0 END) "
              f"FROM {s}.erp_cust_az12) > 0.15"),

        # ------------------------------------------------------------- gold
        Check("gold", "dim_customers", "dim_customers_key_unique", "error",
              f"SELECT customer_key FROM {g}.dim_customers GROUP BY customer_key HAVING count(*) > 1"),
        # product_key is row_number (unique by construction); the real fanout
        # risk is two CURRENT versions of one product double-counting facts.
        Check("gold", "dim_products", "dim_products_number_unique", "error",
              f"SELECT product_number FROM {g}.dim_products GROUP BY product_number HAVING count(*) > 1"),
        Check("gold", "dim_products", "dim_products_category_coverage", "warn",
              f"SELECT 1 FROM {g}.dim_products WHERE category IS NULL"),
        Check("gold", "fact_sales", "fact_no_orphan_dimensions", "error",
              f"SELECT 1 FROM {g}.fact_sales WHERE product_key IS NULL OR customer_key IS NULL"),
        Check("gold", "fact_sales", "fact_date_key_in_dim_date", "error",
              f"SELECT 1 FROM {g}.fact_sales f WHERE f.order_date_key IS NOT NULL "
              f"AND NOT EXISTS (SELECT 1 FROM {g}.dim_date d WHERE d.date_key = f.order_date_key)"),
        # On SQL Server this invariant is ALSO enforced by a filtered unique
        # index, so a violation is impossible. Delta has no unique indexes, so
        # here this check is the only thing standing between a loader bug and a
        # silently double-counted customer.
        Check("gold", "dim_customers_scd2", "scd2_at_most_one_current", "error",
              f"SELECT customer_id FROM {g}.dim_customers_scd2 GROUP BY customer_id "
              f"HAVING sum(CAST(is_current AS INT)) > 1"),
        # Zero current rows is legitimate: a customer deleted from the source is
        # expired with no replacement, by loader design.
        Check("gold", "dim_customers_scd2", "scd2_deleted_customers_visible", "warn",
              f"SELECT customer_id FROM {g}.dim_customers_scd2 GROUP BY customer_id "
              f"HAVING sum(CAST(is_current AS INT)) = 0"),
        Check("gold", "dim_customers_scd2", "scd2_windows_sane", "error",
              f"SELECT 1 FROM {g}.dim_customers_scd2 WHERE valid_from > valid_to"),
        Check("gold", "dim_customers_scd2", "scd2_no_overlapping_versions", "error",
              f"SELECT 1 FROM {g}.dim_customers_scd2 a JOIN {g}.dim_customers_scd2 b "
              f"ON a.customer_id = b.customer_id AND a.customer_sk != b.customer_sk "
              f"AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to"),
    )
