"""Source definitions and table names for the lakehouse build.

Bronze schemas are declared explicitly rather than inferred: schema inference
costs an extra pass over every file and, worse, lets a source change silently
alter downstream types. These mirror the SQL Server bronze DDL 1:1 so the two
implementations can be diffed row for row.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    table: str          # bronze table name
    rel_path: str       # path under datasets/
    schema: str         # Spark DDL string


# Column order matches the CSV header exactly (no header-name matching: the
# ERP extracts use upper-case headers for lower-case target columns).
SOURCES: tuple[Source, ...] = (
    Source(
        "crm_cust_info",
        "source_crm/cust_info.csv",
        "cst_id INT, cst_key STRING, cst_firstname STRING, cst_lastname STRING, "
        "cst_marital_status STRING, cst_gndr STRING, cst_create_date DATE",
    ),
    Source(
        "crm_prd_info",
        "source_crm/prd_info.csv",
        "prd_id INT, prd_key STRING, prd_nm STRING, prd_cost INT, prd_line STRING, "
        "prd_start_dt DATE, prd_end_dt DATE",
    ),
    Source(
        "crm_sales_details",
        "source_crm/sales_details.csv",
        # dates arrive as integer yyyymmdd, including 0 and malformed values
        "sls_ord_num STRING, sls_prd_key STRING, sls_cust_id INT, sls_order_dt INT, "
        "sls_ship_dt INT, sls_due_dt INT, sls_sales INT, sls_quantity INT, sls_price INT",
    ),
    Source(
        "erp_cust_az12",
        "source_erp/CUST_AZ12.csv",
        "cid STRING, bdate DATE, gen STRING",
    ),
    Source(
        "erp_loc_a101",
        "source_erp/LOC_A101.csv",
        "cid STRING, cntry STRING",
    ),
    Source(
        "erp_px_cat_g1v2",
        "source_erp/PX_CAT_G1V2.csv",
        "id STRING, cat STRING, subcat STRING, maintenance STRING",
    ),
)

SILVER_TABLES = (
    "crm_cust_info",
    "crm_prd_info",
    "crm_sales_details",
    "erp_cust_az12",
    "erp_loc_a101",
    "erp_px_cat_g1v2",
)

# Attributes tracked for SCD2 change detection, in hash order.
#
# This is documentation, NOT configuration: the hash is built from an explicit
# expression list in gold.py (each attribute needs its own NULL handling and the
# CRM-over-ERP gender precedence, which a bare column list cannot express).
# Editing this tuple changes nothing on its own — change the concat_ws in
# gold.SRC_SNAPSHOT and keep this list in step. Doing so re-hashes every row,
# which expires and re-inserts the whole dimension, so treat the pair as part of
# the table contract.
SCD2_TRACKED = (
    "customer_number",
    "first_name",
    "last_name",
    "country",
    "marital_status",
    "gender",
    "birthdate",
    "create_date",
)

# Plausibility window for the guarded yyyymmdd parser (see silver.py).
MIN_YEAR, MAX_YEAR = 1990, 2049
