# Data Catalog — Gold Layer

The gold layer is the business-facing star schema. Analysts should query gold
objects only; bronze and silver are internal.

## gold.fact_sales (view)

One row per order line.

| Column | Type | Description |
|---|---|---|
| order_number | NVARCHAR(50) | Sales order identifier, e.g. `SO43697`; repeats across lines of the same order |
| product_key | INT | FK → `gold.dim_products.product_key` |
| customer_key | INT | FK → `gold.dim_customers.customer_key` (stable SCD2 surrogate) |
| customer_id | INT | Customer natural key, for as-of joins against the SCD2 history |
| order_date_key | INT | FK → `gold.dim_date.date_key` (yyyymmdd); NULL when the source date was invalid |
| order_date | DATE | Order date; NULL for the 19 source rows with `0`/malformed integer dates |
| shipping_date | DATE | Ship date |
| due_date | DATE | Payment due date |
| sales_amount | INT | Line revenue; repaired to `quantity × |price|` where the source violated the identity |
| quantity | INT | Units ordered (1–10 in this dataset) |
| price | INT | Unit price; derived from `|sales| / quantity` where the source was missing/nonpositive |

## gold.dim_customers (view — current slice of the SCD2 table)

| Column | Type | Description |
|---|---|---|
| customer_key | INT | Stable surrogate key (`customer_sk` of the current SCD2 row) |
| customer_id | INT | CRM natural key (`cst_id`) |
| customer_number | NVARCHAR(50) | Cross-system business key (`AW...`), joins the ERP extracts |
| first_name / last_name | NVARCHAR(50) | Trimmed CRM names |
| country | NVARCHAR(50) | From ERP locations, normalized (`DE`→`Germany`, `US`/`USA`→`United States`, blank→`n/a`) |
| marital_status | NVARCHAR(20) | `Single` / `Married` / `n/a` |
| gender | NVARCHAR(20) | CRM value wins; falls back to ERP when CRM is `n/a` |
| birthdate | DATE | From ERP; future dates nulled (16 rows) |
| create_date | DATE | CRM record creation date |

## gold.dim_customers_scd2 (table — full history)

All business columns of `dim_customers` (`customer_id` … `create_date`;
`customer_key` exists only on the view, as an alias of `customer_sk`) plus:

| Column | Type | Description |
|---|---|---|
| customer_sk | INT IDENTITY | Surrogate key, one per customer **version** |
| attr_hash | VARBINARY(32) | SHA2_256 over tracked attributes; drives change detection |
| valid_from / valid_to | DATETIME2(3) | Version validity window; `9999-12-31` = open. First-ever version per customer is backdated to `1900-01-01` so as-of joins cover facts that predate the warehouse; later versions start at the load that observed the change (warehouse time, not business time) |
| is_current | BIT | Exactly one current row per `customer_id` (enforced by filtered unique index + quality check) |

Point-in-time query pattern:

```sql
SELECT ...
FROM gold.fact_sales f
JOIN gold.dim_customers_scd2 c
  ON c.customer_id = f.customer_id                    -- natural key
 AND f.order_date >= c.valid_from
 AND f.order_date <  c.valid_to;                      -- version valid at order time
```

## gold.dim_products (view)

| Column | Type | Description |
|---|---|---|
| product_key | INT | `ROW_NUMBER()` surrogate — **not stable across rebuilds**; acceptable because nothing persists it outside the warehouse (documented tradeoff vs. the materialized customer dimension) |
| product_id | INT | CRM `prd_id` of the current version |
| product_number | NVARCHAR(50) | Model key parsed from `prd_key` (chars 7+), joins fact lines |
| product_name | NVARCHAR(100) | Descriptive name |
| category_id | NVARCHAR(50) | Parsed from `prd_key` (chars 1–5, `-`→`_`) |
| category / subcategory / maintenance | NVARCHAR(50) | From ERP categories; NULL for `CO_PE` (missing from the ERP extract — 7 products, kept as a warning) |
| cost | INT | Standard cost; NULL→0 (2 rows) |
| product_line | NVARCHAR(20) | `Mountain` / `Road` / `Other Sales` / `Touring` / `n/a` |
| start_date | DATE | Version start of the current product version |

## gold.dim_date (table)

Generated calendar covering the full span of order/ship/due dates
(2010-12-29 → 2014-02-09, 1,139 days).

| Column | Type | Description |
|---|---|---|
| date_key | INT | yyyymmdd |
| full_date | DATE | Calendar date |
| year / quarter / month / day | numeric | Calendar parts |
| month_name / weekday_name | NVARCHAR(15) | English names |
| is_weekend | BIT | Saturday/Sunday |

## ETL framework (schema `etl`)

| Object | Purpose |
|---|---|
| etl.config | Key/value config; `data_root` points BULK INSERT at the landing zone |
| etl.load_run / etl.load_run_detail | Per-run and per-table audit: rows, duration, status, error |
| etl.quality_check | 40 declarative checks (violating-rows SQL + severity) |
| etl.quality_result | Check outcomes per run |
| etl.run_quality_checks | Executes enabled checks; THROWs on error-severity violations |
| etl.try_yyyymmdd | Guarded integer-yyyymmdd → DATE parser |
