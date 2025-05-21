/*====================================================================

  Builds / refreshes the **Gold layer** star-schema views in
  `bike_sales_data_warehouse`.

  • Runs in one explicit transaction (`BEGIN … COMMIT`) so either the
    whole star schema is replaced or nothing changes on failure.

  • Views created / replaced
      1. gold.dim_customers   – customer dimension
         – Generates a surrogate `customer_key` via ROW_NUMBER().
         – Combines CRM core data with ERP demographics (gender, DoB)
           and location lookup.
         – Fills missing gender from ERP feed, defaults to 'n/a'.

      2. gold.dim_products    – product dimension
         – Surrogate `product_key` via ROW_NUMBER() over start-date + key.
         – Joins CRM product info to ERP category mapping.
         – Filters out discontinued products (`prd_end_dt IS NULL`).

      3. gold.fact_sales      – sales fact
         – Grain: one row per sales order line.
         – Links raw sales details to `dim_customers` and `dim_products`
           via natural keys, exposing surrogate keys for BI tools.
         – Carries core measures (`sales_amount`, `quantity`, `price`)
           plus order/ship/due dates.

  • All views are defined with `CREATE OR REPLACE` (except fact_sales,
    which is dropped first to avoid dependency cycles) so downstream
    permissions and dependencies remain intact.

====================================================================*/

\connect bike_sales_data_warehouse;

BEGIN;
  --  create customers dimension
  CREATE OR REPLACE VIEW gold.dim_customers AS
  SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    CASE
      WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
      ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    cl.cntry AS country,
    ci.cst_marital_status AS marital_status,
    ci.cst_create_date AS create_date
  FROM silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 ca
  ON ci.cst_key = ca.cid
  LEFT JOIN silver.erp_loc_a101 cl
  ON ci.cst_key = cl.cid;

  -- create product dimension
  CREATE OR REPLACE VIEW gold.dim_products AS
  SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    p_cat.cat AS category,
    p_cat.subcat AS sub_category,
    p_cat.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
  FROM silver.crm_prd_info pi
  LEFT JOIN silver.erp_px_cat_g1v2 p_cat
  ON pi.cat_id = p_cat.id
  WHERE pi.prd_end_dt IS NULL;

  -- create sales fact.
  DROP VIEW IF EXISTS gold.fact_sales;
  CREATE VIEW gold.fact_sales AS
    SELECT
      sd.sls_order_nm AS order_number,
      prd.product_key,
      cust.customer_key,
      sd.sls_order_dt AS order_date,
      sd.sls_ship_dt AS shipping_date,
      sd.sls_due_dt AS due_date,
      sd.sls_sales AS sales_amount,
      sd.sls_quantity AS quantity,
      sd.sls_price AS price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_customers cust
    ON sd.sls_cust_id = cust.customer_id
    LEFT JOIN gold.dim_products prd
    ON sd.sls_prd_key = prd.product_number;
COMMIT;