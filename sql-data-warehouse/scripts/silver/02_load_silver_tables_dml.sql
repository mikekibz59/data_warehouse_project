/*====================================================================
  Defines and immediately runs the procedure **silver.load_silver()**  
  (PL/pgSQL) that refreshes every table in the Silver layer from the
  raw Bronze layer.

  ▶  High-level flow inside the procedure
     1. For each Silver table:
          • `TRUNCATE … CASCADE`            – wipe previous data, clear FK/view deps
          • `INSERT INTO … SELECT …`        – load cleansed, typed data from Bronze
          • `RAISE NOTICE …`                – emit progress messages for logs
     2. Cleansing rules applied include
          • De-duplication via `ROW_NUMBER()` (latest row per `cst_id`)
          • Null/blank fallback to `'n/a'`
          • Gender and marital-status decoding
          • Date casting & sanity checks (`YYYYMMDD` ↔ `DATE`, future/ancient cutoff)
          • Product category/key parsing (`SUBSTRING`, `REPLACE`)
          • Re-computing sales totals if inconsistent
     3. Every insert relies on *set-based* SQL—no row-by-row loops.

  ▶  Tables refreshed
       ─ silver.crm_cust_info
       ─ silver.crm_prd_info
       ─ silver.crm_sales_details
       ─ silver.erp_cust_az12
       ─ silver.erp_loc_a101
       ─ silver.erp_px_cat_g1v2

  ▶  Usage
       • Script creates/updates the procedure with
             CREATE OR REPLACE PROCEDURE silver.load_silver();
       • Immediately calls it:
             CALL silver.load_silver();
       • Suitable for docker-entrypoint or scheduled ETL runs.

  NOTE
  • Ensure Bronze layer exists and is populated before calling.
  • Procedure runs in a single implicit transaction—if any step fails
    the whole load is rolled back.
====================================================================*/

\connect bike_sales_data_warehouse;

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
  BEGIN
    -- clean and load crm_cust_info table.
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE silver.crm_cust_info CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
    )
    SELECT
      cst_id,
      cst_key,
    CASE
      WHEN cst_firstname IS NULL OR TRIM(cst_firstname) = ''
        THEN 'n/a'
      ELSE TRIM(cst_firstname)
    END AS cst_firstname,

    CASE
      WHEN cst_lastname IS NULL OR TRIM(cst_lastname) = ''
        THEN 'n/a'
      ELSE TRIM(cst_lastname)
    END cst_lastname,

    CASE
      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
      WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
      ELSE 'n/a'
    END cst_marital_status,

    CASE
      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
      WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
      ELSE 'n/a'
    END cst_gndr,
    cst_create_date::DATE
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      from bronze.crm_cust_info
    ) T WHERE flag_last = 1 AND cst_id IS NOT NULL;

    -- Clean and load silver.crm_prd_info.
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE silver.crm_prd_info CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    CASE
      WHEN prd_cost IS NULL THEN 0
      ELSE CAST(prd_cost AS INTEGER)
    END prd_cost,

    CASE UPPER(TRIM(prd_line))
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END prd_line,
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
    (LEAD(prd_start_dt::date) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1)  AS prd_end_dt
        
    FROM bronze.crm_prd_info;

    -- Clean and load silver.crm_sales_details
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE silver.crm_sales_details CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
      sls_order_nm,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
    )
    SELECT
    sls_order_nm,
    sls_prd_key,
    sls_cust_id::INTEGER,
    CASE
      WHEN sls_order_dt::INTEGER = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
      ELSE sls_order_dt::DATE
    END sls_order_dt,

    CASE
      WHEN CAST(sls_ship_dt AS INTEGER) = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
      ELSE CAST(sls_ship_dt AS DATE)
    END sls_ship_dt,

    CASE
      WHEN sls_due_dt::INTEGER = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
      ELSE sls_due_dt::DATE
    END sls_due_dt,

    CASE
      WHEN sls_sales IS NULL OR sls_sales::INTEGER <=0 OR sls_sales::INTEGER != sls_quantity::INTEGER * ABS(sls_price::INTEGER)
        THEN sls_quantity::INTEGER * ABS(sls_price::INTEGER)
      ELSE sls_sales::INTEGER
    END sls_sales,

    sls_quantity::INTEGER,

    CASE
      WHEN sls_price IS NULL OR sls_price::INTEGER <= 0
        THEN sls_sales::INTEGER / NULLIF(sls_quantity::INTEGER, 0)
      ELSE sls_price::INTEGER
    END sls_price
    FROM bronze.crm_sales_details;

    -- load silver.erp_cust_az12
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE silver.erp_cust_az12 CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(
      cid,
      bdate,
      gen
    )
    SELECT
    CASE
      WHEN cid LIKE 'NA%' THEN SUBSTRING(cid, 4, LENGTH(cid))
      ELSE cid
    END AS cid,
    CASE
      WHEN bdate::DATE > NOW() OR bdate::DATE < '1924-01-01' THEN NULL
      ELSE bdate::DATE
    END bdate,
    CASE
      WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
    END gen
    FROM bronze.erp_cust_az12;

    -- load silver.erp_loc_a101
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE silver.erp_loc_a101 CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT 
    REPLACE(cid, '-', '') cid,

    CASE
      WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
      WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
      WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
      ELSE TRIM(cntry)
    END cntry
    FROM bronze.erp_loc_a101;

    -- load silver.erp_px_cat_g1v2
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE silver.erp_px_cat_g1v2 CASCADE;
    RAISE NOTICE '>> Inserting data into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(id, cat,subcat,maintenance)

    SELECT
      id,
      TRIM(cat) AS cat,
      TRIM(subcat) as subcat,
      TRIM(maintenance) AS maintenance
    FROM bronze.erp_px_cat_g1v2;
	END;
$$;

CALL silver.load_silver();