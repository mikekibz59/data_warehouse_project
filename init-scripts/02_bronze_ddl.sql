/*====================================================================
  02_create_bronze_tables.sql
  --------------------------------------------------------------------
  Rebuilds the **Bronze layer** (raw-landing tables) in the
  `bike_sales_data_warehouse` database.

  • Executes inside a single transaction → all tables are recreated
    together or the script rolls back on error.

  • For each staging table it
        1. drops the old table;
        2. recreates the table with columns as-received from the
           source CSV/ERP feeds (all strings / ints, no cleansing).

      Tables refreshed
        - bronze.crm_cust_info
        - bronze.crm_prd_info
        - bronze.crm_sales_details
        - bronze.erp_cust_az12
        - bronze.erp_loc_a101
        - bronze.erp_px_cat_g1v2

  • Keeps the Bronze layer schema identical to the raw files so that
    later Silver/Gold transforms can enforce proper types and business
    rules without risk of partial mismatch.
====================================================================*/
\connect bike_sales_data_warehouse;

BEGIN;
  \echo '>> Dropping Table: bronze.crm_cust_info';
  DROP TABLE IF EXISTS bronze.crm_cust_info;
  \echo '>> Creating Table: bronze.crm_cust_info';
  CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date VARCHAR(50)
  );

  \echo '>> Dropping Table: bronze.crm_prd_info';
  DROP TABLE IF EXISTS bronze.crm_prd_info;
  \echo '>> Creating Table: bronze.crm_prd_info';
  CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt VARCHAR(50),
    prd_end_dt VARCHAR(50)
  );

  \echo '>> Dropping Table: bronze.crm_sales_details';
  DROP TABLE IF EXISTS bronze.crm_sales_details;
  \echo '>> Creating Table: bronze.crm_sales_details';
  CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_order_nm VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id VARCHAR(50),
    sls_order_dt VARCHAR(50),
    sls_ship_dt VARCHAR(50),
    sls_due_dt VARCHAR(50),
    sls_sales VARCHAR(50),
    sls_quantity VARCHAR(50),
    sls_price VARCHAR(50)
  );

  \echo '>> Dropping Table: bronze.erp_cust_az12';
  DROP TABLE IF EXISTS bronze.erp_cust_az12;
  \echo '>> Creating Table: bronze.erp_cust_az12';
  CREATE TABLE  IF NOT EXISTS bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate VARCHAR(50),
    gen VARCHAR(50)
  );

  \echo '>> Dropping Table: bronze.erp_loc_a101';
  DROP TABLE IF EXISTS bronze.erp_loc_a101;
  \echo '>> Creating Table: bronze.erp_loc_a101';
  CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
  );

  \echo '>> Dropping Table: bronze.erp_px_cat_g1v2';
  DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
  \echo '>> Creating Table: bronze.erp_px_cat_g1v2';
  CREATE TABLE  IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
  );

COMMIT;


