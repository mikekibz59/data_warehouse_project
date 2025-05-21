/*====================================================================
  Re-creates the **Silver layer** tables (lightly cleansed / typed
  staging) in the `bike_sales_data_warehouse` database.

  WHY A SILVER LAYER?
  • Keeps raw Bronze data unchanged while providing strongly-typed
    tables for business logic and analytics.
  • Adds a `dwh_create_date` timestamp on every row for lineage /
    reload troubleshooting.
  • Keeps surrogate-key friendly integer types where possible.

  WHAT THIS SCRIPT DOES
  • Opens an explicit transaction → all tables are dropped & rebuilt
    together (or nothing changes if an error occurs).
  • For each table:
        1. `CREATE TABLE …` with
             – Correct data types (`DATE`, `BIGINT`, `INT` vs VARCHAR).
             – `dwh_create_date TIMESTAMP DEFAULT NOW()`.
  • Tables refreshed
        ─ silver.crm_cust_info
        ─ silver.crm_prd_info
        ─ silver.crm_sales_details
        ─ silver.erp_cust_az12
        ─ silver.erp_loc_a101
        ─ silver.erp_px_cat_g1v2
====================================================================*/

\connect bike_sales_data_warehouse;

BEGIN;
    \echo '>> Dropping Table: silver.crm_cust_info';
    DROP TABLE IF EXISTS silver.crm_cust_info CASCADE;
    \echo '>> Creating Table: silver.crm_cust_info';
    CREATE TABLE silver.crm_cust_info (
        cst_id INT,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE,
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    \echo '>> Dropping Table: silver.crm_prd_info';
    DROP TABLE IF EXISTS silver.crm_prd_info CASCADE;
    \echo '>> Creating Table: silver.crm_prd_info';
    CREATE TABLE silver.crm_prd_info (
        prd_id INT,
        prd_key VARCHAR(50),
        cat_id VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    \echo '>> Dropping Table: silver.crm_sales_details';
    DROP TABLE IF EXISTS silver.crm_sales_details CASCADE;
    \echo '>> Creating Table: silver.crm_sales_details';
    CREATE TABLE silver.crm_sales_details (
        sls_order_nm VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INT,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales BIGINT,
        sls_quantity INT,
        sls_price BIGINT,
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    \echo '>> Dropping Table: silver.erp_cust_az12';
    DROP TABLE IF EXISTS silver.erp_cust_az12 CASCADE;
    \echo '>> Creating Table: silver.erp_cust_az12';
    CREATE TABLE  silver.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    \echo '>> Dropping Table: silver.erp_loc_a101';
    DROP TABLE IF EXISTS silver.erp_loc_a101 CASCADE;
    \echo '>> Creating Table: silver.erp_loc_a101';
    CREATE TABLE silver.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    \echo '>> Dropping Table: silver.erp_px_cat_g1v2';
    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2 CASCADE;
    \echo '>> Creating Table: silver.erp_px_cat_g1v2';
    CREATE TABLE  silver.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT NOW()
    );
COMMIT;


