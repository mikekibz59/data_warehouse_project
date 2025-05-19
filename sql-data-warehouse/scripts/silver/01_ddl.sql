\connect bike_sales_data_warehouse;

BEGIN;
    DROP TABLE IF EXISTS sliver.crm_cust_info CASCADE;
    CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    DROP TABLE IF EXISTS sliver.crm_prd_info CASCADE;
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

    DROP TABLE IF EXISTS silver.crm_sales_details CASCADE;
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

    DROP TABLE IF EXISTS silver.erp_cust_az12;
    CREATE TABLE  silver.erp_cust_az12 (
    cid VARCHAR(50),
    bdate VARCHAR(50),
    gen VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    DROP TABLE IF EXISTS silver.erp_loc_a101;
    CREATE TABLE silver.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
    );

    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
    CREATE TABLE  silver.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
    );
COMMIT;


