\connect bike_sales_data_warehouse;

CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr VARCHAR(50),
  cst_create_date VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
  prd_id INT,
  prd_key VARCHAR(50),
  prd_nm VARCHAR(50),
  prd_cost INT,
  prd_line VARCHAR(50),
  prd_start_dt VARCHAR(50),
  prd_end_dt VARCHAR(50)
);

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

CREATE TABLE  IF NOT EXISTS bronze.erp_cust_az12 (
  cid VARCHAR(50),
  bdate VARCHAR(50),
  gen VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
  cid VARCHAR(50),
  cntry VARCHAR(50)
);

CREATE TABLE  IF NOT EXISTS bronze.erp_px_cat_g1v2 (
  id VARCHAR(50),
  cat VARCHAR(50),
  subcat VARCHAR(50),
  maintenance VARCHAR(50)
);


