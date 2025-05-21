\connect bike_sales_data_warehouse;

-- CRM
COPY bronze.crm_cust_info     FROM '/docker-entrypoint-initdb.d/datasets/source_crm/cust_info.csv'   WITH (FORMAT csv, HEADER true);
COPY bronze.crm_prd_info      FROM '/docker-entrypoint-initdb.d/datasets/source_crm/prd_info.csv'    WITH (FORMAT csv, HEADER true);
COPY bronze.crm_sales_details FROM '/docker-entrypoint-initdb.d/datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);

-- ERP
COPY bronze.erp_cust_az12  FROM '/docker-entrypoint-initdb.d/datasets/source_erp/CUST_AZ12.csv'  WITH (FORMAT csv, HEADER true);
COPY bronze.erp_loc_a101   FROM '/docker-entrypoint-initdb.d/datasets/source_erp/LOC_A101.csv'   WITH (FORMAT csv, HEADER true);
COPY bronze.erp_px_cat_g1v2 FROM '/docker-entrypoint-initdb.d/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true);


