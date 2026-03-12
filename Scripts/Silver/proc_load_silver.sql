/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
/*

--Creating Stored Procedure for loading the silver layer's tables.


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$

DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	silver_layer_loading_start_time TIMESTAMP;
	silver_layer_loading_end_time TIMESTAMP;
	crm_cust_info_load_duration BIGINT;
	crm_prd_info_load_duration BIGINT;
	crm_sales_details_load_duration BIGINT;
	erp_cust_az12_load_duration BIGINT;
	erp_loc_a101_load_duration BIGINT;
	erp_px_cat_g1v2_load_duration BIGINT;
	silver_layer_load_duration BIGINT;
	
	BEGIN


		silver_layer_loading_start_time:=NOW();
		RAISE NOTICE '====================================';
		RAISE NOTICE 'Loading the silver layer started at %', silver_layer_loading_start_time;
		RAISE NOTICE '====================================';
		RAISE NOTICE 'Loading the silver layer';
		RAISE NOTICE '====================================';
	
	
		RAISE NOTICE '---------------------------';
		RAISE NOTICE 'Loading CRM tables';
		RAISE NOTICE '---------------------------';

		start_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_cust_info table started at %', start_time;
		RAISE NOTICE 'Truncating silver.crm_cust_info table';
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE NOTICE 'Inserting Data into silver.crm_cust_info';
		
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,	
			cst_gndr,
			cst_create_date)
		
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		
		CASE WHEN UPPER (TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER (TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END
		cst_marital_status,
		
		CASE WHEN UPPER (TRIM(cst_gndr)) ='F' 
				THEN 'Female'
			WHEN UPPER (TRIM(cst_gndr)) ='M' 
				THEN 'Male'
			ELSE 'N/A'
		END
		cst_gndr,
		cst_create_date
		FROM (
			SELECT*, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL)
		WHERE flag_last = 1;

		end_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_cust_info table ended at %', end_time;
		crm_cust_info_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.crm_cust_info table Load Duration: % seconds', crm_cust_info_load_duration;
		RAISE Notice '------------------';

		
		
		start_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_prd_info table started at %', start_time;
		RAISE NOTICE 'Truncating silver.crm_prd_info table';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE NOTICE 'Inserting Data into silver.crm_prd_info';
		
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
		REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING (prd_key, 7, LENGTH(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'M' THEN 'Mountain'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		prd_start_dt,
		LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
		FROM bronze.crm_prd_info;

		end_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_prd_info table ended at %', end_time;
		crm_prd_info_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.crm_prd_info table Load Duration: % seconds', crm_prd_info_load_duration;
		RAISE Notice '------------------';

		
		
		start_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_sales_details table started at %', start_time;
		RAISE NOTICE 'Truncating silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE 'Inserting Data into silver.crm_sales_details';
		
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
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
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			
			CASE WHEN sls_order_dt =0 OR LENGTH(sls_order_dt::text)!=8 
					THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			
			CASE WHEN sls_ship_dt =0 OR LENGTH(sls_ship_dt::text)!=8 
					THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			
			CASE WHEN sls_due_dt =0 OR LENGTH(sls_due_dt::text)!=8 
					THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			
			sls_quantity,
			
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
			
		FROM bronze.crm_sales_details;
		
		end_time:=NOW();
		RAISE NOTICE 'Loading silver.crm_sales_details table ended at %', end_time;
		crm_sales_details_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.crm_sales_details table Load Duration: % seconds', crm_sales_details_load_duration;
		RAISE Notice '------------------';

		RAISE NOTICE '---------------------------';
		RAISE NOTICE 'Loading ERP tables';
		RAISE NOTICE '---------------------------';

		
		start_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_cust_az12 table started at %', start_time;
		RAISE NOTICE 'Truncating silver.erp_cust_az12 table';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE 'Inserting Data into silver.erp_cust_az12';
		
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%'
				THEN SUBSTRING (cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		
		CASE WHEN bdate > NOW() 
				THEN NULL
			ELSE bdate
		END AS bdate,
		
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'N/A'
		END AS gen
		
		FROM bronze.erp_cust_az12;

		end_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_cust_az12 table ended at %', end_time;
		erp_cust_az12_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.erp_cust_az12 table Load Duration: % seconds', erp_cust_az12_load_duration;
		RAISE Notice '------------------';
		

		start_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_loc_a101 table started at %', start_time;
		RAISE NOTICE 'Truncating silver.erp_loc_a101 table';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE 'Inserting Data into silver.erp_loc_a101';
		
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT
		REPLACE (cid, '-', ''),
		
		CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA')
				THEN 'United States'
			WHEN UPPER(TRIM(cntry)) ='DE'
				THEN 'Germany'
			WHEN TRIM(cntry) = '' OR cntry IS NULL
				THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;

		end_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_loc_a101 table ended at %', end_time;
		erp_loc_a101_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.erp_loc_a101 table Load Duration: % seconds', erp_loc_a101_load_duration;
		RAISE Notice '------------------';

		
		
		start_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_px_cat_g1v2 table started at %', start_time;
		RAISE NOTICE 'Truncating silver.erp_px_cat_g1v2 table';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE 'Inserting Data into silver.erp_px_cat_g1v2';
		
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

			end_time:=NOW();
		RAISE NOTICE 'Loading silver.erp_px_cat_g1v2 table ended at %', end_time;
		erp_px_cat_g1v2_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> silver.erp_px_cat_g1v2 table Load Duration: % seconds', erp_px_cat_g1v2_load_duration;
		RAISE Notice '------------------';
	
		RAISE NOTICE '====================================';
		RAISE NOTICE 'silver layer loading is complete !';
		RAISE NOTICE '====================================';

		silver_layer_loading_end_time:=NOW();
		RAISE NOTICE 'Loading the silver layer ended at %', end_time;
		silver_layer_load_duration := EXTRACT(EPOCH FROM (silver_layer_loading_end_time - silver_layer_loading_start_time));
		RAISE NOTICE '>> the silver layer Loading Duration: % seconds', silver_layer_load_duration;
		RAISE Notice '------------------';
		
	EXCEPTION

		WHEN OTHERS THEN
		
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING silver LAYER';
		RAISE NOTICE 'Error Message :%', SQLERRM;
		RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
		RAISE NOTICE '-------------------------------------------';
	
END;
$$;
