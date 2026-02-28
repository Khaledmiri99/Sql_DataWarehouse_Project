/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

--Creating a stored procedure that loads raw data from CSV files into the tables
--Refreshing the tables and Loading raw data into them from the CSV files

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$

DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	bronze_layer_loading_start_time TIMESTAMP;
	bronze_layer_loading_end_time TIMESTAMP;
	crm_cust_info_load_duration BIGINT;
	crm_prd_info_load_duration BIGINT;
	crm_sales_details_load_duration BIGINT;
	erp_cust_az12_load_duration BIGINT;
	erp_loc_a101_load_duration BIGINT;
	erp_px_cat_g1v2_load_duration BIGINT;
	bronze_layer_load_duration BIGINT;
	
	
BEGIN
	BEGIN  

		bronze_layer_loading_start_time:=NOW();
		RAISE NOTICE 'Loading the bronze layer started at %', bronze_layer_loading_start_time;
		RAISE NOTICE '====================================';
		RAISE NOTICE 'Loading the bronze layer';
		RAISE NOTICE '====================================';
	
	
		RAISE NOTICE '---------------------------';
		RAISE NOTICE 'Loading CRM tables';
		RAISE NOTICE '---------------------------';

		start_time:=NOW();
		RAISE NOTICE 'Loading crm_cust_info table started at %', start_time;
		
		RAISE NOTICE 'Truncating crm_cust_info table';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		RAISE NOTICE 'Inserting Data into: bronze.crm_cust_info';
		copy bronze.crm_cust_info 
			FROM 'C:/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);
			
		end_time:=NOW();
		RAISE NOTICE 'Loading crm_cust_info table ended at %', end_time;
		crm_cust_info_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', crm_cust_info_load_duration;
		RAISE Notice '------------------';

		
		start_time:=NOW();
		RAISE NOTICE 'Loading crm_prd_info table started at %', start_time;
		RAISE NOTICE 'Truncating crm_prd_info table';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		RAISE NOTICE 'Inserting Data into: bronze.crm_prd_info';
		copy bronze.crm_prd_info 
			FROM 'C:/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);
		end_time:=NOW();
		RAISE NOTICE 'Loading crm_prd_info table ended at %', end_time;
		crm_prd_info_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', crm_prd_info_load_duration;
		RAISE Notice '------------------';
		

		start_time:=NOW();
		RAISE NOTICE 'Loading crm_sales_details table started at %', start_time;
		RAISE NOTICE 'Truncating crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		RAISE NOTICE 'Inserting Data into: bronze.crm_sales_details';
		copy bronze.crm_sales_details 
			FROM 'C:/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);
		end_time:=NOW();
		RAISE NOTICE 'Loading crm_sales_details table ended at %', end_time;
		crm_sales_details_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', crm_sales_details_load_duration;
		RAISE Notice '------------------';

	
		RAISE NOTICE '---------------------------';
		RAISE NOTICE 'Loading ERP tables';
		RAISE NOTICE '---------------------------';

		start_time:=NOW();
		RAISE NOTICE 'Loading erp_cust_az12 table started at %', start_time;
		RAISE NOTICE 'Truncating erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		RAISE NOTICE 'Inserting Data into: bronze.erp_cust_az12';
		copy bronze.erp_cust_az12 
			FROM 'C:/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);
		end_time:=NOW();
		RAISE NOTICE 'Loading erp_cust_az12 table ended at %', end_time;
		erp_cust_az12_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', erp_cust_az12_load_duration;
		RAISE Notice '------------------';


		start_time:=NOW();
		RAISE NOTICE 'Loading erp_loc_a101 table started at %', start_time;
		RAISE NOTICE 'Truncating erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		RAISE NOTICE 'Inserting Data into: bronze.erp_loc_a101';
		copy bronze.erp_loc_a101 
			FROM 'C:/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);

		end_time:=NOW();
		RAISE NOTICE 'Loading erp_loc_a101 table ended at %', end_time;
		erp_loc_a101_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', erp_loc_a101_load_duration;
		RAISE Notice '------------------';


		start_time:=NOW();
		RAISE NOTICE 'Loading erp_px_cat_g1v2 table started at %', start_time;
		RAISE NOTICE 'Truncating erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		RAISE NOTICE 'Inserting Data into: bronze.erp_px_cat_g1v2';
		copy bronze.erp_px_cat_g1v2 
			FROM 'C:/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
			WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ',',
			QUOTE '"',
			ESCAPE '/'
			);
		
		end_time:=NOW();
		RAISE NOTICE 'Loading erp_px_cat_g1v2 table ended at %', end_time;
		erp_px_cat_g1v2_load_duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> crm_cust_info table Load Duration: % seconds', erp_px_cat_g1v2_load_duration;
		RAISE Notice '------------------';
	
		RAISE NOTICE '====================================';
		RAISE NOTICE 'Bronze layer loading is complete !';
		RAISE NOTICE '====================================';

		bronze_layer_loading_end_time:=NOW();
		RAISE NOTICE 'Loading the bronze layer ended at %', end_time;
		bronze_layer_load_duration := EXTRACT(EPOCH FROM (bronze_layer_loading_end_time - bronze_layer_loading_start_time));
		RAISE NOTICE '>> the bronze layer Loading Duration: % seconds', bronze_layer_load_duration;
		RAISE Notice '------------------';
		
	EXCEPTION

		WHEN others THEN
		
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'Error Message :%', SQLERRM;
		RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
		RAISE NOTICE '-------------------------------------------';
	END;
END;
$$;
