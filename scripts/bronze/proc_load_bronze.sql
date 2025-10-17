/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================';
		PRINT 'Loading Bronze layer';
		PRINT '======================';
		PRINT '-------------------';
		PRINT 'Loading CRM tables';
		PRINT '-------------------';
		PRINT '---------------------------------------';
		PRINT 'truncating table : bronze.crm_cust_info';
		PRINT '---------------------------------------';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '--------------------------------------';
		PRINT 'Inserting table : bronze.crm_cust_info';
		PRINT '--------------------------------------';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------';
		
		PRINT 'truncating table : bronze.crm_prd_info';
		SET @start_time = GETDATE();
		PRINT '-------------------';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '-------------------';
		PRINT 'Inserting table : bronze.crm_prd_info';
		PRINT '-------------------------------------';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'truncating table : bronze.crm_sales_details';
		PRINT '-------------------------------------------';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '-------------------------------------------';
		PRINT 'Inserting table : bronze.crm_sales_details';
		PRINT '-------------------------------------------';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------';
		PRINT 'Loading ERP tables';
		PRINT '-------------------';
		PRINT '-------------------------------------------';
		PRINT 'truncating table : bronze.erp_cust_az12';
		PRINT '-------------------------------------------';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '-------------------------------------------';
		PRINT 'Inserting table : bronze.erp_cust_az12';
		PRINT '-------------------------------------------';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------------';
		PRINT 'truncating table : bronze.erp_loc_a101';
		PRINT '-------------------------------------------';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '-------------------------------------------';
		PRINT 'Inserting table : bronze.erp_loc_a101';
		PRINT '-------------------------------------------';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		PRINT '-------------------------------------------';
		PRINT 'truncating table : bronze.erp_px_cat_g1v2';
		PRINT '-------------------------------------------';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '-------------------------------------------';
		PRINT 'Inserting table : bronze.erp_px_cat_g1v2';
		PRINT '-------------------------------------------';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\gawra\Downloads\workspace\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading duration:  ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		SET @batch_start_time = GETDATE();
		PRINT '-------------------------------------------';
		PRINT 'BATCH LOADING TIME IS CALCULATED BELOW';
		PRINT 'Total load duration Batch : ' + CAST(DATEDIFF(second,@batch_start_time,@batch_start_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '-----------------------------------------'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR(50));
	END CATCH
END
