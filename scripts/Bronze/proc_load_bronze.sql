/*
===================================================================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
===================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None
  This stored procedure does not accept any parameter or return any values.

Usage Example:
  EXEC bronze.load_bronze;
===================================================================================================
*/
CREATE OR ALTER PROCEDURE BRONZE.LOAD_BRONZE AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();
		print'=========================================================================';
		print'Loading Bronze Layer';
		print'=========================================================================';

		print '------------------------------------------------------------------------';
		print 'Loading CRM Table';
		print '------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_cust_info' 
		truncate table [Bronze].[crm_cust_info];

		print '>>Bulk Inserting data: bronze.crm_cust_info'
		bulk insert [Bronze].[crm_cust_info]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_crm\cust_info.csv'
		with (
		firstrow = 2,    --FROM 2ND ROW IT STARTS
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_prd_info'
		truncate table [Bronze].[crm_prd_info];

		print '>>Bulk Inserting data: bronze.crm_prd_info'
		bulk insert [Bronze].[crm_prd_info]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_crm\prd_info.csv'
		with (
		firstrow = 2,		--ROW STARTS FROM 2ND
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_sales_details'
		truncate table  [Bronze].[crm_sales_details]

		print '>>Bulk Inserting data: bronze.crm_sales_details'
		bulk insert [Bronze].[crm_sales_details]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_crm\sales_details.csv'
		with (
		firstrow = 2,		--ROW STARTS FROM 2ND
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		print '------------------------------------------------------------------------';
		print 'Loading ERP Table';
		print '------------------------------------------------------------------------';
		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_cust_AZ12'
		truncate table [Bronze].[erp_cust_az12]

		print '>>Bulk Inserting data: bronze.erp_cust_AZ12'
		bulk insert [Bronze].[erp_cust_az12]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,			--ROW STARTS FROM 2ND
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_loc_A101'
		truncate table [Bronze].[erp_loc_a101]

		print '>>Bulk Inserting data: bronze.erp_loc_A101'
		bulk insert [Bronze].[erp_loc_a101]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,       --ROW STARTS FROM 2ND
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_PX_CAT_G1V2'
		truncate table [Bronze].[erp_px_cat_g1v2]
		print '>>Bulk Inserting data: bronze.erp_PX_CAT_G1V2'
		bulk insert [Bronze].[erp_px_cat_g1v2]
		from 'C:\Users\MSVPraveenPallapothu\sql\dwh_project\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,   --ROW STARTS FROM 2ND
		fieldterminator = ',',
		tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> --------------------------------------------------------------------------';

		set @batch_end_time = GETDATE();
		print '========================================================================================='
		print 'Loading Bronze Layer is Completed';
		    print '      -Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
    print '========================================================================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER '
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ cast (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================================================================='
	END CATCH
END
