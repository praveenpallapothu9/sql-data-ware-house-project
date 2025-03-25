/*
========================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================================================
Script Purpose:
  This stored Procedure performs the ETL (Extract, Transform, Load) process to 
  Populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates Silver Tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This is stored procedure does not accept any parameter or return any values.

Usage example
  Exec silver.load_silver
========================================================================================================  
*/

--stored procedure

create or alter procedure Silver.load_silver as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		--truncate the table  which remains the structure
		--INSERTING THE DATA WITH A PROPER FORMAT AND DATA WITHOUT THE NUMBER OF TIMES THE DATA DUPLICATED
		--BATCH TIME FORMATION
		SET @batch_start_time = GETDATE();
		print'=========================================================================';
		print'Loading Silver Layer';
		print'=========================================================================';

		print '------------------------------------------------------------------------';
		print 'Loading CRM Table';
		print '------------------------------------------------------------------------';

		--Loading silver.crm_cust_info
		set @start_time = GETDATE();
		print'>>Truncating Table: silver.crm_cust_info'
		truncate table Silver.crm_cust_info;
		print '>>Inserting Data Into: silver.crm_cust_info'
		insert into Silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select 
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname,
				trim(cst_lastname) as cst_lastname,
				case 
					when upper(trim(cst_marital_status)) = 'M' then 'Married'
					when upper(trim(cst_marital_status)) = 'S' then 'Single'
					else 'n/a'
				end as cst_marital_status,
				case 
					when upper(trim(cst_gndr)) = 'M' then 'Male'
					when upper(trim(cst_gndr)) = 'F' then 'Female'
					else 'n/a'
				end as cst_gndr,cst_create_date
		from ( 
			select 
				*,
				ROW_NUMBER() over(partition by cst_id order by cst_id desc) as flag_last 
			from 
				bronze.crm_cust_info
			where cst_id is not null
			) t 
			where flag_last = 1 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		print('-------------------------------------------------------------------------------')

		--Silver.crm_prd_info

		/*if OBJECT_ID ('silver.crm_prd_info','U') is not null
			drop table silver.crm_prd_info;
		--CREATE A TABLE
		create table silver.crm_prd_info (
			prd_id int,
			cat_id nvarchar(50),
			prd_key nvarchar(50),
			prd_nm nvarchar(50),
			prd_cost int,
			prd_line nvarchar(50),
			prd_start_dt date,
			prd_end_dt date,
			dwh_create_date Datetime2 default getdate()
		);*/
		set @start_time = GETDATE();
		print'>>Truncating Table: Silver.crm_prd_info'
		truncate table Silver.crm_prd_info;
		print '>>Inserting Data Into: Silver.crm_prd_info'
		insert into Silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
		)	
		select 
				prd_id,
				replace(substring(prd_key,1,5),'-','_')as cat_id,			--Extract category id
				SUBSTRING(prd_key,7,len(prd_key)) as prd_key,				--Extract product key
				prd_nm,
				isnull(prd_cost,0) as prd_cost,
				case 
					when upper(trim(prd_line)) = 'M' then 'Mountain'
					when upper(trim(prd_line)) = 'R' then 'Road'
					when upper(trim(prd_line)) = 'S' then 'Other Sales'
					when upper(trim(prd_line)) = 'T' then 'Touring'
					else 'n/a'
				end as prd_line,
				cast(prd_start_dt as date) as prd_strt_dt,
				cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt_test    --we will be displaying only the date not the time.
		from 
		Bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';


		print('-------------------------------------------------------------------------------')

		--Silver.crm_sales_details
		/*
		if object_id ('Silver.crm_sales_details','U') is not null
			drop table Silver.crm_sales_details;
		create table Silver.crm_sales_details(
				sls_ord_num nvarchar(50),
				sls_prd_key nvarchar(50),
				sls_cust_id int,
				sls_order_dt date,
				sls_ship_dt date,
				sls_due_dt date,
				sls_sales int,
				sls_quantity int,
				sls_price int,
				dwh_create_date datetime2 default getdate()
			);
			*/
		set @start_time = GETDATE();
		print'>>Truncating Table: Silver.crm_sales_details'
		truncate table Silver.crm_sales_details;
		print '>>Inserting Data Into: Silver.crm_sales_details'
		insert into Silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price)
		select 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					case 
						when sls_order_dt <= 0 or len(sls_order_dt) != 8 then null
						else cast(cast(sls_order_dt as varchar)as date)
					end as sls_order_dt,
					case
						when sls_ship_dt < 0 or len(sls_ship_dt) != 8 then null
						else cast(cast(sls_ship_dt as varchar)as date)
					end as sls_ship_dt,
					case
						when sls_due_dt < 0 or len(sls_due_dt) != 8 then null
						else cast(cast(sls_due_dt as varchar)as date)
					end as sls_due_dt,
					case 
						when sls_sales is null or  sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
						then sls_quantity * ABS(sls_price)
						else sls_sales 
					end as sls_sales,
					sls_quantity,
					case 
						when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity,0)
						else sls_price
					end as sls_price
		from 
		Bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';

		print('-------------------------------------------------------------------------------')

		--Silver.erp_cust_az12
		
		/*
		if object_id ('Silver.erp_cust_az12','U') is not null
			drop table Silver.erp_cust_az12;
		--CREATES A TABLE
		create table Silver.erp_cust_az12 (
			cid nvarchar(50),
			bdate date,
			gen nvarchar(50),
			dwh_create_date Datetime2 default getdate()
		);
		*/
		set @start_time = GETDATE();
		print'>>Truncating Table: Silver.erp_cust_az12'
		truncate table Silver.erp_cust_az12;
		print '>>Inserting Data Into: Silver.erp_cust_az12'
		insert into Silver.erp_cust_az12(
				cid,
				bdate,
				gen
				)
		select 
				case 
					when cid like 'NAS%' then substring(cid,4,len(cid))
					else cid
				end as cid,
				case 
					when bdate > getdate() then null
					else bdate
				end as bdate,
				case
					when upper(trim(gen)) in ('F','Female') then 'Female'
					when upper(trim(gen)) in ('M','Male') then 'Male'
					else 'n/a'
				end as gen
		from Bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';


		print('-------------------------------------------------------------------------------')

		--Silver.erp_loc_a101
		/*
		if object_id ('silver.erp_loc_a101','U') is not null
			drop table Silver.erp_loc_a101;
		--CREATES A TABLE
		create table Silver.erp_loc_a101 (
			cid nvarchar(50),
			cntry nvarchar(50),
			dwh_create_date Datetime2 default getdate()
		);
		*/
		set @start_time = getdate();
		print'>>Truncating Table: Silver.erp_loc_a101'
		truncate table Silver.erp_loc_a101;
		print '>>Inserting Data Into: Silver.erp_loc_a101'
		insert into Silver.erp_loc_a101
						(
						cid,
						cntry
						)
		select replace(cid,'-','') as cid,
				case
				when trim(cntry) ='DE' then 'Germany'
				when trim(cntry) in ('US','USA') then 'United States'
				when trim(cntry) = ' ' or cntry is null then 'n/a'
				else trim(cntry)
		end as cntry			--normalize and handle missing or blank country codes
		from Bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';


		print('-------------------------------------------------------------------------------')

		--Silver.erp_px_cat_g1v2
		/*if object_id ('silver.erp_px_cat_g1v2','U') is not null
			drop table silver.erp_px_cat_g1v2;
		--CREATES A TABLE
		create table silver.erp_px_cat_g1v2 (
			id nvarchar(50),
			cat nvarchar(50),
			subcat nvarchar(50),
			maintainence nvarchar(50),
			dwh_create_date Datetime2 default getdate()
		);
		*/
		set @start_time = getdate();
		print'>>Truncating Table: Silver.erp_px_cat_g1v2'
		truncate table Silver.erp_px_cat_g1v2;
		print '>>Inserting Data Into: Silver.erp_px_cat_g1v2'
		insert into Silver.erp_px_cat_g1v2
		(			id,
					cat,
					subcat,
					maintainence)
		select	id,
				cat,
				subcat,
				maintainence 
		from Bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '--------------------------------------------------------------------------';
	end try
	Begin catch
		PRINT '=========================================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER '
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ cast (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================================================================='
	end catch
end


exec Silver.load_silver
