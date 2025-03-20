/*
======================================================================================================
DDL Script: Create Bronze Tables
======================================================================================================
Script Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables
  if they already exist.
Run this script to re-define the ddl Structure of 'bronze' Tables

======================================================================================================
*/

if OBJECT_ID ('bronze.crm_cust_info','U') is not null
	drop table bronze.crm_cust_info;
GO
--CREATES A TABLE 
create table bronze.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(10),
	cst_gndr nvarchar(10),
	cst_create_date date
);
GO
--IF THE DATABASE IS NOT NUL THE IT WILL DELETE THE ENTIRE DATA FROM THE TABLE AND REINSERT THE DATA FROM THE LOCATION
if OBJECT_ID ('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info;
GO
--CREATE A TABLE
create table bronze.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);
GO
--IF THE DATABASE IS NOT NUL THE IT WILL DELETE THE ENTIRE DATA FROM THE TABLE AND REINSERT THE DATA FROM THE LOCATION
if OBJECT_ID ('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;
GO
--CREATES A TABLE
create table bronze.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
GO
--create table erp data
--IF THE DATABASE IS NOT NUL THE IT WILL DELETE THE ENTIRE DATA FROM THE TABLE AND REINSERT THE DATA FROM THE LOCATION
if object_id ('bronze.erp_loc_a101','U') is not null
	drop table bronze.erp_loc_a101;
GO
  --CREATES A TABLE
create table bronze.erp_loc_a101 (
	cid nvarchar(50),
	cntry nvarchar(50)
);
GO
--IF THE DATABASE IS NOT NUL THE IT WILL DELETE THE ENTIRE DATA FROM THE TABLE AND REINSERT THE DATA FROM THE LOCATION
if object_id ('bronze.erp_cust_az12','U') is not null
	drop table bronze.erp_cust_az12;
GO
--CREATES A TABLE
create table bronze.erp_cust_az12 (
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);
GO
--IF THE DATABASE IS NOT NUL THE IT WILL DELETE THE ENTIRE DATA FROM THE TABLE AND REINSERT THE DATA FROM THE LOCATION
if object_id ('bronze.erp_px_cat_g1v2','U') is not null
	drop table bronze.erp_px_cat_g1v2;
GO
  --CREATES A TABLE
create table bronze.erp_px_cat_g1v2 (
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintainence nvarchar(50)
);
GO
