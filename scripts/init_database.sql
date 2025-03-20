/*
==============================================================================================
Create Database and Schemas
==============================================================================================
Script Purpose:
  This script creates a new database named 'DataWareHouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the scripts up three schemas
  within the database: 'Bronze','Silver','Gold'

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it is already exists.
  All data in the database will be permanenetly deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

--CREATE A DATABASE WE USE THIS.
USE master;
GO

--DROP AND RECREATE THE DATABASE IF IT IS EXISTING 
IF EXISTS (SELECT 1 FROM sys.databases where name = 'DataWareHouse')
BEGIN
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWareHouse;
END;
GO

--create the 'DataWareHouse' database
create DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

--create schema
create schema bronze;
GO

create schema silver;
GO

create schema gold;
Go
