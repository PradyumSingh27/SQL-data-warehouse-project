/*
==========================================================
Create Database and Schemas
==========================================================
Script purpose:
   This script create a new database named 'DataWarehouse' after checking if it already exists.
   If the database exists, it is dropped and recreate. Additional, the script sets up three schemas 
   within the database: 'bronze','silver','gold'.

WRNING:
   Running this script will drop the entire 'DataWarehouse' databaseif it exists.
   All data in the database will be permanently deleted. Proceed with caution
   and ensure you have proper backups before running this script.

*/
USE master;
GO

-- Drop and recreate the 'DataWareHouse' database
if EXISTS (select 1 from sys.databases where name = 'DataWarehouse')
BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
