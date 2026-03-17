 /*
This script creates a Data Warehouse database with three schemas: Bronze, Silver, and Gold.

WARNING: Before running this script, ensure that you have the necessary permissions to create databases
and schemas on the SQL Server instance. Additionally, make sure to review and modify the database 
name and schema names as needed to fit your specific requirements.
*/



USE master;
Go 

-- Drop and recrete the DataWareHouse database 
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWareHouse')	
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
Go

-- Create the 'DataWareHouse' database
CREATE DATABASE DataWareHouse;
GO


USE DataWareHouse;
GO

--Create the Bronze, Silver, and Gold schemas


CREATE SCHEMA bronze;
GO


CREATE SCHEMA Silver;
GO


CREATE SCHEMA Gold;
GO



