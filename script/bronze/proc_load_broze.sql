CREATE or ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT'===================================================================================';
	PRINT 'Loading data into Bronze layer...';
	PRINT '===================================================================================';

	PRINT'------------------------------------------------------------------------------------';
	PRINT'LOARDING CRM TABLE';
	PRINT'-------------------------------------------------------------------------------------';

 
 SET @start_time = GETDATE();
	 PRINT '>>TRUNCATING TABLE: bronze.crm.info';
		 TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>>Inserting data into bronze.crm_cust_info from cust_info.csv';

		 BULK INSERT bronze.crm_cust_info
		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_crm\cust_info.csv'
		 WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';




	SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE: bronze.crm_prd_info';
		 TRUNCATE TABLE bronze.crm_prd_info;
    PRINT '>>Inserting data into bronze.crm_prd_info from prd_info.csv';  
		 BULK INSERT bronze.crm_prd_info

		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_crm\prd_info.csv'
		 WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 );
		 SET @end_time = GETDATE();
   PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';



   SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
         TRUNCATE TABLE bronze.crm_sales_details;
   PRINT '>>Inserting data into bronze.crm_sales_details from sales_details.csv';
		 BULK INSERT bronze.crm_sales_details
		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_crm\sales_details.csv'
		 WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 );
		 SET @end_time = GETDATE();
 PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';


		 PRINT'------------------------------------------------------------------------------------';
	PRINT'LOARDING ERP TABLE';
	PRINT'-------------------------------------------------------------------------------------';

 
         SET @start_time = GETDATE();
	 PRINT '>>TRANCATING TABLE beonze.erp_cust_az12';


		 TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT '>>Inserting data into bronze.erp_cust_az12 from cust_az12.csv';
		 BULK INSERT bronze.erp_cust_az12
		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_erp\cust_az12.csv'
		 WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 );
		 SET @end_time = GETDATE();
  PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';




       SET @start_time = GETDATE();
	PRINT '>>TRANCATING TABLE: bronze.erp_loc_a101';
		 TRUNCATE TABLE bronze.erp_loc_a101;
    PRINT '>>Inserting data into bronze.erp_loc_a101 from loc_a101.csv';
		 BULK INSERT bronze.erp_loc_a101
		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_erp\loc_a101.csv'
		 WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 );
		SET @end_time = GETDATE();
	PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';



	 SET @start_time = GETDATE();
 	PRINT '>>TRANCATING TABLE: bronze.erp_px_cat_g1v2';
		 TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
   PRINT '>>Inserting data into bronze.erp_px_cat_g1v2 from px_cat_g1v2.csv';
		 BULK INSERT bronze.erp_px_cat_g1v2
		 FROM 'C:\Users\wwww\OneDrive\Documents\DWH_project\datasets\source_erp\px_cat_g1v2.csv'
		  WITH
		 (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'load duration: ' + CasT(DATEDIFF(SECOND,@start_time, @end_time)AS NVARCHAR)+ ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT '==================================================================';
		PRINT 'LOADING BROZE IS COMPLETED';
		PRINT 'Total time taken to load data into Bronze layer: ' + CasT(DATEDIFF(SECOND,@batch_start_time, @batch_end_time)AS NVARCHAR)+ ' seconds';
		PRINT '==================================================================';
	END TRY
	BEGIN CATCH
	PRINT 'Error occurred while loading data into Bronze layer: ' + ERROR_MESSAGE();
	END CATCH
END
