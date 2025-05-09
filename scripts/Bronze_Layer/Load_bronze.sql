/*
======================================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
======================================================================================
Script purpose:
    This stored procedure loads data into the 'brinze' schema from external CSV Files.
    It performs the following actions:
    - Truncate the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV Files to brone table.

Parameters:
   None
  This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;
======================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		/*
		After Loading the Source into Table all check for the Data Quatity and IF youre working with CSv Files and it not load/insert the data so check for the FileDterminator "," sometime it not comma that why it give error 
		*/
		SET @batch_start_time = GETDATE();
		PRINT'==============================================';
		PRINT'Loadung Bronze Layer';
		PRINT'==============================================';
		-- print the source where you data to load in your layer
		PRINT'----------------------------------------------';
		PRINT'Loading CRM Table FORM CSV Files';
		PRINT'----------------------------------------------';
		-- First delet the content and then full insert the ccontent using bulk insert mathod
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT'>>>   -----------------    <<<';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> Inserting Data Into: BRONZE.CRM_PRD_INFO';
		BULK INSERT BRONZE.CRM_PRD_INFO
		FROM 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT'>>>     ---------------    <<<';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_detail';
		TRUNCATE TABLE bronze.crm_sales_detail;
		PRINT'>> Inserting Data Into: bronze.crm_sales_detail';
		BULK INSERT bronze.crm_sales_detail
		FROM 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT'>>>     ---------------    <<<';
		PRINT'----------------------------------------------';
		PRINT'Loading ERP FROM CSV Files';
		PRINT'----------------------------------------------';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT'>>>     ---------------    <<<';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT'>>>     ---------------    <<<';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\NatU\SQL_projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'>>>     ---------------    <<<';
		
		SET @batch_end_time = GETDATE();
		PRINT'======================================================';
		PRINT'Loading Bronze Layer is Completed';
		PRINT'   - Total Loading Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT'====================================================='
	END TRY
	BEGIN CATCH
		PRINT'======================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR Message: ' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'======================================================';
	END CATCH
END
