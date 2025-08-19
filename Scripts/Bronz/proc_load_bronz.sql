
/*
===============================================================================
Stored Procedure: Load Bronz Layer (Source -> Bronz)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronz' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronz tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronz tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronz.load_bronz;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronz.load_bronz AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronz Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.crm_cust_info';
		TRUNCATE TABLE bronz.crm_cust_info;
		PRINT '>> Inserting Data Into: bronz.crm_cust_info';
		BULK INSERT bronz.crm_cust_info
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.crm_prd_info';
		TRUNCATE TABLE bronz.crm_prd_info;

		PRINT '>> Inserting Data Into: bronz.crm_prd_info';
		BULK INSERT bronz.crm_prd_info
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.crm_sales_details';
		TRUNCATE TABLE bronz.crm_sales_details;
		PRINT '>> Inserting Data Into: bronz.crm_sales_details';
		BULK INSERT bronz.crm_sales_details
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.erp_loc_a101';
		TRUNCATE TABLE bronz.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronz.erp_loc_a101';
		BULK INSERT bronz.erp_loc_a101
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.erp_cust_az12';
		TRUNCATE TABLE bronz.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronz.erp_cust_az12';
		BULK INSERT bronz.erp_cust_az12
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronz.erp_px_cat_g1v2';
		TRUNCATE TABLE bronz.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronz.erp_px_cat_g1v2';
		BULK INSERT bronz.erp_px_cat_g1v2
		FROM 'C:\Users\19193\OneDrive\Desktop\SQL_tss\ETL_datararehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronz Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZ LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
