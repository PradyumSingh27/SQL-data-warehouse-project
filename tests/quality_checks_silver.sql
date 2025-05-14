/*
============================================================================================
Quality Checks 
============================================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, and
    standardization across the 'Silver' Schemas. It includes checks for: 
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardizatiion and consistency.
    - Invalid date ranges and orders.
    - Data cconsistency between related fields.

Usage Notes: 
    - Run these checks after data loading Silver Layer.
    - Investigate andd resolve any discrepancies found during the checks.
===========================================================================================
*/

-- =================================================================
-- Checking 'silver.crm_cust_info'
--==================================================================

-- Checck for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
* 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 

-- Check For unwanted Spaces
SELECT cst_material_status 
from silver.crm_cust_info 
WHERE cst_material_status <> TRIM(cst_material_status);

SELECT
*
FROM
(
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM silver.crm_cust_info
)t WHERE cst_firstname <> TRIM(cst_firstname);

-- Data Standarddization & Consistency
SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info;

-- =================================================================
-- Checking 'silver.crm_prd_info'
--==================================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
prd_id 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 

-- Check For unwanted Spaces
SELECT prd_nm 
from silver.crm_prd_info 
WHERE prd_nm <> TRIM(prd_nm);

-- Data Consistency
SELECT prd_cost
from silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost is null ;

-- Data Standarddization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check For Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- =================================================================
-- Checking 'silver.crm_sales_detail'
--==================================================================

--Check For Invalid Dates
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_detail
WHERE sls_order_dt < = 0 OR LEN(sls_order_dt) <> 8

-- Check for Invalid Date Orders
SELECT 
*
FROM silver.crm_sales_detail
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Prioce
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero, or Negative

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
FROM silver.crm_sales_detail
WHERE sls_sales != sls_quantity * sls_price
 OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
 OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL

-- =================================================================
-- Checking 'silver.erp_cust_az12l'
--==================================================================

-- Identify Out-of-Range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
