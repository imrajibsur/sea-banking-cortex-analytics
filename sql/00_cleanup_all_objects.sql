-- =====================================================================
-- CLEANUP SCRIPT - DROP ALL DEMO OBJECTS
-- SEA Retail Banking Demo - Snowflake Cortex + Streamlit
-- Run as: ACCOUNTADMIN
-- WARNING: This will delete all data and objects!
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- =====================================================================
-- 1. DROP STREAMLIT APPS
-- =====================================================================

-- Optional: inspect first
SHOW STREAMLIT APPS IN SCHEMA RETAIL_BANKING_DEMO.SILVER;

-- Drop Streamlit app(s) - update name(s) if different
-- Example:
-- DROP STREAMLIT IF EXISTS RETAIL_BANKING_DEMO.SILVER.SEA_BANKING_ANALYTICS_APP;

-- =====================================================================
-- 2. DROP SEMANTIC VIEWS
-- =====================================================================

USE DATABASE RETAIL_BANKING_DEMO;
USE SCHEMA SILVER;

DROP SEMANTIC VIEW IF EXISTS BANKING_SEMANTIC_VIEW;
DROP SEMANTIC VIEW IF EXISTS MINIMAL_TEST_VIEW;

SHOW SEMANTIC VIEWS IN SCHEMA SILVER;

-- =====================================================================
-- 3. DROP SEMANTIC MODEL (MANUAL - UI)
-- =====================================================================

/*
MANUAL STEP (in Snowsight UI):
1. Go to: AI & ML → Cortex Analyst
2. Find your semantic model: "SEA Banking Analytics"
3. Click the "..." menu → Delete
*/

-- =====================================================================
-- 4. DROP FUNCTIONS (IF YOU CREATED ANY)
-- =====================================================================

DROP FUNCTION IF EXISTS SILVER.TEST_CORTEX_ANALYST(STRING);
DROP FUNCTION IF EXISTS SILVER.ASK_ANALYST(STRING);

-- =====================================================================
-- 5. DROP ANALYTICS VIEWS
-- =====================================================================

USE SCHEMA ANALYTICS;

DROP VIEW IF EXISTS VW_CUSTOMER_360;
DROP VIEW IF EXISTS VW_TRANSACTION_SUMMARY;
DROP VIEW IF EXISTS VW_LOAN_PORTFOLIO;

-- =====================================================================
-- 6. DROP TABLES (GOLD, SILVER, BRONZE)
-- =====================================================================

-- GOLD tables
USE SCHEMA GOLD;
DROP TABLE IF EXISTS CUSTOMERS;
DROP TABLE IF EXISTS ACCOUNTS;
DROP TABLE IF EXISTS TRANSACTIONS;
DROP TABLE IF EXISTS LOANS;

-- SILVER tables
USE SCHEMA SILVER;
DROP TABLE IF EXISTS STG_CUSTOMERS;
DROP TABLE IF EXISTS STG_ACCOUNTS;
DROP TABLE IF EXISTS STG_TRANSACTIONS;
DROP TABLE IF EXISTS STG_LOANS;
DROP TABLE IF EXISTS DATA_QUALITY_REJECTS;

-- BRONZE tables
USE SCHEMA BRONZE;
DROP TABLE IF EXISTS SRC_CUSTOMERS;
DROP TABLE IF EXISTS SRC_ACCOUNTS;
DROP TABLE IF EXISTS SRC_TRANSACTIONS;
DROP TABLE IF EXISTS SRC_LOANS;

-- =====================================================================
-- 7. DROP STAGES
-- =====================================================================

USE SCHEMA SILVER;

DROP STAGE IF EXISTS SEMANTIC_STAGE;
DROP STAGE IF EXISTS APP_STAGE;
DROP STAGE IF EXISTS DOCS_STAGE;

-- =====================================================================
-- 8. DROP NOTIFICATION INTEGRATION
-- =====================================================================

DROP NOTIFICATION INTEGRATION IF EXISTS EMAIL_INT;

-- =====================================================================
-- 9. DROP SCHEMAS & DATABASE
-- =====================================================================

DROP SCHEMA IF EXISTS RETAIL_BANKING_DEMO.ANALYTICS CASCADE;
DROP SCHEMA IF EXISTS RETAIL_BANKING_DEMO.DOCS CASCADE;
DROP SCHEMA IF EXISTS RETAIL_BANKING_DEMO.GOLD CASCADE;
DROP SCHEMA IF EXISTS RETAIL_BANKING_DEMO.SILVER CASCADE;
DROP SCHEMA IF EXISTS RETAIL_BANKING_DEMO.BRONZE CASCADE;

DROP DATABASE IF EXISTS RETAIL_BANKING_DEMO;

-- =====================================================================
-- 10. DROP WAREHOUSE & ROLE
-- =====================================================================

DROP WAREHOUSE IF EXISTS BANKING_WH;
DROP ROLE IF EXISTS BANKING_APP_ROLE;

-- =====================================================================
-- 11. OPTIONAL: REVOKE CORTEX GRANTS
-- =====================================================================

-- ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST = FALSE;
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'NONE';

-- =====================================================================
-- VERIFICATION
-- =====================================================================

SHOW DATABASES LIKE 'RETAIL_BANKING_DEMO';
SHOW WAREHOUSES LIKE 'BANKING_WH';
SHOW ROLES LIKE 'BANKING_APP_ROLE';
SHOW NOTIFICATION INTEGRATIONS LIKE 'EMAIL_INT';

SELECT '✓ CLEANUP COMPLETE - All objects removed!' AS STATUS;
