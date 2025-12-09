-- =====================================================================
-- SEA RETAIL BANKING DEMO - STREAMLIT APP SETUP
-- SEA Retail Banking Demo – Snowflake Cortex + Streamlit
--
-- Created By  : Rajib Lochan Sur
-- Organization: Deloitte Southeast Asia
-- Purpose     : Grant required roles/permissions and deploy Streamlit app
--               for the SEA Retail Banking Analytics demo.
--
-- Description :
--   • Assign BANKING_APP_ROLE to demo user
--   • (Optional) Test EMAIL_INT notification integration
--   • Create or replace the Streamlit app using the uploaded Python file
--
-- Notes       :
--   ⚠ Run as ACCOUNTADMIN
--   ⚠ Ensure APP_STAGE contains streamlit_app.py
--   ⚠ Replace CAPSTONE1 with your actual Snowflake username
--
-- Version     : 1.0
-- Last Updated: 12 Sept 2025
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_BANKING_DEMO;
USE SCHEMA SILVER;

-- =====================================================================
-- 1. ASSIGN ROLE TO DEMO USER
-- =====================================================================

-- Replace with your actual Snowflake username
-- Example:
--   GRANT ROLE BANKING_APP_ROLE TO USER CAPSTONE;

SELECT CURRENT_USER();

GRANT ROLE BANKING_APP_ROLE TO USER CAPSTONE1;

-- Optional verification
SHOW ROLES LIKE 'BANKING_APP_ROLE';
SHOW GRANTS TO USER CAPSTONE1;

-- =====================================================================
-- 2. (OPTIONAL) TEST EMAIL INTEGRATION
-- =====================================================================

/*
-- Uncomment to verify email configuration:

SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('CAPSTONE1');

CALL SYSTEM$SEND_EMAIL(
    'EMAIL_INT',
    'rlochansur@deloitte.com',
    'Test from Snowflake - SEA Banking Analytics',
    'Hello! This is a test email from the SEA Banking Analytics demo.',
    'text/plain'
);
*/

-- =====================================================================
-- 3. CREATE / REPLACE STREAMLIT APP
-- =====================================================================

-- Upload your app file first:
-- Snowsight → Data → RETAIL_BANKING_DEMO.SILVER → Stages → APP_STAGE → Upload
--
-- Ensure the file name is:
--      streamlit_app.py

CREATE OR REPLACE STREAMLIT SEA_BANKING_ANALYTICS_APP
  ROOT_LOCATION  = '@RETAIL_BANKING_DEMO.SILVER.APP_STAGE'
  MAIN_FILE      = 'streamlit_app.py'
  QUERY_WAREHOUSE = BANKING_WH
  COMMENT = 'SEA Retail Banking Analytics - Cortex + Streamlit demo';

-- Verify deployment
SHOW STREAMLIT APPS IN SCHEMA RETAIL_BANKING_DEMO.SILVER;

SELECT '✓ STREAMLIT APP CREATED: SEA_BANKING_ANALYTICS_APP' AS STATUS;
