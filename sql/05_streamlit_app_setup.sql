-- =====================================================================
-- SEA RETAIL BANKING DEMO - STREAMLIT APP SETUP
-- Purpose: Grant role to user and create Streamlit app
-- Run as: ACCOUNTADMIN
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_BANKING_DEMO;
USE SCHEMA SILVER;

-- =====================================================================
-- 1. ASSIGN ROLE TO DEMO USER
-- =====================================================================

-- TODO: Replace with your actual Snowflake username
-- Example:
--   GRANT ROLE BANKING_APP_ROLE TO USER CAPSTONE;
select CURRENT_USER()
GRANT ROLE BANKING_APP_ROLE TO USER CAPSTONE1;

-- Optional: verify
SHOW ROLES LIKE 'BANKING_APP_ROLE';
SHOW GRANTS TO USER CAPSTONE1;

-- =====================================================================
-- 2. (OPTIONAL) TEST EMAIL INTEGRATION
-- =====================================================================

-- Uncomment and set a valid email to test email integration manually
/*
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

-- Upload streamlit_app.py to @APP_STAGE before running this:
--   Snowsight → Data → RETAIL_BANKING_DEMO.SILVER → Stages → APP_STAGE → Upload

CREATE OR REPLACE STREAMLIT SEA_BANKING_ANALYTICS_APP
  ROOT_LOCATION = '@RETAIL_BANKING_DEMO.SILVER.APP_STAGE'
  MAIN_FILE     = 'streamlit_app.py'
  QUERY_WAREHOUSE = BANKING_WH
  COMMENT = 'SEA Retail Banking Analytics - Cortex + Streamlit demo';

-- Verify
SHOW STREAMLIT APPS IN SCHEMA RETAIL_BANKING_DEMO.SILVER;

SELECT '✓ STREAMLIT APP CREATED: SEA_BANKING_ANALYTICS_APP' AS STATUS;
