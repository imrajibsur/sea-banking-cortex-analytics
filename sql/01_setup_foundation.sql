-- =====================================================================
-- SEA RETAIL BANKING DEMO – FOUNDATION SETUP
--
-- Created By  : Rajib Lochan Sur
-- Organization: Deloitte Southeast Asia
-- Purpose     : Create full database + schema foundation for the 
--               "SEA Banking Analytics – Capstone Project"
-- Description : This script provisions all base components required
--               for the end-to-end demo, including:
--                 • RETAIL_BANKING_DEMO database & schemas
--                 • BANKING_WH warehouse
--                 • BANKING_APP_ROLE role & privileges
--                 • Stages (APP_STAGE, SEMANTIC_STAGE, DOCS_STAGE)
--                 • Email integration for Streamlit notifications
--                 • Cortex Analyst enablement
--
-- Notes       :
--   ✓ Run as ACCOUNTADMIN  
--   ✓ Mandatory step before loading data or running Streamlit
--
-- Version     : 1.0
-- Last Updated: 09 Dec 2025
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- =====================================================================
-- 1. CREATE DATABASE AND SCHEMAS
-- =====================================================================

CREATE OR REPLACE DATABASE RETAIL_BANKING_DEMO
    COMMENT = 'SEA Retail Banking Demo - Malaysia/Singapore';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_DEMO.BRONZE 
    COMMENT = 'Raw landing zone for source data';
    
CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_DEMO.SILVER 
    COMMENT = 'Cleansed and validated data';
    
CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_DEMO.GOLD 
    COMMENT = 'Business-ready curated data';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_DEMO.DOCS 
    COMMENT = 'Banking documents and policies for Cortex Search';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_DEMO.ANALYTICS
    COMMENT = 'Business analytics and KPI views';

-- =====================================================================
-- 2. CREATE WAREHOUSE
-- =====================================================================

CREATE WAREHOUSE IF NOT EXISTS BANKING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for SEA banking demo';

-- =====================================================================
-- 3. CREATE ROLE AND GRANT PERMISSIONS
-- =====================================================================

CREATE ROLE IF NOT EXISTS BANKING_APP_ROLE
    COMMENT = 'Role for Streamlit app and end users';

-- Database & schema usage
GRANT USAGE ON DATABASE RETAIL_BANKING_DEMO TO ROLE BANKING_APP_ROLE;

GRANT USAGE ON SCHEMA RETAIL_BANKING_DEMO.BRONZE    TO ROLE BANKING_APP_ROLE;
GRANT USAGE ON SCHEMA RETAIL_BANKING_DEMO.SILVER    TO ROLE BANKING_APP_ROLE;
GRANT USAGE ON SCHEMA RETAIL_BANKING_DEMO.GOLD      TO ROLE BANKING_APP_ROLE;
GRANT USAGE ON SCHEMA RETAIL_BANKING_DEMO.DOCS      TO ROLE BANKING_APP_ROLE;
GRANT USAGE ON SCHEMA RETAIL_BANKING_DEMO.ANALYTICS TO ROLE BANKING_APP_ROLE;

-- Table access (current)
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_BANKING_DEMO.BRONZE    TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_BANKING_DEMO.SILVER    TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_BANKING_DEMO.GOLD      TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_BANKING_DEMO.ANALYTICS TO ROLE BANKING_APP_ROLE;

-- Table access (future)
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_BANKING_DEMO.BRONZE    TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_BANKING_DEMO.SILVER    TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_BANKING_DEMO.GOLD      TO ROLE BANKING_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_BANKING_DEMO.ANALYTICS TO ROLE BANKING_APP_ROLE;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE BANKING_WH TO ROLE BANKING_APP_ROLE;

-- Cortex privileges
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER         TO ROLE BANKING_APP_ROL_
