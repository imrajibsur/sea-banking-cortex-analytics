-- =====================================================================
-- SEA RETAIL BANKING DEMO - CORTEX ANALYST SETUP
-- Purpose: Create semantic model for Malaysia/Singapore banking
-- Run as: ACCOUNTADMIN
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_BANKING_DEMO;
USE SCHEMA SILVER;
USE WAREHOUSE BANKING_WH;

-- =====================================================================
-- STEP 1: VERIFY PRIMARY KEYS EXIST
-- =====================================================================

SHOW PRIMARY KEYS IN SCHEMA RETAIL_BANKING_DEMO.GOLD;
SHOW PRIMARY KEYS IN SCHEMA RETAIL_BANKING_DEMO.SILVER;

-- Primary keys should already be created from previous script
-- If not, run these:
-- ALTER TABLE GOLD.CUSTOMERS ADD PRIMARY KEY (CUSTOMER_ID);
-- ALTER TABLE GOLD.ACCOUNTS ADD PRIMARY KEY (ACCOUNT_ID);
-- ALTER TABLE GOLD.TRANSACTIONS ADD PRIMARY KEY (TRANSACTION_ID);
-- ALTER TABLE GOLD.LOANS ADD PRIMARY KEY (LOAN_ID);
-- ALTER TABLE SILVER.DATA_QUALITY_REJECTS ADD PRIMARY KEY (REJECT_ID);

-- =====================================================================
-- STEP 2: UPLOAD YAML FILE
-- =====================================================================

/*
MANUAL STEP:

1. Save the banking_semantic_sea.yaml file (provided separately)
2. In Snowsight, go to: Data → RETAIL_BANKING_DEMO → SILVER → Stages → SEMANTIC_STAGE
3. Click "Upload Files"
4. Select banking_semantic_sea.yaml
5. Click Upload
*/

-- Verify upload
LIST @SEMANTIC_STAGE;

-- =====================================================================
-- STEP 3: CREATE SEMANTIC MODEL IN SNOWSIGHT UI
-- =====================================================================

/*
MANUAL STEP:

1. In Snowsight, go to: AI & ML → Cortex Analyst

2. Click "+ Semantic Model"

3. Fill in:
   - Name: SEA Banking Analytics
   - Database: RETAIL_BANKING_DEMO
   - Schema: SILVER
   - Stage: @SEMANTIC_STAGE
   - YAML File: banking_semantic_sea.yaml

4. Click "Create"

5. Go to "Playground" tab
*/

-- =====================================================================
-- STEP 4: TEST QUESTIONS IN PLAYGROUND
-- =====================================================================

/*
Try these SEA-specific questions:

BASIC:
✓ How many customers do we have?
✓ How many customers in Malaysia?
✓ How many customers in Singapore?
✓ Show me account count by account type

LOCATION-BASED:
✓ Show me customers in Kuala Lumpur
✓ Show me customers in Penang
✓ Show me customers in Johor Bahru
✓ Show me customers in Singapore
✓ List customers by state in Malaysia

AGGREGATION:
✓ Show me total transaction amount by customer segment
✓ What is the total balance by country?
✓ Show me transaction count by channel
✓ What is the average credit score by customer segment?

ADVANCED:
✓ Show me top 5 customers by transaction amount in Malaysia
✓ Which branches in Kuala Lumpur have the most accounts?
✓ Show me premium customers in Singapore
✓ What are the top spending categories in Penang?
✓ Show me loan portfolio by country
*/

-- =====================================================================
-- VERIFICATION
-- =====================================================================

SHOW PARAMETERS LIKE 'ENABLE_CORTEX_ANALYST' IN ACCOUNT;
SHOW SEMANTIC VIEWS IN SCHEMA SILVER;

SELECT '✓ CORTEX ANALYST SETUP COMPLETE - SEA REGION!' AS STATUS;
SELECT 'Test queries in Snowsight Playground!' AS NEXT_STEP;