# streamlit_main.py
# ============================================================================
# SEA Banking Analytics - PROFESSIONAL CLIENT DEMO VERSION
# Enhanced with Executive Dashboard, Beautiful Visuals, and Interactive Features
#
# Created By  : Rajib Lochan Sur
# Organization: Deloitte Southeast Asia
# Purpose     : Streamlit application to present SEA Banking Analytics powered
#               by Snowflake (Cortex Analyst + curated GOLD datasets).
# Description :
#   - Executive dashboard with KPIs and charts
#   - Natural-language -> SQL via Cortex Analyst integration
#   - Data quality dashboard (rejects) and Customer 360 view
#   - Email reporting using Snowflake notification integration
#
# Notes       :
#   - This file contains only UI and orchestration logic; Snowflake objects
#     (DATABASE, SCHEMAS, TABLES, STAGES, INTEGRATIONS) are created by
#     the accompanying SQL scripts in the repository.
#   - Ensure the Snowflake user running the Streamlit app has the BANKING_APP_ROLE
#     and access to the RETAIL_BANKING_DEMO database.
#
# Version     : 1.0
# Last Updated: 9 Dec 2025
# ============================================================================

import json
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# ============================================================================
# CONFIGURATION
# ============================================================================

DB = "RETAIL_BANKING_DEMO"
SCHEMA_SILVER = "SILVER"
SCHEMA_GOLD = "GOLD"

CUSTOMERS_TBL = f"{DB}.{SCHEMA_GOLD}.CUSTOMERS"
ACCOUNTS_TBL = f"{DB}.{SCHEMA_GOLD}.ACCOUNTS"
TRANSACTIONS_TBL = f"{DB}.{SCHEMA_GOLD}.TRANSACTIONS"
LOANS_TBL = f"{DB}.{SCHEMA_GOLD}.LOANS"
REJECTS_TBL = f"{DB}.{SCHEMA_SILVER}.DATA_QUALITY_REJECTS"

EMAIL_INTEGRATION = "EMAIL_INT"

# ============================================================================
# PROFESSIONAL STYLING
# ============================================================================

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

* {
    font-family: 'Inter', -apple-system, sans-serif;
}

.main-header {
    background: linear-gradient(135deg, #003366 0%, #005a8c 50%, #008B8B 100%);
    padding: 40px 30px;
    border-radius: 15px;
    margin-bottom: 30px;
    box-shadow: 0 10px 40px rgba(0, 51, 102, 0.3);
    position: relative;
    overflow: hidden;
}

.main-header::before {
    content: '';
    position: absolute;
    top: 0;
    right: 0;
    width: 300px;
    height: 300px;
    background: radial-gradient(circle, rgba(255, 215, 0, 0.1) 0%, transparent 70%);
}

.main-header h1 {
    color: white;
    font-size: 36px;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.5px;
}

.main-header p {
    color: #FFD700;
    font-size: 16px;
    margin: 10px 0 0 0;
    font-weight: 400;
}

.main-header .subtitle {
    color: rgba(255, 255, 255, 0.8);
    font-size: 14px;
    margin-top: 5px;
}

.exec-kpi {
    background: linear-gradient(135deg, rgba(255, 255, 255, 0.95) 0%, rgba(255, 255, 255, 0.85) 100%);
    border-radius: 15px;
    padding: 25px;
    text-align: center;
    box-shadow: 0 5px 20px rgba(0, 0, 0, 0.08);
    border-left: 4px solid #003366;
    transition: all 0.3s ease;
}

.exec-kpi:hover {
    transform: translateY(-5px);
    box-shadow: 0 8px 30px rgba(0, 51, 102, 0.15);
}

.exec-kpi-value {
    font-size: 42px;
    font-weight: 700;
    color: #003366;
    margin: 10px 0;
    line-height: 1;
}

.exec-kpi-label {
    color: #666;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 600;
}

.exec-kpi-change {
    color: #28a745;
    font-size: 12px;
    margin-top: 8px;
    font-weight: 500;
}

.action-card {
    background: white;
    border-radius: 12px;
    padding: 20px;
    margin: 10px 0;
    box-shadow: 0 3px 15px rgba(0, 0, 0, 0.06);
    border: 1px solid rgba(0, 51, 102, 0.1);
    transition: all 0.3s ease;
    cursor: pointer;
}

.action-card:hover {
    border-color: #003366;
    box-shadow: 0 5px 25px rgba(0, 51, 102, 0.15);
    transform: translateX(5px);
}

.action-card h3 {
    color: #003366;
    font-size: 18px;
    font-weight: 600;
    margin: 0 0 8px 0;
}

.action-card p {
    color: #666;
    font-size: 14px;
    margin: 0;
    line-height: 1.5;
}

.insight-card {
    background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
    border-radius: 12px;
    padding: 25px;
    margin: 15px 0;
    border-left: 5px solid #FFD700;
    box-shadow: 0 3px 15px rgba(0, 0, 0, 0.05);
}

.insight-card h4 {
    color: #003366;
    font-size: 16px;
    font-weight: 600;
    margin: 0 0 10px 0;
}

.insight-card p {
    color: #555;
    font-size: 14px;
    margin: 0;
    line-height: 1.6;
}

.chart-container {
    background: white;
    border-radius: 12px;
    padding: 25px;
    margin: 15px 0;
    box-shadow: 0 3px 15px rgba(0, 0, 0, 0.06);
}

.chart-title {
    color: #003366;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 20px;
}

.success-banner {
    background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
    color: white;
    padding: 15px 20px;
    border-radius: 10px;
    margin: 15px 0;
    font-weight: 500;
    box-shadow: 0 5px 20px rgba(40, 167, 69, 0.3);
}

.error-banner {
    background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
    color: white;
    padding: 15px 20px;
    border-radius: 10px;
    margin: 15px 0;
    font-weight: 500;
    box-shadow: 0 5px 20px rgba(220, 53, 69, 0.3);
}

.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}

.stTabs [data-baseweb="tab"] {
    background-color: rgba(255, 255, 255, 0.5);
    border-radius: 10px;
    padding: 12px 24px;
    font-weight: 600;
    border: 2px solid transparent;
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #003366, #008B8B);
    color: white !important;
    border-color: #FFD700;
}

.footer {
    text-align: center;
    padding: 30px 20px;
    color: #666;
    border-top: 2px solid #f0f0f0;
    margin-top: 50px;
}

.footer-logo {
    font-size: 24px;
    margin-bottom: 10px;
}
</style>
"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def html_escape(text: str) -> str:
    """Minimal HTML escape for body content."""
    if text is None:
        return ""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


def query_cortex_analyst(session, question: str):
    """Enhanced Cortex Analyst with better prompting"""
    try:
        schema_info = """
        RETAIL_BANKING_DEMO.GOLD database schema:
        
        CUSTOMERS: CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, 
                   CITY, STATE, COUNTRY, CUSTOMER_SEGMENT, CREDIT_SCORE, ANNUAL_INCOME_MYR
        
        ACCOUNTS: ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS, 
                  CURRENT_BALANCE_MYR, OPEN_DATE
        
        TRANSACTIONS: TRANSACTION_ID, CUSTOMER_ID, TRANSACTION_DATE, TRANSACTION_TYPE,
                      AMOUNT_MYR, CHANNEL, CATEGORY, MERCHANT_NAME
        
        LOANS: LOAN_ID, CUSTOMER_ID, LOAN_TYPE, LOAN_AMOUNT_MYR, 
               OUTSTANDING_BALANCE_MYR, LOAN_STATUS
        
        CRITICAL: Use FIRST_NAME || ' ' || LAST_NAME for customer names.
        """
        
        prompt = f"""Generate SQL for: {question}

Schema: {schema_info}

Rules:
1. Use FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER_NAME for names
2. Tables in RETAIL_BANKING_DEMO.GOLD
3. Return ONLY executable SQL
4. No markdown or explanations

SQL:"""

        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt.replace(chr(39), chr(39)+chr(39))}') as generated_sql"
        
        result = session.sql(sql).collect()
        
        if result:
            generated_sql = result[0]['GENERATED_SQL'].replace('```sql', '').replace('```', '').strip()
            
            try:
                data = session.sql(generated_sql).to_pandas()
                return {
                    'success': True,
                    'question': question,
                    'sql': generated_sql,
                    'data': data,
                    'row_count': len(data)
                }
            except Exception as e:
                return {
                    'success': False,
                    'question': question,
                    'sql': generated_sql,
                    'error': f"Execution error: {str(e)}"
                }
        
        return {'success': False, 'question': question, 'error': "No SQL generated"}
            
    except Exception as e:
        return {'success': False, 'question': question, 'error': str(e)}


def send_email_report(session, recipient: str, subject: str, body_text: str):
    """
    Send an email via SYSTEM$SEND_EMAIL as HTML.
    - Escapes single quotes in recipient, subject, and body for SQL.
    - Treats any successful CALL (no exception) as success.
    - Returns the status text from Snowflake (first column of first row).
    """
    try:
        def _esc(s: str) -> str:
            return s.replace("'", "''") if s is not None else ""

        recipient_esc = _esc(recipient)
        subject_esc   = _esc(subject)
        body_esc      = _esc(body_text)

        sql = f"""
        CALL SYSTEM$SEND_EMAIL(
            '{EMAIL_INTEGRATION}',
            '{recipient_esc}',
            '{subject_esc}',
            '{body_esc}',
            'text/html'
        )
        """

        result = session.sql(sql).collect()

        if result:
            row = result[0]
            if isinstance(row, dict):
                msg = list(row.values())[0]
            else:
                msg = row[0]
            return True, str(msg)
        else:
            return True, "Email request submitted (no status returned)."

    except Exception as e:
        return False, f"Error calling SYSTEM$SEND_EMAIL: {e}"


def get_executive_metrics(session):
    """Get key metrics for executive dashboard"""
    try:
        metrics = {}
        
        metrics['total_customers'] = session.sql(f"SELECT COUNT(*) FROM {CUSTOMERS_TBL}").collect()[0][0]
        metrics['total_accounts'] = session.sql(f"SELECT COUNT(*) FROM {ACCOUNTS_TBL}").collect()[0][0]
        metrics['total_balance'] = session.sql(f"SELECT SUM(CURRENT_BALANCE_MYR) FROM {ACCOUNTS_TBL}").collect()[0][0]
        metrics['total_transactions'] = session.sql(f"SELECT COUNT(*) FROM {TRANSACTIONS_TBL}").collect()[0][0]
        metrics['total_loans'] = session.sql(f"SELECT SUM(OUTSTANDING_BALANCE_MYR) FROM {LOANS_TBL}").collect()[0][0]
        metrics['avg_credit_score'] = session.sql(f"SELECT AVG(CREDIT_SCORE) FROM {CUSTOMERS_TBL}").collect()[0][0]
        
        return metrics
    except:
        return None


def create_country_distribution_chart(session):
    """Create customer distribution by country"""
    try:
        df = session.sql(f"""
            SELECT COUNTRY, COUNT(*) as CUSTOMER_COUNT
            FROM {CUSTOMERS_TBL}
            GROUP BY COUNTRY
            ORDER BY CUSTOMER_COUNT DESC
        """).to_pandas()
        
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('CUSTOMER_COUNT:Q', title='Number of Customers'),
            y=alt.Y('COUNTRY:N', sort='-x', title='Country'),
            color=alt.Color('COUNTRY:N', scale=alt.Scale(scheme='blues'), legend=None),
            tooltip=['COUNTRY', 'CUSTOMER_COUNT']
        ).properties(
            height=250
        )
        
        return chart
    except:
        return None


def create_segment_chart(session):
    """Create customer segment distribution"""
    try:
        df = session.sql(f"""
            SELECT CUSTOMER_SEGMENT, COUNT(*) as COUNT
            FROM {CUSTOMERS_TBL}
            GROUP BY CUSTOMER_SEGMENT
        """).to_pandas()
        
        chart = alt.Chart(df).mark_arc(innerRadius=60, outerRadius=120).encode(
            theta=alt.Theta('COUNT:Q'),
            color=alt.Color('CUSTOMER_SEGMENT:N', 
                scale=alt.Scale(scheme='category10'),
                legend=alt.Legend(title="Segment")
            ),
            tooltip=['CUSTOMER_SEGMENT', 'COUNT']
        ).properties(
            height=300
        )
        
        return chart
    except:
        return None


def create_transaction_trend_chart(session):
    """Create transaction trend over time"""
    try:
        df = session.sql(f"""
            SELECT 
                DATE_TRUNC('DAY', TRANSACTION_DATE) as DATE,
                COUNT(*) as TRANSACTION_COUNT,
                SUM(AMOUNT_MYR) as TOTAL_AMOUNT
            FROM {TRANSACTIONS_TBL}
            GROUP BY DATE_TRUNC('DAY', TRANSACTION_DATE)
            ORDER BY DATE
        """).to_pandas()
        
        chart = alt.Chart(df).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X('DATE:T', title='Date'),
            y=alt.Y('TRANSACTION_COUNT:Q', title='Transaction Count'),
            tooltip=['DATE:T', 'TRANSACTION_COUNT', 'TOTAL_AMOUNT']
        ).properties(
            height=250
        )
        
        return chart
    except:
        return None

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    st.set_page_config(
        page_title="SEA Banking Analytics - Executive Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="🏦"
    )
    
    st.markdown(CSS, unsafe_allow_html=True)
    
    session = get_active_session()
    
    # ========== HEADER ==========
    st.markdown("""
    <div class="main-header">
        <h1>🏦 SEA Banking Analytics Platform</h1>
        <p>AI-Powered Insights for Malaysia & Singapore Banking Operations</p>
        <p class="subtitle">Powered by Snowflake Cortex Analyst | Real-time Data Intelligence</p>
    </div>
    """, unsafe_allow_html=True)

    # rest of the app unchanged - truncated in canvas for brevity

if __name__ == "__main__":
    main()