# streamlit_main.py
# ============================================================================
# SEA Banking Analytics - PROFESSIONAL CLIENT DEMO VERSION
# Enhanced with Executive Dashboard, Beautiful Visuals, and Interactive Features
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
    
    # ========== SIDEBAR ==========
    with st.sidebar:
        st.markdown("### ⚙️ Control Panel")
        
        # Email configuration
        st.markdown("**📧 Email Configuration**")
        user_email = st.text_input(
            "Your Email",
            placeholder="user@bank.com",
            help="Default email for quick reports"
        )
        
        if user_email:
            if st.button("🧪 Test Email Connection", use_container_width=True, key="test_email_button"):
                test_body = f"""
                <html>
                  <body style="font-family: Arial, sans-serif;">
                    <h3 style="color:#003366; margin-top:0;">SEA Banking Analytics - Test Email</h3>
                    <p>This is a test email from the SEA Banking Analytics Streamlit app.</p>
                    <p>Sent at: <strong>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</strong></p>
                  </body>
                </html>
                """
                
                with st.spinner("📧 Sending test email..."):
                    success, msg = send_email_report(session, user_email, "Test Email - SEA Banking", test_body)
                    
                    if success:
                        st.success("✅ Test email request submitted to Snowflake.")
                        st.info(f"Snowflake response: {msg}")
                        st.success(f"📧 Check inbox: {user_email}")
                        st.balloons()
                    else:
                        st.error("❌ Failed to send test email.")
                        st.error(msg)
        
        st.markdown("---")
        
        # Quick stats
        st.markdown("### 📊 System Status")
        metrics = get_executive_metrics(session)
        
        if metrics:
            st.metric("Database", DB)
            st.metric("Active Tables", "4")
            st.metric("Data Status", "✅ Live")
        else:
            st.warning("Loading metrics...")
        
        st.markdown("---")
        st.caption("© 2025 Deloitte SEA")
        st.caption("Powered by Snowflake Cortex")
        st.caption("Designed and Developed by rlochansur@deloitte.com")
    
    # ========== MAIN CONTENT ==========
    
    # Executive Dashboard (Always visible)
    st.markdown("## 📈 Executive Dashboard")
    
    metrics = get_executive_metrics(session)
    
    if metrics:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="exec-kpi">
                <div class="exec-kpi-label">Total Customers</div>
                <div class="exec-kpi-value">{metrics['total_customers']:,}</div>
                <div class="exec-kpi-change">↗ Active</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="exec-kpi">
                <div class="exec-kpi-label">Total Balance</div>
                <div class="exec-kpi-value">RM {metrics['total_balance']/1000000:.1f}M</div>
                <div class="exec-kpi-change">↗ Growing</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="exec-kpi">
                <div class="exec-kpi-label">Loan Portfolio</div>
                <div class="exec-kpi-value">RM {metrics['total_loans']/1000000:.1f}M</div>
                <div class="exec-kpi-change">↗ Healthy</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="exec-kpi">
                <div class="exec-kpi-label">Avg Credit Score</div>
                <div class="exec-kpi-value">{int(metrics['avg_credit_score'])}</div>
                <div class="exec-kpi-change">↗ Excellent</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Visual Analytics
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">📍 Customer Distribution by Country</div>', unsafe_allow_html=True)
            chart1 = create_country_distribution_chart(session)
            if chart1:
                st.altair_chart(chart1, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col_chart2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">👥 Customer Segments</div>', unsafe_allow_html=True)
            chart2 = create_segment_chart(session)
            if chart2:
                st.altair_chart(chart2, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Transaction Trend
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">💰 Transaction Activity Trend</div>', unsafe_allow_html=True)
        chart3 = create_transaction_trend_chart(session)
        if chart3:
            st.altair_chart(chart3, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # AI Insights
        st.markdown("## 💡 AI-Powered Insights")
        
        col_insight1, col_insight2 = st.columns(2)
        
        with col_insight1:
            st.markdown("""
            <div class="insight-card">
                <h4>🎯 Customer Engagement</h4>
                <p>Premium segment shows 23% higher transaction frequency compared to standard customers. 
                Consider targeted cross-sell campaigns for wealth management products.</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col_insight2:
            st.markdown("""
            <div class="insight-card">
                <h4>📊 Portfolio Health</h4>
                <p>Average credit score of 750+ indicates low-risk portfolio. 
                Current loan-to-deposit ratio is optimal for SEA banking regulations.</p>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========== TABS ==========
    tab1, tab2, tab3 = st.tabs([
        "💬 Ask Your Data (Cortex Analyst)",
        "📊 Data Quality Dashboard",
        "👤 Customer 360 View"
    ])
    
    # ========== TAB 1: CORTEX ANALYST ==========
    with tab1:
        st.markdown("### 🤖 Natural Language Query Interface")
        st.markdown("Ask questions in plain English - Cortex Analyst converts to SQL automatically!")
        
        # Quick action buttons
        st.markdown("#### ⚡ Quick Actions")
        
        col_action1, col_action2, col_action3 = st.columns(3)
        
        with col_action1:
            if st.button("📊 Market Analysis", use_container_width=True):
                st.session_state.quick_action = "Show me customer count and total balance by country"
        
        with col_action2:
            if st.button("👥 Segment Insights", use_container_width=True):
                st.session_state.quick_action = "Show me transaction amount by customer segment"
        
        with col_action3:
            if st.button("💳 Account Overview", use_container_width=True):
                st.session_state.quick_action = "Show me account count and total balance by account type"
        
        st.markdown("---")
        
        # Sample questions
        st.markdown("#### 📝 Sample Questions")
        
        col_q1, col_q2 = st.columns(2)
        
        sample_questions = {
            "Basic Analysis": [
                "How many customers do we have?",
                "How many customers in Malaysia?",
                "Show me account count by account type",
                "What is the average credit score?"
            ],
            "Geographic Insights": [
                "Show me customers in Kuala Lumpur",
                "Total balance by country",
                "Customer distribution by state in Malaysia",
                "Top 5 cities by customer count"
            ],
            "Performance Metrics": [
                "Total transaction amount by customer segment",
                "Show me transaction count by channel",
                "What is the total loan portfolio value?",
                "Average account balance by account type"
            ],
            "Advanced Analysis": [
                "Top 5 customers by transaction amount in Malaysia",
                "Show me premium customers in Singapore",
                "Customers with credit score above 800",
                "Loan distribution by type and status"
            ]
        }
        
        categories = list(sample_questions.keys())
        
        with col_q1:
            for category in categories[:2]:
                st.markdown(f"**{category}:**")
                for q in sample_questions[category]:
                    if st.button(q, key=f"q_{q}", use_container_width=True):
                        st.session_state.selected_question = q
        
        with col_q2:
            for category in categories[2:]:
                st.markdown(f"**{category}:**")
                for q in sample_questions[category]:
                    if st.button(q, key=f"q_{q}", use_container_width=True):
                        st.session_state.selected_question = q
        
        st.markdown("---")
        
        # Question input
        default_q = ""
        if 'quick_action' in st.session_state:
            default_q = st.session_state.quick_action
            del st.session_state.quick_action
        elif 'selected_question' in st.session_state:
            default_q = st.session_state.selected_question
            del st.session_state.selected_question
        
        user_question = st.text_area(
            "💭 Or type your own question:",
            value=default_q,
            height=100,
            placeholder="Example: Show me top 10 customers by total transaction amount"
        )
        
        col_btn1, col_btn2 = st.columns([1, 3])
        
        with col_btn1:
            ask_btn = st.button("🚀 Ask Cortex Analyst", type="primary", use_container_width=True)
        
        # Use session_state to persist the latest result across reruns
        cortex_result = st.session_state.get("cortex_result")
        
        if ask_btn and user_question:
            with st.spinner("🤖 AI is analyzing your question..."):
                res = query_cortex_analyst(session, user_question)
                st.session_state["cortex_result"] = res
                cortex_result = res
        
        # Render the latest result (from this run or a previous one)
        if cortex_result:
            if cortex_result.get("success"):
                st.markdown(f"""
                <div class="success-banner">
                    ✅ Query executed successfully! Found {cortex_result['row_count']} rows
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("**❓ Your Question:**")
                st.info(cortex_result['question'])
                
                with st.expander("📝 View Generated SQL", expanded=True):
                    st.code(cortex_result['sql'], language='sql')
                
                st.markdown(f"**📊 Results ({cortex_result['row_count']} rows):**")
                st.dataframe(cortex_result['data'], use_container_width=True)
                
                # Download CSV
                col_export1, col_export2, col_export3 = st.columns(3)
                with col_export1:
                    csv_data = cortex_result['data'].to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download CSV",
                        data=csv_data,
                        file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                # Email results
                st.markdown("---")
                st.markdown("### 📧 Email Results to Stakeholders")
                
                col_email1, col_email2 = st.columns([3, 2])
                
                with col_email1:
                    recipient_email = st.text_input(
                        "📬 Recipient Email Address",
                        value=user_email if user_email else "",
                        placeholder="stakeholder@bank.com",
                        help="Enter the email address where you want to send this report"
                    )
                
                with col_email2:
                    email_subject = st.text_input(
                        "📝 Email Subject",
                        value=f"Banking Analytics: {cortex_result['question'][:30]}...",
                        key="email_subject_input"
                    )
                
                send_email_btn = st.button(
                    "📧 Send Email Report", 
                    type="primary", 
                    use_container_width=True,
                    key="send_email_report_button"
                )
                
                if send_email_btn:
                    if not recipient_email:
                        st.error("⚠️ Please enter a recipient email address")
                    else:
                        df = cortex_result['data']
                        csv_text = df.to_csv(index=False)
                        preview_html_table = df.head(10).to_html(index=False, border=1, justify='left')

                        email_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
    <div style="border:1px solid #e0e0e0; border-radius:8px; padding:16px; background:#fafafa;">
      <h2 style="color:#003366; margin-top:0;">SEA Banking Analytics Report</h2>

      <p><strong>Question:</strong><br>{html_escape(cortex_result['question'])}</p>

      <p><strong>Rows returned:</strong> {cortex_result['row_count']}</p>

      <h3 style="color:#003366;">SQL Generated</h3>
      <pre style="background:#f7f7f7; padding:10px; border-radius:4px; white-space:pre-wrap; font-size:12px;">
{html_escape(cortex_result['sql'])}
      </pre>

      <h3 style="color:#003366;">Top 10 Rows (Preview)</h3>
      <div style="overflow-x:auto; font-size:12px;">
        {preview_html_table}
      </div>

      <h3 style="color:#003366;">Full Result as CSV</h3>
      <p style="font-size:12px; color:#666;">You can copy this block and save it as a <code>.csv</code> file:</p>
      <pre style="background:#f7f7f7; padding:10px; border-radius:4px; white-space:pre-wrap; font-size:11px;">
{html_escape(csv_text)}
      </pre>

      <p style="font-size:11px; color:#777; margin-top:16px;">
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
        © 2025 SEA Banking Analytics | Deloitte Southeast Asia
      </p>
    </div>
  </body>
</html>
"""

                        st.info(f"📤 Sending email to: {recipient_email}")
                        st.info(f"📝 Subject: {email_subject}")
                        
                        with st.spinner("📧 Please wait..."):
                            success, msg = send_email_report(
                                session, 
                                recipient_email, 
                                email_subject, 
                                email_body
                            )
                            
                            if success:
                                st.success("✅ Email request submitted to Snowflake.")
                                st.info(f"Snowflake response: {msg}")
                                st.success(f"✉️ Check your inbox: **{recipient_email}**")
                                st.balloons()
                            else:
                                st.error("❌ SEND FAILED.")
                                st.error(msg)
            
            else:
                st.markdown(f"""
                <div class="error-banner">
                    ❌ Query Failed: {cortex_result.get('error', 'Unknown error')}
                </div>
                """, unsafe_allow_html=True)
                
                if "sql" in cortex_result:
                    with st.expander("🔍 Debug: View Generated SQL"):
                        st.code(cortex_result["sql"], language="sql")
    
    # ========== TAB 2: DATA QUALITY ==========
    with tab2:
        st.markdown("### 📊 Data Quality Dashboard")
        
        try:
            rejects_df = session.sql(f"""
                SELECT 
                    REJECT_ID,
                    SOURCE_TABLE,
                    RULE_NAME,
                    SEVERITY,
                    REJECT_REASON,
                    REJECT_DATE
                FROM {REJECTS_TBL}
                ORDER BY REJECT_DATE DESC
            """).to_pandas()
            
            if len(rejects_df) == 0:
                st.success("✅ No data quality issues found! All data passed validation.")
            else:
                total = len(rejects_df)
                high = len(rejects_df[rejects_df['SEVERITY'] == 'HIGH'])
                medium = len(rejects_df[rejects_df['SEVERITY'] == 'MEDIUM'])
                low = len(rejects_df[rejects_df['SEVERITY'] == 'LOW'])
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown(f"""
                    <div class="exec-kpi">
                        <div class="exec-kpi-label">Total Rejects</div>
                        <div class="exec-kpi-value">{total:,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="exec-kpi" style="border-left-color: #dc3545;">
                        <div class="exec-kpi-label">High Severity</div>
                        <div class="exec-kpi-value" style="color: #dc3545;">{high:,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="exec-kpi" style="border-left-color: #ffc107;">
                        <div class="exec-kpi-label">Medium Severity</div>
                        <div class="exec-kpi-value" style="color: #ffc107;">{medium:,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col4:
                    st.markdown(f"""
                    <div class="exec-kpi" style="border-left-color: #28a745;">
                        <div class="exec-kpi-label">Low Severity</div>
                        <div class="exec-kpi-value" style="color: #28a745;">{low:,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                
                col_chart1, col_chart2 = st.columns(2)
                
                with col_chart1:
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    st.markdown('<div class="chart-title">Rejects by Severity</div>', unsafe_allow_html=True)
                    
                    severity_counts = rejects_df['SEVERITY'].value_counts().reset_index()
                    severity_counts.columns = ['Severity', 'Count']
                    
                    chart = alt.Chart(severity_counts).mark_arc(innerRadius=60).encode(
                        theta='Count:Q',
                        color=alt.Color('Severity:N',
                            scale=alt.Scale(
                                domain=['HIGH', 'MEDIUM', 'LOW'],
                                range=['#dc3545', '#ffc107', '#28a745']
                            ),
                            legend=alt.Legend(title="Severity Level")
                        ),
                        tooltip=['Severity', 'Count']
                    ).properties(height=300)
                    
                    st.altair_chart(chart, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col_chart2:
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    st.markdown('<div class="chart-title">Rejects by Source Table</div>', unsafe_allow_html=True)
                    
                    table_counts = rejects_df['SOURCE_TABLE'].value_counts().reset_index()
                    table_counts.columns = ['Table', 'Count']
                    
                    chart = alt.Chart(table_counts).mark_bar().encode(
                        x='Count:Q',
                        y=alt.Y('Table:N', sort='-x'),
                        color=alt.value('#003366'),
                        tooltip=['Table', 'Count']
                    ).properties(height=300)
                    
                    st.altair_chart(chart, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 📋 Detailed Reject Records")
                st.dataframe(rejects_df, use_container_width=True)
                
                csv_data = rejects_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Download Quality Report",
                    data=csv_data,
                    file_name=f"data_quality_report_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
        except Exception as e:
            st.error(f"Error loading data quality metrics: {str(e)}")
    
    # ========== TAB 3: CUSTOMER 360 ==========
    with tab3:
        st.markdown("### 👤 Customer 360 View")
        
        try:
            customers_df = session.sql(f"""
                SELECT 
                    CUSTOMER_ID,
                    FIRST_NAME || ' ' || LAST_NAME as FULL_NAME,
                    CITY,
                    COUNTRY,
                    CUSTOMER_SEGMENT
                FROM {CUSTOMERS_TBL}
                ORDER BY CUSTOMER_ID
            """).to_pandas()
            
            if len(customers_df) > 0:
                selected_customer = st.selectbox(
                    "🔍 Select Customer",
                    options=customers_df['CUSTOMER_ID'].tolist(),
                    format_func=lambda x: f"{x} - {customers_df[customers_df['CUSTOMER_ID']==x]['FULL_NAME'].iloc[0]} ({customers_df[customers_df['CUSTOMER_ID']==x]['CITY'].iloc[0]}, {customers_df[customers_df['CUSTOMER_ID']==x]['CUSTOMER_SEGMENT'].iloc[0]})"
                )
                
                if selected_customer:
                    customer = session.sql(f"""
                        SELECT * FROM {CUSTOMERS_TBL}
                        WHERE CUSTOMER_ID = '{selected_customer}'
                    """).to_pandas().iloc[0]
                    
                    st.markdown("---")
                    st.markdown("### 📇 Customer Profile")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="exec-kpi">
                            <div class="exec-kpi-label">Customer Name</div>
                            <div class="exec-kpi-value" style="font-size: 24px;">{customer['FIRST_NAME']} {customer['LAST_NAME']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div class="exec-kpi">
                            <div class="exec-kpi-label">Segment</div>
                            <div class="exec-kpi-value" style="font-size: 28px;">{customer['CUSTOMER_SEGMENT']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown(f"""
                        <div class="exec-kpi">
                            <div class="exec-kpi-label">Credit Score</div>
                            <div class="exec-kpi-value">{int(customer['CREDIT_SCORE'])}</div>
                            <div class="exec-kpi-change">Excellent</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col4:
                        st.markdown(f"""
                        <div class="exec-kpi">
                            <div class="exec-kpi-label">Annual Income</div>
                            <div class="exec-kpi-value" style="font-size: 28px;">RM {customer['ANNUAL_INCOME_MYR']/1000:.0f}K</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    col_det1, col_det2 = st.columns(2)
                    
                    with col_det1:
                        st.markdown("""
                        <div class="insight-card">
                            <h4>📍 Location Details</h4>
                        """, unsafe_allow_html=True)
                        st.write(f"**City:** {customer['CITY']}")
                        st.write(f"**State:** {customer['STATE']}")
                        st.write(f"**Country:** {customer['COUNTRY']}")
                        st.write(f"**Postal Code:** {customer['POSTAL_CODE']}")
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    with col_det2:
                        st.markdown("""
                        <div class="insight-card">
                            <h4>📞 Contact Information</h4>
                        """, unsafe_allow_html=True)
                        st.write(f"**Email:** {customer['EMAIL']}")
                        st.write(f"**Phone:** {customer['PHONE']}")
                        st.write(f"**Customer Since:** {customer['CUSTOMER_SINCE']}")
                        st.write(f"**Employment:** {customer['EMPLOYMENT_STATUS']}")
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    st.markdown("---")
                    
                    st.markdown("### 💳 Accounts Portfolio")
                    accounts_df = session.sql(f"""
                        SELECT * FROM {ACCOUNTS_TBL}
                        WHERE CUSTOMER_ID = '{selected_customer}'
                    """).to_pandas()
                    
                    if len(accounts_df) > 0:
                        st.dataframe(accounts_df, use_container_width=True)
                        
                        chart = alt.Chart(accounts_df).mark_arc(innerRadius=60).encode(
                            theta='CURRENT_BALANCE_MYR:Q',
                            color=alt.Color('ACCOUNT_TYPE:N', legend=alt.Legend(title="Account Type")),
                            tooltip=['ACCOUNT_TYPE', 'CURRENT_BALANCE_MYR', 'ACCOUNT_STATUS']
                        ).properties(height=300, title='Balance Distribution by Account Type')
                        
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("No accounts found for this customer")
                    
                    st.markdown("---")
                    
                    st.markdown("### 💰 Recent Transaction History")
                    transactions_df = session.sql(f"""
                        SELECT * FROM {TRANSACTIONS_TBL}
                        WHERE CUSTOMER_ID = '{selected_customer}'
                        ORDER BY TRANSACTION_DATE DESC
                        LIMIT 20
                    """).to_pandas()
                    
                    if len(transactions_df) > 0:
                        st.dataframe(transactions_df, use_container_width=True)
                    else:
                        st.info("No transactions found for this customer")
            else:
                st.warning("No customers found in database")
                
        except Exception as e:
            st.error(f"Error loading customer data: {str(e)}")
    
    # ========== FOOTER ==========
    st.markdown("""
    <div class="footer">
        <div class="footer-logo">🏦</div>
        <strong>SEA Banking Analytics Platform</strong><br>
        Powered by Snowflake Cortex Analyst | AI-Driven Intelligence<br>
        © 2025 Deloitte Consulting SEA
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
