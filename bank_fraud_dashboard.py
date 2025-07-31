import streamlit as st
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from mysql.connector import Error

# -------------------
# Layout
# -------------------
st.set_page_config(page_title="Bank Fraud Detection", layout="wide")

# -------------------
# Sidebar - Query Selector
# -------------------
st.title("Bank Fraud Analytics Dashboard")
query_option = st.sidebar.selectbox(
    "Choose an analysis to run:",
    (
        "Top Failed Logins (7 days)",
        "Accounts with >5 Failures (1 day)",
        "High Device Failures",
        "Multi-Account Access via Device",
        "Multi-Location Access (60 mins)",
        "Blacklisted Devices Access",
        "Parties with Active Fraud Alerts",
        "Multiple Alerts per Party"
    )
)

# -------------------
# SQL Query Definitions
# -------------------
query_map = {
"Top Failed Logins (7 days)": """
    SELECT cd.party_id, cd.phone_number, COUNT(*) AS failed_logins
    FROM customer_data cd
    JOIN login_instance_data lid ON cd.party_id = lid.party_id
    WHERE lid.successful = FALSE AND lid.timestamp >= CURRENT_DATE - INTERVAL 7 DAY
    GROUP BY cd.party_id, cd.phone_number
    ORDER BY failed_logins DESC
    LIMIT 10
""",

    "Accounts with >5 Failures (1 day)": """
        SELECT cad.account_id, COUNT(*) AS failed_attempts, MAX(lid.timestamp) AS last_failed_attempt
        FROM login_instance_data lid
        JOIN customer_account_data cad ON lid.party_id = cad.party_id
        WHERE lid.successful = FALSE AND lid.timestamp >= NOW() - INTERVAL 1 DAY
        GROUP BY cad.account_id
        HAVING failed_attempts > 5
        ORDER BY failed_attempts DESC
    """,
    "High Device Failures": """
        SELECT lid.device_id, COUNT(*) AS failure_count
        FROM login_instance_data lid
        WHERE lid.successful = FALSE AND lid.timestamp >= NOW() - INTERVAL 7 DAY
        GROUP BY lid.device_id
        HAVING failure_count > 10
        ORDER BY failure_count DESC
    """,
    "Multi-Account Access via Device": """
        SELECT lid.device_id, COUNT(DISTINCT cad.account_id) AS distinct_accounts_accessed
        FROM login_instance_data lid
        JOIN customer_account_data cad ON lid.party_id = cad.party_id
        WHERE lid.timestamp >= NOW() - INTERVAL 7 DAY
        GROUP BY lid.device_id
        HAVING COUNT(DISTINCT cad.account_id) > 3
        ORDER BY distinct_accounts_accessed DESC
    """,
    "Multi-Location Access (60 mins)": """
        SELECT lid.party_id, COUNT(DISTINCT lid.location_id) AS location_count,
               MIN(lid.timestamp) AS first_access, MAX(lid.timestamp) AS last_access
        FROM login_instance_data lid
        WHERE lid.timestamp >= NOW() - INTERVAL 1 DAY
        GROUP BY lid.party_id
        HAVING location_count > 1 AND TIMESTAMPDIFF(MINUTE, first_access, last_access) <= 60
        ORDER BY location_count DESC
    """,
    "Blacklisted Devices Access": """
        SELECT '⚠️ No blacklist flag available in device_data' AS message
    """,
    "Parties with Active Fraud Alerts": """
        SELECT DISTINCT ca.party_id
        FROM fraud_alert_data fa
        JOIN transaction_data td ON fa.transaction_id = td.transaction_id
        JOIN customer_account_data ca 
          ON td.payer_party_id = ca.party_id OR td.payee_party_id = ca.party_id 
        WHERE fa.status IN ('active', 'open')
    """,
    "Multiple Alerts per Party": """
        SELECT ca.party_id,
               GROUP_CONCAT(DISTINCT ca.account_id ORDER BY ca.account_id) AS associated_accounts,
               COUNT(*) AS fraud_alert_count
        FROM fraud_alert_data fa
        JOIN transaction_data td ON fa.transaction_id = td.transaction_id
        JOIN customer_account_data ca 
          ON td.payer_party_id = ca.party_id OR td.payee_party_id = ca.party_id 
        WHERE fa.status IN ('active', 'open')
        GROUP BY ca.party_id
        HAVING fraud_alert_count > 1
        ORDER BY fraud_alert_count DESC
    """
}

# -------------------
# Main Header
# -------------------
st.title("🧫 Bank Fraud Detection Report")
st.markdown("Use this dashboard to explore patterns that may indicate fraudulent behavior across customer activity.")

# -------------------
# Database Connector
# -------------------
def get_connection():
    try:
        return mysql.connector.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"]
        )
    except Error as e:
        st.error(f"Error connecting to MySQL DB: {e}")
        return None


# -------------------
# Run Query 
# -------------------
def run_query(sql_query, params=None):
    conn = get_connection()
    cursor = None
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query, params or ())
            result = cursor.fetchall()
            columns = cursor.column_names
            df = pd.DataFrame(result, columns=columns)
            return df
        except Error as e:
            st.error(f"Query failed: {e}")
        finally:
            if cursor:
                cursor.close()
            conn.close()
    else:
        st.warning("Connection to the database could not be established.")
    return pd.DataFrame()


selected_query = query_map.get(query_option)

st.subheader(f"📊 Results: {query_option}")

if selected_query:
    with st.spinner("Running query..."):
        result_df = run_query(selected_query)

    if not result_df.empty:
        st.dataframe(result_df, use_container_width=True)

        st.subheader("📈 Visual Summary")

        if query_option == "Top Failed Logins (7 days)":
             # ⛔️ Commented out to avoid empty DataFrame error
             # result_df = result_df[result_df['failed_logins'] > 1]

            fig, ax = plt.subplots()
            result_df.plot(kind='bar', x='party_id', y='failed_logins', ax=ax)
            ax.set_ylabel("Failed Logins")
            ax.set_title("Top Failed Logins (Past 7 Days)")
            st.pyplot(fig)



        elif query_option == "Accounts with >5 Failures (1 day)":
            fig, ax = plt.subplots()
            result_df.plot(kind='bar', x='account_id', y='failed_attempts', ax=ax)
            ax.set_ylabel("Failures")
            ax.set_title("Accounts with >5 Failures in 1 Day")
            st.pyplot(fig)

        elif query_option == "High Device Failures":
            fig, ax = plt.subplots()
            result_df.plot(kind='bar', x='device_id', y='failure_count', ax=ax)
            ax.set_ylabel("Failure Count")
            ax.set_title("Devices with Most Login Failures")
            st.pyplot(fig)

        elif query_option == "Multi-Account Access via Device":
            fig, ax = plt.subplots()
            result_df.plot(kind='bar', x='device_id', y='distinct_accounts_accessed', ax=ax)
            ax.set_ylabel("Accounts Accessed")
            ax.set_title("Devices Accessing Multiple Accounts")
            st.pyplot(fig)

        elif query_option == "Multiple Alerts per Party":
            if {'party_id', 'fraud_alert_count'}.issubset(result_df.columns):
                result_df_sorted = result_df.sort_values(by='fraud_alert_count', ascending=False)
                fig, ax = plt.subplots(figsize=(10, 6))
                result_df_sorted.plot(kind='bar', x='party_id', y='fraud_alert_count', ax=ax, legend=False, color='firebrick')
                ax.set_ylabel("Alert Count")
                ax.set_xlabel("Party ID")
                ax.set_title("Parties with Multiple Fraud Alerts")
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                st.pyplot(fig)
            else:
                st.warning("Expected columns 'party_id' and 'fraud_alert_count' not found in query result.")

        st.markdown("### 📌 Summary Stats")
        with st.expander("Show Summary"):
            st.write("🔢 Row count:", len(result_df))
            numeric_cols = result_df.select_dtypes(include='number').columns
            if len(numeric_cols) > 0:
                st.dataframe(result_df[numeric_cols].describe())
            else:
                st.info("No numeric columns to summarize.")

        with st.expander("📈 Summary Statistics"):
            st.write(result_df.describe(include='all'))

        st.download_button(
            label="📅 Download results as CSV",
            data=result_df.to_csv(index=False).encode('utf-8'),
            file_name='fraud_detection_results.csv',
            mime='text/csv'
        )
    else:
        st.warning("⚠️ No results found for this query.")
