import streamlit as st
import pandas as pd
import psycopg2
import os

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres-db")
DB_PORT = os.environ.get("POSTGRES_PORT", 5432)
DB_NAME = os.environ.get("POSTGRES_DB", "airflow")
DB_USER = os.environ.get("POSTGRES_USER", "airflow")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "airflow")

DB_SCHEMA = "analytics_staging"
DB_TABLE = "stg_orders"

@st.cache_data(ttl=60)
def fetch_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("[INFO] Streamlit connection successful!")
        
        query = f"SELECT * FROM {DB_SCHEMA}.{DB_TABLE};"
        df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

st.set_page_config(layout="wide")
st.title("Order Analytics (Live)")

df = fetch_data()

if not df.empty:
    st.success(f"Loaded {len(df)} orders from DWH.")
    
    col1, col2, col3 = st.columns(3)
    total_revenue = df['total_price'].sum()
    avg_check = df['total_price'].mean()
    
    col1.metric("Total Revenue", f"{total_revenue:,.2f}")
    col2.metric("Average Check", f"{avg_check:,.2f}")
    col3.metric("Total Orders", f"{len(df)}")
    
    st.divider()

    st.header("Order Dynamics")
    
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    orders_by_hour = df.set_index('created_at').resample('h').size().reset_index(name='order_count')
    
    st.bar_chart(orders_by_hour, x='created_at', y='order_count')
    
    st.divider()
    
    st.header("Raw Data (from dbt View)")
    st.dataframe(df)

else:
    st.warning("Data not yet loaded. Please run the Airflow DAG.")