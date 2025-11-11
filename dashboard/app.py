import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Retail Sales Demo", layout="wide")
st.title("🛍️ Retail Sales — Simple PySpark Demo")

# file paths (relative to project root)
raw_path = "outputs/processed_transactions"
daily_path = "outputs/daily_sales"
top_path = "outputs/top_products"

# check processed data
if not os.path.exists(raw_path):
    st.error("Processed transactions not found. Run: python src/etl.py")
    st.stop()

# load small sample of processed transactions
raw = pd.read_parquet(raw_path)
st.subheader("Sample processed transactions")
st.dataframe(raw.head(10))

# load and show daily sales if available
if os.path.exists(daily_path):
    daily = pd.read_parquet(daily_path)
    # ensure date column is datetime
    daily['date'] = pd.to_datetime(daily['date'])
    st.subheader("Daily revenue")
    # date range selector
    c1, c2 = st.columns([3,1])
    with c2:
        start, end = st.date_input("Date range", [daily['date'].min(), daily['date'].max()])
    filtered = daily[(daily['date'] >= pd.to_datetime(start)) & (daily['date'] <= pd.to_datetime(end))]
    st.line_chart(filtered.set_index('date')['daily_revenue'])

# load and show top products if available
if os.path.exists(top_path):
    top = pd.read_parquet(top_path)
    top = top.sort_values('revenue', ascending=False)
    st.subheader("Top products")
    # Top 5 bar chart + table
    top5 = top.head(5).set_index('product_name')
    st.markdown("**Top 5 products by revenue**")
    st.columns(2)
    st.bar_chart(top5['revenue'])
    st.table(top5[['revenue', 'quantity_sold']])

st.markdown("---")
st.caption("Data: sample CSV → PySpark ETL → Parquet. Project repo: retail-simple.")
