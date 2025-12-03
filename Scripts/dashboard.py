# ---------------- Imports ----------------
import streamlit as st
st.set_page_config(page_title="Vendor & Brand Analytics", layout="wide")  # Must be first Streamlit command

import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# ---------------- Database Connection ----------------
engine = create_engine("mysql+pymysql://root:Sanjana%401611@localhost:3306/inventorydb")

# ---------------- Load Data ----------------
@st.cache_data
def load_data():
    vendor_summary = pd.read_sql("SELECT * FROM vendor_final_summary", con=engine)
    purchase_contribution = pd.read_sql("SELECT * FROM purchase_contribution", con=engine)
    lowturnover = pd.read_sql("SELECT * FROM lowturnovervendors", con=engine)
    brand_perf = pd.read_sql("SELECT * FROM brandperformance", con=engine)
    return vendor_summary, purchase_contribution, lowturnover, brand_perf

vendor_summary, purchase_contribution, lowturnover, brand_perf = load_data()

# ---------------- Dashboard Layout ----------------
st.title("📊 Vendor & Brand Analytics Dashboard")

# -------- Section 1: Purchase Contribution --------
st.subheader("Top Vendors by Purchase Contribution")
fig1 = px.pie(
    purchase_contribution,
    names="VendorName",
    values="PurchaseContributionPercent",
    title="Purchase Contribution (%)"
)
st.plotly_chart(fig1, use_container_width=True)

# -------- Section 2: Low Turnover Vendors --------
st.subheader("Bottom 10 Vendors (Low Turnover)")
fig2 = px.bar(
    lowturnover,
    x="VendorName",
    y="PurchaseContributionPercent",
    title="Low Turnover Vendors",
    text="PurchaseContributionPercent"
)
st.plotly_chart(fig2, use_container_width=True)

# -------- Section 3: Brand Performance --------
st.subheader("Top Brands by Purchase Contribution")
fig3 = px.pie(
    brand_perf,
    names="Brand",
    values="PurchaseContributionPercent",
    title="Brand Contribution (%)"
)
st.plotly_chart(fig3, use_container_width=True)

# -------- Section 4: Vendor Profitability --------
st.subheader("Vendor Profitability (Profit Margin %)")
fig4 = px.scatter(
    vendor_summary,
    x="TotalSalesDollars",
    y="ProfitMargin",
    size="GrossProfit",
    color="VendorName",
    hover_data=["VendorName", "GrossProfit"]
)
st.plotly_chart(fig4, use_container_width=True)

st.success("✅ Dashboard generated successfully!")



