#py all_data_profiles_hardcoded.py

# -----------------------------
# BigQuery Data Profiling Script (Hardcoded Tables)
# -----------------------------
# Author: Tsveta
# Purpose: Profile multiple hardcoded tables from BigQuery into a single HTML report
# Prerequisites:
#   pip install google-cloud-bigquery pandas ydata-profiling pyarrow
#   gcloud auth application-default login
# -----------------------------

from google.cloud import bigquery
import pandas as pd
from ydata_profiling import ProfileReport

# -----------------------------
# Config
# -----------------------------
PROJECT_ID = "eighth-keyword-476220-f6"
DATASET = "dl_northwind"
REPORT_FILE = "combined_profiling_report_hardcoded.html"

# Hardcoded table list
TABLES = [
    "employee_privileges", "employees", "inventory_transaction_types",
    "inventory_transactions", "invoices", "order_details",
    "order_details_status", "orders", "orders_status",
    "orders_tax_status", "privileges", "products",
    "purchase_order_details", "purchase_order_status", "purchase_orders",
    "sales_reports", "shippers", "strings", "suppliers", "customer"
]

# -----------------------------
# Initialize BigQuery client
# -----------------------------
client = bigquery.Client(project=PROJECT_ID)

# -----------------------------
# Load each table into DataFrame
# -----------------------------
dfs = []
for table in TABLES:
    print(f"📌 Loading table: {table}")
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table}`"
    df = client.query(query).to_dataframe()
    df["__table_name__"] = table
    dfs.append(df)

# Combine all tables into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)
print(f"✅ Combined {len(TABLES)} tables into one DataFrame with shape: {combined_df.shape}")

# -----------------------------
# Show schema info
# -----------------------------
print("\n📄 Combined Schema & Data Types:")
combined_df.info()

# -----------------------------
# Descriptive statistics
# -----------------------------
print("\n🔍 Sample Data:")
print(combined_df.head())

print("\n📊 Descriptive Statistics:")
print(combined_df.describe(include='all'))

# -----------------------------
# Generate HTML profiling report
# -----------------------------
print("\n📝 Generating combined HTML profiling report...")
profile = ProfileReport(
    combined_df,
    title="Combined BigQuery Profiling Report (Hardcoded Tables)",
    explorative=True,
    minimal=False  # Full detailed profiling
)
profile.to_file(REPORT_FILE)
print(f"✅ Combined HTML profiling report saved as: {REPORT_FILE}")
