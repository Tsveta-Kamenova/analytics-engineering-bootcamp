# py Data_Profiling_Python_Customers_sample_NOviz.py

# -----------------------------
# BigQuery Data Profiling Script (No Visuals)
# -----------------------------
# Author: Tsveta (for Analytics Engineering Bootcamp)
# Purpose: Profile table structure, quality, and stats from BigQuery using Python
# Prerequisites:
#   - pip install google-cloud-bigquery pandas ydata-profiling pyarrow
#   - gcloud auth application-default login
# ------------------------------------------------------

from google.cloud import bigquery
import pandas as pd
from ydata_profiling import ProfileReport
import os

# -----------------------------
# Configuration
# -----------------------------
PROJECT_ID = "eighth-keyword-476220-f6"
DATASET = "dl_northwind"
TABLE = "customer"
REPORT_FILE = f"{TABLE}_profiling_report_v2.html"

# -----------------------------
# Connect and load data
# -----------------------------
client = bigquery.Client(project=PROJECT_ID)
query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`"
df = client.query(query).to_dataframe()

print(f"✅ Loaded {df.shape[0]} rows and {df.shape[1]} columns from {TABLE}.")
print("\n📄 Schema & Data Types:")
print(df.info())

print("\n🔍 Sample Data:")
print(df.head())

print("\n📊 Descriptive Statistics:")
print(df.describe(include='all'))

# -----------------------------
# Generate YData Profiling Report
# -----------------------------
print("\n📝 Generating full YData Profiling report (no visuals)...")

profile = ProfileReport(
    df,
    title=f"{TABLE.capitalize()} Profiling Report"
)

profile.to_file(REPORT_FILE)

print(f"✅ HTML profiling report saved as: {REPORT_FILE}")
