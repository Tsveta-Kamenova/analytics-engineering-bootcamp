# py Data_Profiling_Python_Customers_sample.py


# -----------------------------
# BigQuery Data Profiling Script
# -----------------------------
# Author: Tsveta (for Analytics Engineering Bootcamp)
# Purpose: Profile table structure, quality, and stats from BigQuery using Python
# Prerequisites:
#   - Run: pip install google-cloud-bigquery pandas ydata-profiling pyarrow matplotlib
#   - Run: gcloud auth application-default login
# ------------------------------------------------------
# Full Data Profiling Script for BigQuery
# ------------------------------------------------------


import matplotlib
matplotlib.use("Agg")  # Must be first to avoid GUI issues

from google.cloud import bigquery
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ydata_profiling import ProfileReport
import os

PROJECT_ID = "eighth-keyword-476220-f6"
DATASET = "dl_northwind"
TABLE = "customer"
REPORT_FILE = f"{TABLE}_profiling_report.html"
PLOTS_DIR = os.path.join("plots", TABLE)

# Create dynamic plots folder
os.makedirs(PLOTS_DIR, exist_ok=True)

# Load data
client = bigquery.Client(project=PROJECT_ID)
query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`"
df = client.query(query).to_dataframe()
print(f"✅ Loaded {df.shape[0]} rows and {df.shape[1]} columns from {TABLE}.")

# Basic overview
print(df.info())
print(df.head())

# Descriptive stats
print(df.describe(include='all'))

# Visualizations
numeric_cols = df.select_dtypes(include=['int64', 'float64', 'Int64']).columns
if len(numeric_cols) > 0:
    plt.figure(figsize=(10, 8))
    sns.heatmap(df[numeric_cols].corr(), annot=True, cmap='coolwarm')
    plt.title("Correlation Heatmap")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, "correlation_heatmap.png"))
    plt.close()

# Missing values heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(df.isnull(), cbar=False, yticklabels=False)
plt.title("Missing Values Heatmap")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "missing_values_heatmap.png"))
plt.close()

# Value counts for non-empty categorical columns
cat_cols = df.select_dtypes(include=['object']).columns
for col in cat_cols:
    if df[col].notnull().sum() > 0:  # skip empty columns
        plt.figure(figsize=(10, 5))
        df[col].value_counts(dropna=False).plot(kind='bar')
        plt.title(f"Value counts for {col}")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, f"{col}_value_counts.png"))
        plt.close()

print(f"✅ Plots saved in {PLOTS_DIR}")

# HTML report
print("📝 Generating HTML profiling report...")
profile = ProfileReport(df, title=f"{TABLE.capitalize()} Profiling Report", explorative=True)
profile.to_file(REPORT_FILE)
print(f"✅ HTML report saved as {REPORT_FILE}")
