#py all_data_profiles.py

from google.cloud import bigquery
import pandas as pd
from ydata_profiling import ProfileReport

# Config
PROJECT_ID = "eighth-keyword-476220-f6"
DATASET = "dl_northwind"
REPORT_FILE = "bigquery_tables_profile.html"

client = bigquery.Client(project=PROJECT_ID)

# Get all tables
tables = [table.table_id for table in client.list_tables(DATASET)]
print(f"Found {len(tables)} tables: {tables}")

# Containers
report_sections = []
summary_list = []

for table in tables:
    print(f"Profiling table: {table}")
    df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table}`").to_dataframe()

    # Add table name column
    df["__table_name__"] = table

    # Summary for this table
    total_rows = len(df)
    total_cols = df.shape[1]
    missing_pct = round(df.isna().mean().max() * 100, 2)
    dup_pct = round(df.duplicated().mean() * 100, 2)
    summary_list.append({
        "Table": table,
        "Rows": total_rows,
        "Columns": total_cols,
        "% Missing (max col)": missing_pct,
        "% Duplicates": dup_pct
    })

    # Create profile (alerts, duplicates, correlations)
    profile = ProfileReport(df, title=f"Profile: {table}", explorative=True, minimal=True)
    html_str = profile.to_html()
    section = f"<h2>Table: {table}</h2>\n" + html_str
    report_sections.append(section)

# Summary table
summary_df = pd.DataFrame(summary_list)
summary_html = "<h1>Summary of All Tables</h1>\n" + summary_df.to_html(index=False, border=1)

# Combine all
full_html = "<html><head><title>BigQuery Tables Profiling</title></head><body>"
full_html += summary_html
full_html += "<hr>\n" + "\n<hr>\n".join(report_sections)
full_html += "</body></html>"

# Save
with open(REPORT_FILE, "w", encoding="utf-8") as f:
    f.write(full_html)

print(f"✅ Combined HTML profiling report saved as: {REPORT_FILE}")
