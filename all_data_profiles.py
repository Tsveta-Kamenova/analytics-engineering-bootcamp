# py all_data_profiles.py

# all_data_profiles_with_column_dupes.py
# -----------------------------
# BigQuery Data Profiling Script (per-table HTMLs, detailed duplicates)
# -----------------------------
# Author: Tsveta
# -----------------------------

from google.cloud import bigquery
import pandas as pd
import os

# -----------------------------
# Config
# -----------------------------
PROJECT_ID = "eighth-keyword-476220-f6"
DATASET = "dl_northwind"
OUTPUT_FOLDER = "table_profiles"

# Create output folder
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------
# Initialize BigQuery client
# -----------------------------
client = bigquery.Client(project=PROJECT_ID)

# -----------------------------
# List all tables in dataset
# -----------------------------
tables = [t.table_id for t in client.list_tables(DATASET)]
print(f"📋 Found {len(tables)} tables: {tables}")

# -----------------------------
# Profile each table
# -----------------------------
summary_rows = []

for table in tables:
    print(f"\n🔍 Profiling table: {table}")
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table}`"
    
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        print(f"⚠️ Could not fetch table {table}: {e}")
        continue

    # Basic info
    n_rows, n_cols = df.shape
    row_dupes = df[df.duplicated()]
    row_dup_pct = round(len(row_dupes) / len(df) * 100, 2) if len(df) else 0
    max_missing_pct = round(df.isna().mean().max() * 100, 2) if len(df) else 0

    # Column-level duplicates: which columns have duplicate values
    col_dupes_detail = {}
    for col in df.columns:
        dup_vals = df[col][df[col].duplicated(keep=False)]
        if dup_vals.shape[0] > 0:
            dup_counts = dup_vals.value_counts().reset_index()
            dup_counts.columns = [col, "Count"]
            col_dupes_detail[col] = dup_counts

    # Missing columns
    missing_cols = [col for col in df.columns if df[col].isna().any()]
    missing_cols_str = ", ".join(missing_cols) if missing_cols else "None"

    # Summary record
    summary_rows.append({
        "Table": table,
        "Rows": n_rows,
        "Columns": n_cols,
        "% Missing (max col)": max_missing_pct,
        "% Row Duplicates": row_dup_pct,
        "Columns with duplicate values": ", ".join(col_dupes_detail.keys()) if col_dupes_detail else "None",
        "Columns with missing values": missing_cols_str
    })

    # -----------------------------
    # Write HTML per table
    # -----------------------------
    html_file = os.path.join(OUTPUT_FOLDER, f"{table}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(f"<html><head><title>{table} Profile</title></head><body>")
        f.write(f"<h1>Table: {table}</h1>")
        f.write(f"<p><b>Rows:</b> {n_rows} | <b>Columns:</b> {n_cols}</p>")
        f.write(f"<p><b>% Row Duplicates:</b> {row_dup_pct}%</p>")
        f.write(f"<p><b>Columns with missing values:</b> {missing_cols_str}</p>")
        f.write(f"<p><b>Columns with duplicate values:</b> "
                f"{', '.join(col_dupes_detail.keys()) if col_dupes_detail else 'None'}</p>")

        # Sample data
        f.write("<h2>Sample (first 10 rows)</h2>")
        f.write(df.head(10).to_html(index=False, border=1))

        # Statistics
        f.write("<h2>Descriptive Statistics</h2>")
        f.write(df.describe(include='all').to_html(border=1))

        # Row-level duplicates
        if not row_dupes.empty:
            f.write("<h2>Duplicate Rows (full match)</h2>")
            f.write(row_dupes.to_html(index=False, border=1))
        else:
            f.write("<h2>Duplicate Rows (full match)</h2><p>✅ None found</p>")

        # Column-level duplicates (detailed)
        if col_dupes_detail:
            f.write("<h2>Column-level Duplicates</h2>")
            for col, dup_df in col_dupes_detail.items():
                f.write(f"<h3>{col}</h3>")
                f.write(dup_df.to_html(index=False, border=1))
        else:
            f.write("<h2>Column-level Duplicates</h2><p>✅ None found</p>")

        f.write("</body></html>")
    
    print(f"✅ Saved: {html_file}")

# -----------------------------
# Create summary HTML linking all tables
# -----------------------------
summary_df = pd.DataFrame(summary_rows)
summary_html = os.path.join(OUTPUT_FOLDER, "summary.html")

with open(summary_html, "w", encoding="utf-8") as f:
    f.write("<html><head><title>Dataset Profiling Summary</title></head><body>")
    f.write("<h1>Dataset Profiling Summary</h1>")
    f.write("<table border='1'><tr><th>Table</th><th>Rows</th><th>Columns</th>"
            "<th>% Missing (max col)</th><th>% Row Duplicates</th>"
            "<th>Columns with duplicate values</th><th>Columns with missing values</th></tr>")
    for _, r in summary_df.iterrows():
        link = f"<a href='{r['Table']}.html'>{r['Table']}</a>"
        f.write(f"<tr><td>{link}</td><td>{r['Rows']}</td><td>{r['Columns']}</td>"
                f"<td>{r['% Missing (max col)']}</td><td>{r['% Row Duplicates']}</td>"
                f"<td>{r['Columns with duplicate values']}</td><td>{r['Columns with missing values']}</td></tr>")
    f.write("</table></body></html>")

print(f"\n🎯 Summary HTML saved to {summary_html}")
print("✅ Profiling completed for all tables.")
