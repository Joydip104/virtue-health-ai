import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from databricks import sql

# ---------------------------------------
# Load environment variables
# ---------------------------------------
load_dotenv()

HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "default")

# ---------------------------------------
# Validate config
# ---------------------------------------
if not HOST or not HTTP_PATH or not TOKEN:
    raise ValueError("Missing Databricks credentials in .env")

# ---------------------------------------
# Read SQLite table
# ---------------------------------------
sqlite_conn = sqlite3.connect("../database/healthcare.db")

df = pd.read_sql("SELECT * FROM facilities_cleaned", sqlite_conn)
sqlite_conn.close()

print(f"Rows loaded from SQLite: {len(df)}")
print(f"Columns found: {len(df.columns)}")

# ---------------------------------------
# Convert all values safely to string
# ---------------------------------------
df = df.fillna("")

for col in df.columns:
    df[col] = df[col].astype(str)

# ---------------------------------------
# Databricks connection
# ---------------------------------------
conn = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    access_token=TOKEN
)

cursor = conn.cursor()

# ---------------------------------------
# Select catalog/schema
# ---------------------------------------
cursor.execute(f"USE CATALOG {CATALOG}")
cursor.execute(f"USE SCHEMA {SCHEMA}")

# ---------------------------------------
# Clean column names for Databricks SQL
# ---------------------------------------
# Replace spaces and invalid chars
clean_columns = []
rename_map = {}

for col in df.columns:
    clean = (
        col.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "_")
    )
    rename_map[col] = clean
    clean_columns.append(clean)

df.rename(columns=rename_map, inplace=True)

# ---------------------------------------
# Drop old table
# ---------------------------------------
cursor.execute("DROP TABLE IF EXISTS facilities_cleaned")

# ---------------------------------------
# Create table dynamically (all STRING)
# ---------------------------------------
column_defs = ",\n".join([f"`{col}` STRING" for col in clean_columns])

create_sql = f"""
CREATE TABLE facilities_cleaned (
{column_defs}
)
"""

cursor.execute(create_sql)
print("Table created.")

# ---------------------------------------
# Insert rows
# ---------------------------------------
placeholders = ", ".join(["?"] * len(clean_columns))
insert_sql = f"""
INSERT INTO facilities_cleaned VALUES ({placeholders})
"""

for _, row in df.iterrows():
    values = tuple(row[col] for col in clean_columns)
    cursor.execute(insert_sql, values)

print("Upload complete.")

# ---------------------------------------
# Verify
# ---------------------------------------
cursor.execute("SELECT COUNT(*) FROM facilities_cleaned")
count = cursor.fetchone()[0]

print("Rows in Databricks:", count)

cursor.close()
conn.close()