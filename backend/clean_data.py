import sqlite3
import pandas as pd

conn = sqlite3.connect("../database/healthcare.db")

df = pd.read_sql("SELECT * FROM facilities_raw", conn)

# Basic cleaning
df = df.drop_duplicates()

df.columns = df.columns.str.strip()

# Example fill missing values
df = df.fillna("Unknown")

# Save cleaned table
df.to_sql("facilities_cleaned", conn, if_exists="replace", index=False)

print("Cleaned data created!")

conn.close()