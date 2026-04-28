import sqlite3
import pandas as pd

conn = sqlite3.connect("../database/healthcare.db")

query = "SELECT * FROM facilities_raw LIMIT 5"
df = pd.read_sql(query, conn)

print(df)

conn.close()