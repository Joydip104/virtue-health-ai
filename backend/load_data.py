import pandas as pd
import sqlite3

# Connect database
conn = sqlite3.connect("../database/healthcare.db")

# Load CSV
df = pd.read_csv("../data/Virtue Foundation Ghana v0.3 - Sheet1.csv")

# Save to database
df.to_sql("facilities_raw", conn, if_exists="replace", index=False)

print("Data loaded successfully!")

conn.close()