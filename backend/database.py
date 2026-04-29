# backend/database.py

import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "default")


def get_connection():
    conn = sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN
    )
    return conn


def get_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"USE CATALOG {CATALOG}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")
    return conn, cursor