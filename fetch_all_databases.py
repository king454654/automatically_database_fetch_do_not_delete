import os
import json
from dotenv import load_dotenv
from databricks import sql

# Load environment variables
load_dotenv()
DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

def fetch_all_databases():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        rows = cursor.fetchall()
        databases = [row[0] for row in rows]
        return databases
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    dbs = fetch_all_databases()
    with open("databases.json", "w") as f:
        json.dump(dbs, f, indent=2)
    print("Saved all databases to databases.json")
