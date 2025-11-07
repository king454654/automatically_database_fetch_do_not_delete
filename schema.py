import json
import os
import sys
import certifi
from databricks import sql

# ✅ Ensure SSL works for requests
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# 🔐 Load credentials from environment
from dotenv import load_dotenv
load_dotenv()
HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

def describe_entity(cursor, entity_name):
    cursor.execute(f"DESCRIBE TABLE `{entity_name}`")
    return [
        {"column_name": row[0], "type": row[1]}
        for row in cursor.fetchall()
        if row[0] and row[1] and row[0].lower() != "col_name"
    ]

def main():
    if len(sys.argv) < 2:
        print("❌ Error: No database name provided.")
        sys.exit(1)

    db_name = sys.argv[1]
    print(f"🔍 Extracting schema for: {db_name}")

    # Connect to Databricks
    connection = sql.connect(
        server_hostname=HOSTNAME,
        http_path=HTTP_PATH,
        access_token=TOKEN
    )
    cursor = connection.cursor()

    try:
        cursor.execute(f"USE `{db_name}`")

        # --- Extract tables ---
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_schemas = []
        for row in tables:
            table_name = row[1]
            columns = describe_entity(cursor, table_name)
            table_schemas.append({"name": table_name, "columns": columns})

        # --- Extract views ---
        cursor.execute("SHOW VIEWS")
        views = cursor.fetchall()
        view_schemas = []
        for row in views:
            view_name = row[1]
            columns = describe_entity(cursor, view_name)
            view_schemas.append({"name": view_name, "columns": columns})

        # --- Build schema object ---
        full_schema = {
            "database": db_name,
            "tables": table_schemas,
            "views": view_schemas
        }

        # --- Save to file ---
        schema_path = "all_databases_schema.json"
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump([full_schema], f, indent=2)

        print(f"✅ Schema for '{db_name}' saved to {schema_path}")

    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
