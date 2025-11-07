from flask import Flask, request, jsonify, make_response, render_template
from flask_cors import CORS
import json, requests, os, re, subprocess
from decimal import Decimal
import sqlparse
import certifi
from dotenv import load_dotenv
from databricks import sql

# --- Load environment variables ---
load_dotenv()
GROQ_API_KEY = os.getenv("API_KEY")
DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# --- SSL fix ---
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# --- Flask setup ---
app = Flask(__name__)
CORS(app)

# --- Load database list from databases.json ---
def get_database_list():
    try:
        with open("databases.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

# --- Load current schema from all_databases_schema.json ---
def load_current_schema():
    try:
        with open("all_databases_schema.json", "r", encoding="utf-8") as f:
            raw = json.load(f)
            return {
                db_schema["database"]: {
                    **{
                        table["name"]: {
                            "columns": {col["column_name"]: col["type"] for col in table["columns"]}
                        } for table in db_schema.get("tables", [])
                    },
                    **{
                        view["name"]: {
                            "columns": {col["column_name"]: col["type"] for col in view["columns"]}
                        } for view in db_schema.get("views", [])
                    }
                } for db_schema in raw
            }
    except Exception as e:
        print(f"❌ Failed to load schema: {e}")
        return {}

schemas = load_current_schema()

# --- Qualify table names ---
def qualify_table_names(sql_text, db_name):
    def repl(match):
        table = match.group(2)
        if "." in table:
            return match.group(0)
        return f"{match.group(1)} {db_name}.{table}"
    return re.sub(r"\b(FROM|JOIN)\s+([a-zA-Z_][\w]*)", repl, sql_text, flags=re.IGNORECASE)

# --- Generate SQL using Groq ---
def generate_sql(prompt, db_schema, db_name):
    schema_info = "\n".join(
        f"{table}: {', '.join([f'{col} ({dtype})' for col, dtype in db_schema[table]['columns'].items()])}"
        for table in db_schema
    )

    system_msg = (
        f"You are an expert SQL assistant. Use the following schema from database `{db_name}`:\n{schema_info}\n"
        f"Do not use `information_schema` or any system schemas. Only query user tables.\n"
        f"Always generate a query for a table. Output valid SQL only. No explanations."
    )

    payload = {
        "model": "llama-3.3-70b-versatile",
        "temperature": 0,
        "max_tokens": 200,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt.strip()}
        ]
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    res = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        json=payload,
        headers=headers,
        verify=True
    )

    if res.status_code == 200:
        sql_text = res.json()["choices"][0]["message"]["content"]
        sql_text = re.sub(r"```sql\s*", "", sql_text, flags=re.IGNORECASE)
        sql_text = re.sub(r"\s*```$", "", sql_text)

        lines = sql_text.strip().splitlines()
        for i, line in enumerate(lines):
            if line.strip().lower().startswith(("select", "show", "with", "describe", "explain", "use")):
                sql_text = "\n".join(lines[i:])
                break

        sql_text = sql_text.replace("'your_database_name'", f"'{db_name}'")
        sql_text = qualify_table_names(sql_text, db_name)

        if "information_schema" in sql_text.lower():
            raise ValueError("Queries to `information_schema` are not supported in Databricks.")

        return str(sqlparse.parse(sql_text.strip())[0])
    else:
        raise Exception(f"GROQ error: {res.status_code} - {res.text}")

# --- Query Databricks ---
def query_databricks(sql_query, db_name=None):
    connection = sql.connect(
        server_hostname=DATABRICKS_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = connection.cursor()
    try:
        if db_name:
            cursor.execute(f"USE `{db_name}`")
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return rows, columns
    finally:
        cursor.close()
        connection.close()

# --- Generate Insight using Groq ---
def generate_insight(rows, columns, prompt):
    formatted = [
        {columns[i]: (float(cell) if isinstance(cell, Decimal) else cell) for i, cell in enumerate(row)}
        for row in rows
    ]

    payload = {
        "model": "llama-3.3-70b-versatile",
        "temperature": 0.3,
        "max_tokens": 300,
        "messages": [
            {"role": "system", "content": "You're a data analyst. Provide concise insights."},
            {"role": "user", "content": f"User question: {prompt}"},
            {"role": "user", "content": f"Data:\n{json.dumps(formatted, indent=2)}"}
        ]
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    res = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        json=payload,
        headers=headers,
        verify=True
    )

    if res.status_code == 200:
        return res.json()["choices"][0]["message"]["content"]
    else:
        raise Exception(f"GROQ Insight error: {res.status_code} - {res.text}")

# --- Routes ---
@app.route("/")
def index():
    db_names = get_database_list()
    return render_template("index.html", databases=db_names)

@app.route("/refresh_databases", methods=["POST"])
def refresh_databases():
    try:
        script_path = os.path.join(os.path.dirname(__file__), "fetch_all_databases.py")
        result = subprocess.run(
            ["python", script_path],
            check=True,
            capture_output=True,
            text=True
        )
        print("STDOUT:", result.stdout)
        return jsonify({"status": "Database list refreshed."})
    except subprocess.CalledProcessError as e:
        print("STDERR:", e.stderr)
        return make_response(jsonify({
            "error": "Failed to refresh databases.",
            "details": e.stderr or str(e)
        }), 500)

# @app.route("/load_schema", methods=["POST"])
# def load_schema():
#     data = request.get_json(force=True)
#     db_name = data.get("database")

#     if not db_name:
#         return make_response(jsonify({"error": "Missing database name"}), 400)

#     try:
#         script_path = os.path.join(os.path.dirname(__file__), "schema.py")
#         subprocess.run(["python", script_path, db_name], check=True)
#         global schemas
#         schemas = load_current_schema()
#         return jsonify({"status": f"Schema for `{db_name}` loaded successfully."})
#     except subprocess.CalledProcessError as e:
#         return make_response(jsonify({"error": f"Failed to load schema: {e}"}), 500)


@app.route("/load_schema", methods=["POST"])
def load_schema():
    data = request.get_json(force=True)
    db_name = data.get("database")

    if not db_name:
        return make_response(jsonify({"error": "Missing database name"}), 400)

    try:
        # ✅ Clear schema file before loading new one
        schema_path = os.path.join(os.path.dirname(__file__), "all_databases_schema.json")
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump([], f)

        # ✅ Run schema.py to load selected database schema
        script_path = os.path.join(os.path.dirname(__file__), "schema.py")
        subprocess.run(["python", script_path, db_name], check=True)

        # ✅ Reload schema into memory
        global schemas
        schemas = load_current_schema()

        return jsonify({"status": f"Schema for `{db_name}` loaded successfully."})
    except subprocess.CalledProcessError as e:
        return make_response(jsonify({"error": f"Failed to load schema: {e}"}), 500)



@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        data = request.get_json(force=True)
        prompt = data.get("prompt")
        db_name = data.get("database")

        if not prompt or not db_name:
            return make_response(jsonify({"error": "Missing prompt or database"}), 400)

        if db_name not in schemas:
            return make_response(jsonify({"error": f"Unknown database: {db_name}"}), 400)

        db_schema = schemas[db_name]
        sql_query = generate_sql(prompt, db_schema, db_name)
        rows, columns = query_databricks(sql_query, db_name)
        insight = generate_insight(rows, columns, prompt) if rows else "No data found for this query."

        return jsonify({
            "sql": sql_query,
            "columns": columns,
            "rows": rows,
            "insight": insight
        })

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)

# # --- Run locally ---
# if __name__ == "__main__":
#     if os.environ.get("STREAMLIT
# --- Run locally ---
if __name__ == "__main__":
    if os.environ.get("STREAMLIT_RUNTIME") is None:
        app.run(host="0.0.0.0", port=5000, debug=True)