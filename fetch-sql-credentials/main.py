import json
import psycopg
from google.cloud import secretmanager

# ---------- Secret Manager helpers ----------

def get_secret_value(name: str) -> str:
    """Fetch secret value from Secret Manager (fixed project)."""
    project_id = "vertex-ai-scraper-project"  # literal project id
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("utf-8")

# ---------- Core function ----------

def fetch_global_creds():
    """
    Connects to rag_global database using credentials stored in Secret Manager.
    Returns the first row of the credentials table as a Python dict.
    """

    # 🔹 Hard-coded project id already inside get_secret_value()
    db_user = get_secret_value("global_db_user")     # your actual secret names
    db_pw   = get_secret_value("global_db_pw")
    db_name = get_secret_value("global_db_name")     # match exactly to secret name

    # 🔹 Hard-coded Cloud SQL connection string: adjust instance name here
    dsn = (
        "host='/cloudsql/vertex-ai-scraper-project:us-east4:zoho-rag' "
        f"dbname='{db_name}' user='{db_user}' password='{db_pw}'"
    )

    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM global LIMIT 1;")  # replace with real table
        row = cur.fetchone()

    return dict(row) if row else {}

# ---------- HTTP wrapper for Cloud Run ----------

def fetch_global_creds_http(request):
    """
    HTTP Cloud Function entry point.
    """
    try:
        creds = fetch_global_creds()
        return json.dumps({"ok": True, "data": creds}), 200, {"Content-Type": "application/json"}
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}), 500, {"Content-Type": "application/json"}