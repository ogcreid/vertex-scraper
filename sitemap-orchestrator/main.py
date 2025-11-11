import os
import uuid
import psycopg
import google.oauth2.id_token
import google.auth.transport.requests
import requests
import functions_framework

@functions_framework.http
def sitemap_orchestrator(request):
    # 1. Get Environment Variables
    queue_preparer_url = os.environ.get('QUEUE_PREPARER_URL')
    scraper_url = os.environ.get('RECURSIVE_SCRAPER_URL')
    db_user, db_pass, db_instance = (os.environ.get(k) for k in ('DB_USER', 'DB_PASS', 'DB_INSTANCE'))

    if not all([queue_preparer_url, scraper_url, db_user, db_pass, db_instance]):
        return ("Error: All required environment variables must be set.", 500)

    # 2. Get dbname from request
    dbname = request.args.get('dbname')
    if not dbname:
        try:
            request_json = request.get_json(silent=True)
            if request_json: dbname = request_json.get('dbname')
        except Exception: pass
    if not dbname:
        return ("Error: A 'dbname' must be provided.", 400)

    # 3. Generate run_guid and create the initial pipeline state record
    run_guid = str(uuid.uuid4())
    database_url = f"host='/cloudsql/{db_instance}' dbname='{dbname}' user='{db_user}' password='{db_pass}'"
    
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline_state (run_guid, status) VALUES (%s, %s);",
                    (run_guid, 'starting')
                )
            conn.commit()  # <-- This line saves the new record
    except psycopg.Error as e:
        return (f"Failed to create initial pipeline_state record. Error: {e}", 500)
    
    # --- 4. Execute Pipeline Steps ---
    auth_req = google.auth.transport.requests.Request()
    print(f"--- STARTING PIPELINE RUN --- GUID: {run_guid} --- DB: {dbname} ---")

    # Step 1: Prepare the scrape queue
    print(f"\n--- Step 1: Invoking queue preparer function ---")
    try:
        token = google.oauth2.id_token.fetch_id_token(auth_req, queue_preparer_url)
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.post(queue_preparer_url, headers=headers, json={'dbname': dbname}, timeout=900)
        response.raise_for_status()
        print(f"Queue preparer finished successfully: {response.text}")
    except Exception as e:
        return (f"Failed to invoke queue preparer. Details: {str(e)}", 500)
    
    # Step 2: Start the scrapers
    print(f"\n--- Step 2: Invoking recursive scraper function ---")
    try:
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_url)
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.post(scraper_url, headers=headers, json={'dbname': dbname}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.ReadTimeout:
        pass # This is expected
    except Exception as e:
        return (f"Failed to invoke scraper. Details: {str(e)}", 500)

    print("Orchestration complete. Scraper is running.")
    return (f"Orchestration for run '{run_guid}' is complete.", 200)