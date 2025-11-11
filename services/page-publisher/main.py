import os
import time
import json
import psycopg
from google.cloud import pubsub_v1
import functions_framework

# --- Configuration ---
BATCH_SIZE = 100
MAIN_LOOP_SLEEP_SECONDS = 60
QUIESCENCE_CHECKS = 3
QUIESCENCE_SLEEP_SECONDS = 15

@functions_framework.http
def page_publisher(request):
    # 1. Get Input and DB Config
    dbname = request.args.get('dbname')
    if not dbname:
        try:
            request_json = request.get_json(silent=True)
            if request_json: dbname = request_json.get('dbname')
        except Exception: pass
    if not dbname: return ("Error: 'dbname' must be provided.", 400)
    
    print(f"[Publisher] Starting for db: '{dbname}'")

    # Get Pub/Sub Topic from environment
    project_id = os.environ.get('PROJECT_ID')
    topic_id = os.environ.get('PUBSUB_TOPIC_ID')
    if not project_id or not topic_id:
        return ("Error: PROJECT_ID and PUBSUB_TOPIC_ID env vars must be set.", 500)
    topic_path = pubsub_v1.PublisherClient().topic_path(project_id, topic_id)

    db_user, db_pass, db_instance = (os.environ.get(k) for k in ('DB_USER', 'DB_PASS', 'DB_INSTANCE'))
    if not all([db_user, db_pass, db_instance]): return ("Error: DB env vars must be set.", 500)
    database_url = f"host='/cloudsql/{db_instance}' dbname='{dbname}' user='{db_user}' password='{db_pass}'"

    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT run_guid FROM pipeline_state ORDER BY created_at DESC LIMIT 1;")
                run_guid = str(cur.fetchone()[0])
                print(f"[Publisher] Found active run_guid: {run_guid}")
    except (psycopg.Error, TypeError) as e:
        return (f"Database setup failed: {e}", 500)

    # --- Main Publishing Loop ---
    publisher = pubsub_v1.PublisherClient()
    while True:
        print(f"[Publisher] --- Starting new publishing cycle for run {run_guid} ---")
        try:
            with psycopg.connect(database_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, url, check_hash, contextual_patterns FROM urls_to_process WHERE run_guid = %s AND status = 'pending' LIMIT %s;",
                        (run_guid, BATCH_SIZE)
                    )
                    work_batch = cur.fetchall()

                    if not work_batch:
                        print(f"[Publisher] No pending URLs found for run {run_guid}. Checking for active workers...")
                        
                        is_work_truly_done = False
                        active_count = -1 
                        for i in range(QUIESCENCE_CHECKS):
                            cur.execute(
                                "SELECT COUNT(*) FROM urls_to_process WHERE run_guid = %s AND status IN ('published', 'processing');",
                                (run_guid,)
                            )
                            active_count = cur.fetchone()[0]
                            print(f"[Publisher]   - Quiescence Check {i+1}/{QUIESCENCE_CHECKS}: Found {active_count} active job(s).")
                            if active_count > 0:
                                break 
                            if i < QUIESCENCE_CHECKS - 1:
                                time.sleep(QUIESCENCE_SLEEP_SECONDS)
                        
                        if active_count == 0:
                            is_work_truly_done = True

                        if is_work_truly_done:
                            print(f"[Publisher] No active work found for run {run_guid} after 3 checks. Run is complete.")
                            cur.execute("UPDATE pipeline_state SET status = 'complete' WHERE run_guid = %s;", (run_guid,))
                            break 
                        else:
                            print(f"[Publisher] Work is still active for run {run_guid}. Waiting for {MAIN_LOOP_SLEEP_SECONDS} seconds...")
                            time.sleep(MAIN_LOOP_SLEEP_SECONDS)
                            continue

                    if work_batch:
                        batch_ids = [row[0] for row in work_batch]
                        placeholders = ', '.join(['%s'] * len(batch_ids))
                        query = f"UPDATE urls_to_process SET status = 'published' WHERE id IN ({placeholders});"
                        cur.execute(query, batch_ids)
                        
                        for url_id, url, check_hash, patterns in work_batch:
                            message_data = {
                                "url_id": url_id, "url": url, "run_guid": run_guid,
                                "dbname": dbname, "check_hash": check_hash,
                                "contextual_patterns": patterns
                            }
                            publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
                        
                        print(f"[Publisher] Published {len(work_batch)} URLs to topic for run {run_guid}.")

        except psycopg.Error as e:
            print(f"[Publisher] Database error in main loop for run {run_guid}: {e}")
            print(f"[Publisher] Exiting due to database error.")
            break # Exit the while loop
    
    return(f"Publisher for run '{run_guid}' has completed.", 200)