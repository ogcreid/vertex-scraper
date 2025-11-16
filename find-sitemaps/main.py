# find_sitemaps_for_urls.py

import time
import requests
import psycopg
import google.oauth2.id_token
import google.auth.transport.requests
from psycopg.rows import dict_row
from typing import Any, Dict, List

# ----------------------------
# SKELETON: batch discover + save
# ----------------------------
def find_sitemaps_for_urls_http(request):
    """
    HTTP entry point.
    1. Refresh sitemap policies.
    2. Load all url_rules of type 'base_url'.
    3. For each rule:
        - Derive actual base URL from pattern (strip * etc.)
        - Load JSONB policy from sitemap_sources
        - Call discovery routine (policy-driven)
        - Insert roots into site_maps_for_url
    4. Return summary JSON
    """
    t0 = time.time()

    # --- Connect to DB ---
    try:
        credentials_url = 'https://fetch-sql-credentials-677825641273.us-east4.run.app'
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, credentials_url)
        response = requests.get(credentials_url, headers={'Authorization': f'Bearer {token}'}, timeout=10)
        
        # Debug logging
        if response.status_code != 200:
            return {"ok": False, "error": f"fetch-sql-credentials returned {response.status_code}: {response.text}"}, 500
        
        response_data = response.json()
        if 'data' not in response_data:
            return {"ok": False, "error": f"fetch-sql-credentials missing 'data' key: {response_data}"}, 500
        
        creds = response_data['data']
        dsn = f"host='/cloudsql/{creds['db_instance']}' dbname='{creds['db_name']}' user='{creds['user']}' password='{creds['password']}'"
        conn = psycopg.connect(dsn, row_factory=dict_row)
    except Exception as e:
        return {"ok": False, "error": f"DB connect: {e}"}, 500

    processed_details: List[Dict[str, Any]] = []
    saved_total = 0

    try:
        with conn, conn.cursor() as cur:
            # (1) refresh policies
            cur.execute("CALL sp_refresh_all_sitemap_policies();")

            # (2) load base_url rules
            cur.execute("""
                SELECT r.id as base_url_id,
                       r.pattern,
                       r.sitemap_source_id,
                       s.policy
                FROM url_rules r
                JOIN sitemap_sources s
                  ON s.id = r.sitemap_source_id
                WHERE r.type = 'base_url';
            """)
            rules = cur.fetchall()

        # (3) iterate rules
        for rule in rules:
            base_url_id = rule["base_url_id"]
            pattern = rule["pattern"]
            sitemap_source_id = rule["sitemap_source_id"]
            policy = rule.get("policy") or {}

            # strip "*" from pattern → actual base URL
            actual_url = pattern.replace("*", "").strip()

            # call discovery (placeholder)
            # TODO: import your discovery function here
            # from find_sitemaps_policy import discover_roots_policy_driven
            # result = discover_roots_policy_driven(actual_url, policy=policy, ...)
            result = {"roots": {"indexes": [], "urlsets": [], "text": []}, "stats": {}}

            # insert into site_maps_for_url (placeholder)
            saved_count = 0
            with conn, conn.cursor() as cur:
                for idx in result["roots"]["indexes"]:
                    cur.execute("""
                        INSERT INTO site_maps_for_url (base_url_id, sitemap_source_id, url, type, created_at)
                        VALUES (%s,%s,%s,'index',now())
                        ON CONFLICT DO NOTHING;
                    """, (base_url_id, sitemap_source_id, idx))
                    saved_count += cur.rowcount
                for us in result["roots"]["urlsets"]:
                    cur.execute("""
                        INSERT INTO site_maps_for_url (base_url_id, sitemap_source_id, url, type, created_at)
                        VALUES (%s,%s,%s,'urlset',now())
                        ON CONFLICT DO NOTHING;
                    """, (base_url_id, sitemap_source_id, us))
                    saved_count += cur.rowcount
                for tx in result["roots"]["text"]:
                    cur.execute("""
                        INSERT INTO site_maps_for_url (base_url_id, sitemap_source_id, url, type, created_at)
                        VALUES (%s,%s,%s,'text',now())
                        ON CONFLICT DO NOTHING;
                    """, (base_url_id, sitemap_source_id, tx))
                    saved_count += cur.rowcount

            saved_total += saved_count
            processed_details.append({
                "base_url_id": base_url_id,
                "pattern": pattern,
                "sitemap_source_id": sitemap_source_id,
                "saved": saved_count,
                "roots_index_count": len(result["roots"]["indexes"]),
                "stats": result["stats"]
            })

    except Exception as e:
        return {"ok": False, "error": f"processing: {e}"}, 500
    finally:
        conn.close()

    elapsed = round(time.time() - t0, 3)
    return {
        "ok": True,
        "processed": len(processed_details),
        "saved": saved_total,
        "details": processed_details,
        "elapsed_sec": elapsed
    }, 200