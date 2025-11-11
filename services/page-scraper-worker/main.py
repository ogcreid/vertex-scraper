import os
import uuid
import psycopg
import requests
import json
import base64
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone
import functions_framework
import hashlib

# --- Helper Functions ---
def extract_metadata(soup):
    """Extracts title and last modified timestamp from HTML soup."""
    title = (soup.title.string or "").strip() if soup.title else ""
    modified_time_tag = soup.find('meta', property='article:modified_time')
    if modified_time_tag and modified_time_tag.get('content'):
        try:
            return title, datetime.fromisoformat(modified_time_tag['content'].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass  # Ignore malformed dates
    return title, None

def get_base_domain(url: str):
    """Gets the base domain (e.g., zoho.com) from a URL."""
    hostname = urlparse(url).hostname
    if not hostname: return ""
    parts = hostname.split('.')
    return ".".join(parts[-2:]) if len(parts) > 2 else hostname

# --- Main Cloud Function ---
@functions_framework.cloud_event
def page_scraper_worker(cloud_event):
    # 1. Get Job Details from Pub/Sub Message
    try:
        payload_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        job_data = json.loads(payload_str)
        url_id = job_data['url_id']
        url = job_data['url']
        run_guid = job_data['run_guid']
        dbname = job_data['dbname']
        check_hash = job_data['check_hash']
        patterns_str = job_data['contextual_patterns']
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error: Could not decode Pub/Sub message. Malformed payload. Error: {e}")
        return # Acknowledge message to prevent retries on bad payloads

    worker_id = str(uuid.uuid4())
    print(f"Worker {worker_id} started for URL ID {url_id}: {url}")

    # 2. Get DB Config
    db_user, db_pass, db_instance = (os.environ.get(k) for k in ('DB_USER', 'DB_PASS', 'DB_INSTANCE'))
    if not all([db_user, db_pass, db_instance]):
        print("Error: DB env vars must be set.")
        raise RuntimeError("Missing DB environment variables")
    database_url = f"host='/cloudsql/{db_instance}' dbname='{dbname}' user='{db_user}' password='{db_pass}'"

    # 3. Immediately mark job as 'processing'
    try:
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE urls_to_process SET status = 'processing', worker_id = %s WHERE id = %s;", (worker_id, url_id))
    except psycopg.Error as e:
        print(f"Error marking job as processing: {e}")
        raise e # Let Pub/Sub retry if we can't connect to DB

    # 4. Main Scrape Logic
    try:
        # Download
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        html_content = response.text
        new_hash = hashlib.sha256(html_content.encode()).hexdigest()
        
        # Conditional Save
        should_save = True
        if check_hash:
            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT content_hash FROM pages WHERE url = %s;", (url,))
                    old_hash_row = cur.fetchone()
                    if old_hash_row and old_hash_row[0] == new_hash:
                        should_save = False
                        print(f"  - Skipping save for unchanged pages: {url}")
        
        if should_save:
            soup = BeautifulSoup(html_content, 'lxml')
            title, updated_at_from_meta = extract_metadata(soup)
            with psycopg.connect(database_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM fn_upsert_page(%s, %s, %s, %s, %s, %s, %s, %s);",
                        (url, title, None, new_hash, response.status_code, datetime.now(timezone.utc), updated_at_from_meta, html_content)
                    )
        
        # Discover & Queue New Links
        base_domain = get_base_domain(url)
        soup_for_links = BeautifulSoup(html_content, 'lxml')
        new_links = []
        contextual_patterns = [p.strip() for p in patterns_str.splitlines() if p.strip()] if patterns_str else []
        lang_exclusions = set()
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config_value FROM app_config WHERE config_key = 'LANGUAGE_EXCLUSIONS_LIST';")
                lang_exclusions = {line.strip() for line in cur.fetchone()[0].splitlines() if line.strip()}

        for link in soup_for_links.find_all('a', href=True):
            abs_link = urljoin(url, link['href']).split('#')[0]
            if get_base_domain(abs_link) != base_domain: continue
            path = urlparse(abs_link).path.lower()
            if any(f"/{lang}/" in path or f"/{lang}-" in path for lang in lang_exclusions): continue
            if contextual_patterns and not any(p in abs_link for p in contextual_patterns): continue
            new_links.append((run_guid, abs_link, 'organic', True, patterns_str)) # New organic links should always be hash-checked
        
        if new_links:
            with psycopg.connect(database_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.executemany("INSERT INTO urls_to_process (run_guid, url, source, check_hash, contextual_patterns) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (run_guid, url) DO NOTHING;", new_links)

        # 5. Mark as 'complete' on success
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE urls_to_process SET status = 'complete', processed_at = %s WHERE id = %s;", (datetime.now(timezone.utc), url_id))
        print(f"Worker {worker_id} completed successfully for URL ID {url_id}.")

    except Exception as e:
        # 6. Mark as 'failed' on error
        error_message = str(e)
        print(f"Worker {worker_id} failed for URL ID {url_id}. Error: {error_message}")
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE urls_to_process SET status = 'failed', processed_at = %s, error_message = %s WHERE id = %s;", (datetime.now(timezone.utc), error_message, url_id))