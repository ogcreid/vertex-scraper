import os
import json

import time
import requests
import psycopg
import xml.etree.ElementTree as ET
import google.oauth2.id_token
import google.auth.transport.requests
import functions_framework
from datetime import datetime
from typing import List, Tuple, Optional

XML_NS = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

# ---------- Config (kept) ----------
LIMIT_SOURCES = int(os.environ.get("LIMIT_SOURCES", "2"))
LIMIT_SUBSITEMAPS_PER_SOURCE = int(os.environ.get("LIMIT_SUBSITEMAPS_PER_SOURCE", "5"))
LIMIT_PAGES_PER_SUBSITEMAP   = int(os.environ.get("LIMIT_PAGES_PER_SUBSITEMAP", "200"))
TIME_BUDGET_SEC              = int(os.environ.get("TIME_BUDGET_SEC", "240"))

# ---------- Helpers ----------
def _build_db_dsn():
    credentials_url = 'https://fetch-sql-credentials-677825641273.us-east4.run.app'
    auth_req = google.auth.transport.requests.Request()
    token = google.oauth2.id_token.fetch_id_token(auth_req, credentials_url)
    response = requests.get(credentials_url, headers={'Authorization': f'Bearer {token}'}, timeout=10)
    creds = response.json()['data']
    dsn = f"host='/cloudsql/{creds['db_instance']}' dbname='{creds['db_name']}' user='{creds['user']}' password='{creds['password']}'"
    return dsn, creds['db_name']

def _ok(x) -> bool:
    return x is not None and str(x).strip() != ""

def _parse_iso8601(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str: return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

def parse_sitemap_index(xml_bytes: bytes) -> List[str]:
    out = []
    try:
        root = ET.fromstring(xml_bytes)
        for sm in root.findall('ns:sitemap', XML_NS):
            loc = sm.find('ns:loc', XML_NS)
            if loc is not None and _ok(loc.text):
                out.append(loc.text.strip())
    except ET.ParseError as e:
        print(f"[parse_sitemap_index] XML parse error: {e}")
    return out

def parse_urlset(xml_bytes: bytes) -> List[Tuple[str, Optional[datetime]]]:
    pages = []
    try:
        root = ET.fromstring(xml_bytes)
        for u in root.findall('ns:url', XML_NS):
            loc = u.find('ns:loc', XML_NS)
            lm  = u.find('ns:lastmod', XML_NS)
            if loc is not None and _ok(loc.text):
                lastmod = _parse_iso8601(lm.text.strip()) if (lm is not None and _ok(lm.text)) else None
                pages.append((loc.text.strip(), lastmod))
    except ET.ParseError as e:
        print(f"[parse_urlset] XML parse error: {e}")
    return pages

def call_filter_service(sess: requests.Session, endpoint: str, url: str, policy: dict) -> bool:
    """Return True/False by calling the filter service with the (already-flat) policy."""
    try:
        r = sess.post(endpoint, json={"url": url, "policy": policy or {}}, timeout=10)
        if r.status_code == 200:
            return r.text.strip().lower() == "true"
        print(f"[filter-url] non-200 {r.status_code} for {url}, body={r.text[:200]}")
    except requests.exceptions.RequestException as e:
        print(f"[filter-url] request error for {url}: {e}")
    return False

# ---------- Main ----------
@functions_framework.http
def rescrape_prep_http(request):
    t0 = time.time()
    print("rescrape_prep: start (stage + follow-ons 1–3, filter submaps before cap)")

    # Env / endpoints
    try:
        dsn, db_name = _build_db_dsn()
    except Exception as e:
        return (json.dumps({"ok": False, "error": f"DB env missing: {e}"}),
                500, {"Content-Type": "application/json"})
    filter_ep = os.environ.get("FILTER_URL_ENDPOINT")
    if not filter_ep:
        return (json.dumps({"ok": False, "error": "FILTER_URL_ENDPOINT not set"}),
                500, {"Content-Type": "application/json"})

    # Refresh policy + load sources
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CALL sp_refresh_all_sitemap_policies();")
            cur.execute("""
                SELECT id, index_url, policy, discovery_mode
                FROM sitemap_sources
                WHERE is_active = true
                ORDER BY priority DESC, id ASC;
            """)
            sources = cur.fetchall()
            print(f"DB: active sitemap_sources = {len(sources)}")
    except Exception as e:
        return (json.dumps({"ok": False, "error": f"bootstrap: {e}"}),
                500, {"Content-Type": "application/json"})

    sess = requests.Session()
    candidates = []  # (url, lastmod, sitemap_id, source)
    debug = {"notes": [f"sources={len(sources)}"], "sources": []}

    # Crawl top-level only (index → leaf urlsets); worker will find organics later
    for si, (sitemap_id, root_index_url, policy, discovery_mode) in enumerate(sources[:LIMIT_SOURCES], start=1):
        if time.time() - t0 > TIME_BUDGET_SEC:
            print("TIME_BUDGET hit (sources loop)")
            break

        src_dbg = {"sitemap_id": sitemap_id, "index_url": root_index_url, "discovery_mode": discovery_mode}
        debug["sources"].append(src_dbg)

        # Handle seed mode: skip sitemap parsing, just queue the seed URL directly
        if discovery_mode == 'seed':
            print(f"SRC[{si}] SEED mode: {root_index_url}")
            if call_filter_service(sess, filter_ep, root_index_url, policy):
                candidates.append((root_index_url, None, sitemap_id, 'seed'))
                src_dbg["seed_accepted"] = True
                print(f"  seed URL accepted: {root_index_url}")
            else:
                src_dbg["seed_accepted"] = False
                print(f"  seed URL filtered out: {root_index_url}")
            continue  # Move to next source

        # Sitemap mode: fetch the starting URL; DO NOT filter this starting URL
        try:
            r = sess.get(root_index_url, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            # If the “index_url” is actually a leaf page (not XML), treat as user-intended organic
            print(f"Index fetch failed; treat as leaf page: {root_index_url} — {e}")
            candidates.append((root_index_url, None, sitemap_id, 'organic'))
            src_dbg["error"] = str(e)
            src_dbg["treated_as"] = "organic_leaf"
            continue

        # Is it an index or a leaf urlset?
        submaps_all = parse_sitemap_index(r.content)
        if submaps_all:
            # ----------------------------------------------
            # NEW: filter sub-sitemaps first, then cap
            # ----------------------------------------------
            filtered_submaps, skipped_submaps = [], []
            for sm in submaps_all:
                if call_filter_service(sess, filter_ep, sm, policy):
                    filtered_submaps.append(sm)
                else:
                    skipped_submaps.append(sm)

            src_dbg["submaps_total"]   = len(submaps_all)
            src_dbg["submaps_kept"]    = filtered_submaps[:LIMIT_SUBSITEMAPS_PER_SOURCE]
            src_dbg["submaps_skipped"] = skipped_submaps

            print(f"SRC[{si}] submaps={len(submaps_all)} → "
                  f"filtered={len(filtered_submaps)} (cap {LIMIT_SUBSITEMAPS_PER_SOURCE})")

            to_visit = filtered_submaps[:LIMIT_SUBSITEMAPS_PER_SOURCE]

            for sm_i, sm_url in enumerate(to_visit, start=1):
                if time.time() - t0 > TIME_BUDGET_SEC:
                    print("TIME_BUDGET hit (submaps loop)")
                    break
                try:
                    s = sess.get(sm_url, timeout=30)
                    s.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"  submap fetch error: {e}")
                    continue

                pages = parse_urlset(s.content)
                if not pages:
                    # nested index – we stop recursion by design for now
                    continue

                accepted = 0
                leaf_sample = []
                for (u, lm) in pages[:LIMIT_PAGES_PER_SUBSITEMAP]:
                    # apply the same policy to each leaf URL
                    if call_filter_service(sess, filter_ep, u, policy):
                        candidates.append((u, lm, sitemap_id, 'sitemap'))
                        accepted += 1
                        if len(leaf_sample) < 10:
                            leaf_sample.append(u)
                print(f"  submap accepted={accepted}/{len(pages)}")
                # optional per-submap sample (first 10 accepted)
                src_dbg.setdefault("leaf_samples", []).append(
                    {"submap": sm_url, "accepted_count": accepted, "accepted_sample": leaf_sample}
                )

        else:
            # Not an index: try parse as leaf urlset directly
            leaf_pages = parse_urlset(r.content)
            if leaf_pages:
                accepted = 0
                sample = []
                for (u, lm) in leaf_pages[:LIMIT_PAGES_PER_SUBSITEMAP]:
                    if call_filter_service(sess, filter_ep, u, policy):
                        candidates.append((u, lm, sitemap_id, 'sitemap'))
                        accepted += 1
                        if len(sample) < 10:
                            sample.append(u)
                print(f"SRC[{si}] direct-leaf accepted={accepted}/{len(leaf_pages)}")
                src_dbg["direct_leaf_accepted"] = accepted
                src_dbg["direct_leaf_sample"]   = sample
            else:
                # Malformed XML → treat starting URL as organic (user intent)
                candidates.append((root_index_url, None, sitemap_id, 'organic'))
                src_dbg["treated_as"] = "organic_leaf_no_xml"

    # ---------- Stage into urls_candidate_load ----------
    staged = 0
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE urls_candidate_load;")
            if candidates:
                cur.executemany("""
                    INSERT INTO urls_candidate_load (url, lastmod, sitemap_id, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                """, candidates)
                cur.execute("SELECT COUNT(*) FROM urls_candidate_load;")
                staged = cur.fetchone()[0]
            print(f"staged rows in urls_candidate_load = {staged}")
    except Exception as e:
        return (json.dumps({"ok": False, "error": f"staging: {e}"}),
                500, {"Content-Type": "application/json"})

    # ===============================
    # FOLLOW-ONS (1) (2) (3)
    # ===============================
    inserted_new = 0
    flagged_existing = 0

    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            # (1) Clear prior flags (only where TRUE)
            cur.execute("""
                UPDATE public.pages
                   SET touched_this_run = NULL
                 WHERE touched_this_run IS TRUE;
            """)
            print(f"(1) cleared touched_this_run (TRUE→NULL): {cur.rowcount or 0}")

            # (2a) INSERT new pages from candidates
            cur.execute("""
                INSERT INTO public.pages
                    (url, source, sitemap_source_id, needs_update, touched_this_run, created_at, updated_at)
                SELECT
                    c.url,
                    COALESCE(c.source, 'sitemap') AS source,
                    c.sitemap_id,
                    TRUE,    -- new pages must be scraped
                    TRUE,    -- touched in this run
                    now(),
                    COALESCE(c.lastmod, now())
                FROM public.urls_candidate_load c
                LEFT JOIN public.pages p ON p.url = c.url
                WHERE p.url IS NULL;
            """)
            inserted_new = cur.rowcount or 0
            print(f"(2a) inserted new pages: {inserted_new}")

            # (2b) UPDATE existing pages needing refresh
            cur.execute("""
                UPDATE public.pages p
                   SET needs_update     = TRUE,
                       touched_this_run = TRUE
                  FROM public.urls_candidate_load c
                 WHERE c.url = p.url
                   AND (
                        p.updated_at IS NULL
                        OR (c.lastmod IS NOT NULL AND c.lastmod > p.updated_at)
                       );
            """)
            flagged_existing = cur.rowcount or 0
            print(f"(2b) flagged existing pages needs_update: {flagged_existing}")

    except Exception as e:
        return (json.dumps({"ok": False, "error": f"follow_on: {e}"}),
                500, {"Content-Type": "application/json"})

    elapsed = round(time.time() - t0, 3)
    out = {
        "ok": True,
        "db": db_name,
        "sources_used": min(len(sources), LIMIT_SOURCES),
        "staged": staged,
        "inserted_new_pages": inserted_new,
        "flagged_existing_pages": flagged_existing,
        "elapsed_sec": elapsed,
        "limits": {
            "sources": LIMIT_SOURCES,
            "sub_sitemaps_per_source": LIMIT_SUBSITEMAPS_PER_SOURCE,
            "pages_per_sub_sitemap": LIMIT_PAGES_PER_SUBSITEMAP,
            "time_budget_sec": TIME_BUDGET_SEC
        },
        "debug": debug
    }
    print(f"rescrape_prep: done -> {out}")
    return (json.dumps(out), 200, {"Content-Type": "application/json"})