# main.py
# Python 3.11
# Dependencies in requirements.txt: functions-framework, psycopg[binary], beautifulsoup4, lxml

import os
import json
import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urldefrag

import functions_framework
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse, urldefrag

# =========================================================================
#0. Read Configuration from Environment Variables
# =========================================================================
DATABASE_URL = os.environ.get("DATABASE_URL")
CHUNK_SIZE_TOKENS = int(os.environ.get("CHUNK_SIZE_TOKENS", 800))
OVERLAP_FRACTION = float(os.environ.get("OVERLAP_FRACTION", 0.5))
FORCE_REPARSE = os.environ.get("FORCE_REPARSE", "false").lower() in ("true", "1", "t")

@dataclass
class ProcessResult:
    page_id: int
    is_changed: bool
    num_blocks: int
    num_links: int
    num_chunks: int
    elapsed_ms: int

# =======================================================
#1 Define all your helper "specialist" functions first
# =======================================================

def html_to_blocks(html: str, url: str) -> List[Dict[str, Any]]:
    """
    Parses raw HTML into an ordered list of content blocks.
   
    """
    # Strip boilerplate elements first for cleaner processing
    soup = BeautifulSoup(html, 'lxml')
    for selector in ['nav', 'footer', '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]']:
        for element in soup.select(selector):
            element.decompose()

    blocks = []
    ord_counter = 0
    heading_path = [] # Tracks the current "breadcrumb" of headings

    # Find all relevant tags at once to preserve document order
    content_tags = soup.find_all(['h1', 'h2', 'h3', 'p', 'li', 'pre', 'table', 'div'])

    for tag in content_tags:
        block_type = tag.name
        prose_text = None
        code_text = None
        is_code_flag = False

        # Differentiate between code and prose blocks
        if block_type == 'pre':
            is_code_flag = True
            code_text = tag.get_text() # Preserve whitespace
        else:
            prose_text = tag.get_text(" ", strip=True)

        # Skip blocks that are genuinely empty
        if not prose_text and not code_text:
            continue
            
        # Update and maintain the heading path (breadcrumb)
        if block_type in ['h1', 'h2', 'h3']:
            level = int(block_type[1])
            if level == 1:
                heading_path = [prose_text]
            elif level == 2:
                heading_path = heading_path[:1] + [prose_text]
            else: # h3
                heading_path = heading_path[:2] + [prose_text]

        block = {
            "ord": ord_counter,
            "type": block_type,
            "heading_path": list(heading_path),
            "caption": heading_path[-1] if heading_path else None, # Nearest previous heading
            "prose": prose_text,
            "code": code_text,
            "language_hint": None, # Placeholder for now
            "is_code": is_code_flag
        }
        blocks.append(block)
        ord_counter += 1

    return blocks

def extract_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Extracts and normalizes all absolute http/https links from a page.
   
    """
    soup = BeautifulSoup(html, 'lxml')
    links = []
    seen_links = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        
        # Create an absolute URL from a relative one
        absolute_url = urljoin(base_url, href)
        
        # Parse the URL to check its components
        parsed = urlparse(absolute_url)

        # Rule: Keep only http/https links
        if parsed.scheme not in ['http', 'https']:
            continue

        # Rule: Normalize by removing the fragment (#...)
        normalized_url, _ = urldefrag(parsed.geturl())
        
        # Rule: Normalize by removing trailing slashes if the path is not just "/"
        if normalized_url.endswith('/') and urlparse(normalized_url).path != '/':
            normalized_url = normalized_url.rstrip('/')

        # Rule: Deduplicate by the final normalized URL
        if normalized_url in seen_links:
            continue

        seen_links.add(normalized_url)
        
        links.append({
            "href": normalized_url,
            "rel": "href", # Default relation type for now
            "anchor_text": a_tag.get_text(" ", strip=True)
        })

    return links


def _token_estimate(text: str) -> int:
    """A simple and fast approximation of token count by counting words."""
    if not text:
        return 0
    return len(re.findall(r"\S+", text))

def build_chunks(
    blocks: List[Dict[str, Any]],
    chunk_size_tokens: int = 800,
    overlap_fraction: float = 0.5,
) -> List[Dict[str, Any]]:
    """
    Builds overlapping chunks from an ordered list of blocks, ensuring code blocks are not split.
   
    """
    if not blocks:
        return []

    chunks = []
    current_chunk_index = 0
    block_tokens = [_token_estimate(b.get("prose") or b.get("code", "")) for b in blocks]
    num_blocks = len(blocks)
    start_idx = 0

    while start_idx < num_blocks:
        window_tokens = 0
        end_idx = start_idx

        # 1. Grow the window until it reaches the target chunk size
        while end_idx < num_blocks and window_tokens < chunk_size_tokens:
            window_tokens += block_tokens[end_idx]
            end_idx += 1

        # 2. Rule: Do not split a code block. If the next block is code, pull it in.
        if end_idx < num_blocks and blocks[end_idx].get("is_code"):
            end_idx += 1

        window_blocks = blocks[start_idx:end_idx]
        if not window_blocks:
            break

        # 3. Assemble the chunk content from the blocks in the window
        chunk_text_parts = []
        headings_in_chunk = []
        code_in_chunk = []
        is_code_chunk = False
        
        # Find the nearest heading to use as a caption
        caption = window_blocks[0].get("caption")
        for block in reversed(window_blocks):
            if block.get("type") in ['h1', 'h2', 'h3']:
                caption = block.get("prose")
                break
        
        for block in window_blocks:
            if block.get("heading_path"):
                headings_in_chunk.extend(block["heading_path"])
            
            chunk_text_parts.append(block.get("prose") or block.get("code", ""))
            
            if block.get("is_code"):
                is_code_chunk = True
                code_in_chunk.append(block.get("code", ""))

        # 4. Create the final chunk dictionary
        final_chunk_text = "\n\n".join(filter(None, chunk_text_parts))
        final_code_text = "\n\n".join(filter(None, code_in_chunk))
        final_tokens = _token_estimate(final_chunk_text)
        
        dominant_type = "prose-heavy"
        if is_code_chunk and _token_estimate(final_code_text) >= 0.4 * final_tokens:
            dominant_type = "code-heavy" #

        chunk = {
            "chunk_index": current_chunk_index,
            "start_block_ord": window_blocks[0]["ord"],
            "end_block_ord": window_blocks[-1]["ord"],
            "caption": caption,
            "explanation": None, # Placeholder, can be refined
            "code": final_code_text,
            "chunk_text": final_chunk_text,
            "headings": list(dict.fromkeys(headings_in_chunk)), # Unique headings
            "is_code": is_code_chunk,
            "approx_tokens": final_tokens,
            "chunk_size": chunk_size_tokens,
            "chunk_overlap": overlap_fraction,
            "dominant_type": dominant_type,
            "quality_meta": {},
        }
        chunks.append(chunk)
        current_chunk_index += 1

        # 5. Advance the window start index based on the overlap stride
        stride_tokens = int(chunk_size_tokens * (1.0 - overlap_fraction))
        tokens_in_stride = 0
        new_start_idx = start_idx
        
        while new_start_idx < end_idx and tokens_in_stride < stride_tokens:
            tokens_in_stride += block_tokens[new_start_idx]
            new_start_idx += 1
            
        # Ensure we always move forward
        start_idx = new_start_idx if new_start_idx > start_idx else start_idx + 1

    return chunks


def extract_versions(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    From each chunk, detects product/version mentions and computes a sortable score.
    Returns the highest-confidence match per chunk.
   
    """
    # Regex patterns based on the rules in promts.md
    product_re = re.compile(r"\b(creator|deluge|flow|crm(?!\s*api)|analytics)\b", re.IGNORECASE)
    semver_re = re.compile(r"\bv(?:ersion)?\s*(\d+(?:\.\d+){0,2})\b", re.IGNORECASE)
    api_re = re.compile(r"\bapi\s*v?(\d+(?:\.\d+)*)\b", re.IGNORECASE)
    year_month_re = re.compile(r"\b(20\d{2})(?:\.(\d{1,2}))?\b")

    version_hits = []

    for chunk in chunks:
        # Combine text fields to create the search space for this chunk
        text_to_scan = f"{chunk.get('caption', '')} {chunk.get('chunk_text', '')}"
        
        # Find the most likely product mention
        product_match = product_re.search(text_to_scan)
        product = product_match.group(1).lower() if product_match else "unknown"

        best_match = None # Stores the (score, payload) tuple for the best version found

        # Rule: Find semantic versions (e.g., v6, v2.1.3)
        for match in semver_re.finditer(text_to_scan):
            parts = [int(p) for p in match.group(1).split('.')]
            major = parts[0]
            minor = parts[1] if len(parts) > 1 else 0
            patch = parts[2] if len(parts) > 2 else 0
            score = major * 10000 + minor * 100 + patch #
            
            payload = {
                "chunk_index": chunk["chunk_index"], "product": product,
                "version_str": f"v{match.group(1)}", "version_major": major,
                "version_minor": minor, "version_patch": patch,
                "version_score": score, "confidence": 0.9
            }
            if not best_match or score > best_match[0]:
                best_match = (score, payload)

        # Rule: Find API versions (e.g., API v2)
        for match in api_re.finditer(text_to_scan):
            parts = [int(p) for p in match.group(1).split('.') if p.isdigit()]
            if not parts: continue
            major = parts[0]
            score = major * 10000
            
            payload = {
                "chunk_index": chunk["chunk_index"], "product": product,
                "version_str": f"api v{match.group(1)}", "version_major": major,
                "version_minor": 0, "version_patch": 0,
                "version_score": score, "confidence": 0.85
            }
            if not best_match or score > best_match[0]:
                best_match = (score, payload)

        # Rule: Find year/month versions (e.g., 2025.09)
        for match in year_month_re.finditer(text_to_scan):
            year = int(match.group(1))
            month = int(match.group(2)) if match.group(2) else 0
            score = year * 100 + month #
            
            payload = {
                "chunk_index": chunk["chunk_index"], "product": product,
                "version_str": f"{year}.{month:02d}" if month else str(year),
                "version_major": None, "version_minor": None, "version_patch": None,
                "version_year": year, "version_month": month or None,
                "version_score": score, "confidence": 0.8
            }
            if not best_match or score > best_match[0]:
                best_match = (score, payload)

        if best_match:
            version_hits.append(best_match[1])

    return version_hits

    import psycopg

class DatabaseAdapter:
    """
    Provides a clean interface for calling the project's stored procedures.
    Handles database connections and transactions.
   
    """
    def __init__(self, db_url: str):
        self.db_url = db_url
        if not self.db_url:
            raise ValueError("Database URL cannot be empty.")

    def _execute_proc(self, proc_name: str, params: tuple):
        """Helper to connect, execute a procedure, and commit."""
        try:
            with psycopg.connect(self.db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CALL {proc_name}(%s, %s);", params)
                    conn.commit()
        except psycopg.Error as e:
            print(f"Database error calling {proc_name}: {e}")
            raise # Re-raise the exception to be handled by the caller

    def replace_blocks(self, page_id: int, blocks: List[Dict[str, Any]]):
        """Calls the sp_replace_blocks stored procedure."""
        print(f"DB: Replacing {len(blocks)} blocks for page_id {page_id}...")
        self._execute_proc("sp_replace_blocks", (page_id, Jsonb(blocks)))

    def replace_links(self, page_id: int, links: List[Dict[str, Any]]):
        """Calls the sp_replace_page_links stored procedure."""
        print(f"DB: Replacing {len(links)} links for page_id {page_id}...")
        self._execute_proc("sp_replace_page_links", (page_id, Jsonb(links)))

    def replace_chunks(self, page_id: int, chunks: List[Dict[str, Any]]):
        """Calls the sp_replace_chunks stored procedure."""
        print(f"DB: Replacing {len(chunks)} chunks for page_id {page_id}...")
        self._execute_proc("sp_replace_chunks", (page_id, Jsonb(chunks)))
        
    def upsert_chunk_versions(self, page_id: int, versions: List[Dict[str, Any]]):
        """Calls the sp_upsert_chunk_versions stored procedure."""
        if not versions:
            print("DB: No versions to update.")
            return
        print(f"DB: Upserting {len(versions)} versions for page_id {page_id}...")
        self._execute_proc("sp_upsert_chunk_versions", (page_id, Jsonb(versions)))
# (Future helper functions for chunking and versions would also go here)



# =========================================================================
# 2. HTTP Entrypoint for Cloud Function
# =========================================================================
@functions_framework.http
def process_scrape_entrypoint(request):
    """
    Cloud Function entrypoint. Expects a JSON payload with:
    {
        "url": "https://...",
        "html": "<html>...",
        "last_updated": "2025-09-27T20:00:00Z",
        "http_status": 200
    }
    """
    if not DATABASE_URL:
        return ("FATAL: DATABASE_URL environment variable not set.", 500)

    try:
        data = request.get_json()
        url = data["url"]
        html = data["html"]
        http_status = int(data.get("http_status", 200))
        last_updated_str = data.get("last_updated")
        last_updated = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00")) if last_updated_str else None
    except (json.JSONDecodeError, KeyError) as e:
        return (f"Invalid JSON payload: {e}", 400)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            result = save_scraped_data_sql(
                conn=conn,
                url=url,
                html=html,
                last_updated=last_updated,
                http_status=http_status,
                force_reparse=FORCE_REPARSE,
                chunk_size_tokens=CHUNK_SIZE_TOKENS,
                overlap_fraction=OVERLAP_FRACTION
            )
            return (json.dumps(result.__dict__), 200, {'Content-Type': 'application/json'})
    except psycopg.Error as e:
        print(f"Database error for URL {url}: {e}")
        return (f"Database processing error: {e}", 500)
    except Exception as e:
        print(f"Unexpected error for URL {url}: {e}")
        return (f"An unexpected error occurred: {e}", 500)

# =========================================================================
# 3. Core Processing Logic (from ingest_pipeline.py)
# =========================================================================
def save_scraped_data_sql(
    *,
    conn: psycopg.Connection,
    url: str,
    html: str,
    last_updated: Optional[datetime],
    http_status: int,
    force_reparse: bool = False,
    chunk_size_tokens: int = 800,
    overlap_fraction: float = 0.5,
) -> ProcessResult:
    """
    Processes a single page and saves its blocks, links, and chunks to the database.
    """
    t0 = time.time()
    norm_html = _normalize_html_for_hash(html)
    content_hash = hashlib.sha256(norm_html.encode("utf-8")).hexdigest()
    page_title = _extract_title(html)
    crawled_at = datetime.utcnow()
    updated_at = last_updated or crawled_at

    # Upsert page and check if content has changed
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM fn_upsert_page(%(url)s, %(title)s, %(content_hash)s, %(http_status)s, %(crawled_at)s, %(updated_at)s);",
            {"url": url, "title": page_title, "content_hash": content_hash, "http_status": http_status, "crawled_at": crawled_at, "updated_at": updated_at},
        )
        row = cur.fetchone()
        if not row: raise RuntimeError("fn_upsert_page returned no row")
        page_id, is_changed = int(row["page_id"]), bool(row["is_changed"])

    if not is_changed and not force_reparse:
        elapsed_ms = int((time.time() - t0) * 1000)
        return ProcessResult(page_id=page_id, is_changed=False, num_blocks=0, num_links=0, num_chunks=0, elapsed_ms=elapsed_ms)

    # If changed, re-process everything
    blocks = _html_to_blocks(html, base_url=url)
    links = _extract_links(html, base_url=url)
    chunks = _build_chunks(blocks, chunk_size_tokens=chunk_size_tokens, overlap_fraction=overlap_fraction)

    with conn.cursor() as cur:
        cur.execute("CALL sp_replace_blocks(%(page_id)s, %(blocks)s);", {"page_id": page_id, "blocks": Jsonb(blocks)})
        cur.execute("CALL sp_replace_page_links(%(page_id)s, %(links)s);", {"page_id": page_id, "links": Jsonb(links)})
        cur.execute("CALL sp_replace_chunks(%(page_id)s, %(chunks)s);", {"page_id": page_id, "chunks": Jsonb(chunks)})

    elapsed_ms = int((time.time() - t0) * 1000)
    return ProcessResult(page_id=page_id, is_changed=True, num_blocks=len(blocks), num_links=len(links), num_chunks=len(chunks), elapsed_ms=elapsed_ms)

# =========================================================================
# 4. Helper Functions
# =========================================================================
_HEADING_TAGS = {"h1", "h2", "h3"}

def _extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return (soup.title.string or "").strip() if soup.title and soup.title.string else ""

def _normalize_html_for_hash(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for sel in ["script", "style", "nav", "footer", "[role=navigation]"]:
        for el in soup.select(sel):
            el.decompose()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()

def _html_to_blocks(html: str, base_url: str) -> List[Dict[str, Any]]:
    # (Full helper function code from previous version goes here)
    # This is the detailed parser that creates structured blocks.
    soup = BeautifulSoup(html, "lxml")
    for sel in ["[role=navigation]", "nav", "footer"]:
        for el in soup.select(sel):
            el.decompose()

    blocks: List[Dict[str, Any]] = []
    ord_counter = 0
    heading_path: List[str] = []

    for el in soup.find_all(['h1', 'h2', 'h3', 'p', 'li', 'pre', 'table']):
        name = el.name
        text = el.get_text(" ", strip=True)
        
        if name in _HEADING_TAGS:
            depth = int(name[1])
            if depth == 1: heading_path = [text]
            elif depth == 2: heading_path = heading_path[:1] + [text]
            else: heading_path = heading_path[:2] + [text]
        
        if not text and name != 'pre':
            continue

        is_code = name == 'pre'
        block_data = {
            "ord": ord_counter,
            "type": name,
            "heading_path": list(heading_path),
            "caption": heading_path[-1] if heading_path else None,
            "prose": text if not is_code else None,
            "code": el.get_text() if is_code else None,
            "is_code": is_code,
        }
        blocks.append(block_data)
        ord_counter += 1
    return blocks

def _extract_links(html: str, base_url: str) -> List[Dict[str, str]]:
    # (Full helper function code from previous version goes here)
    # This extracts all absolute http/https links from the page.
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict[str, str]] = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href: continue
        href = urljoin(base_url, href)
        href, _frag = urldefrag(href)
        p = urlparse(href)
        if p.scheme not in ("http", "https"): continue
        if href.endswith("/") and p.path != "/": href = href[:-1]
        if href in seen: continue
        seen.add(href)
        out.append({"href": href, "anchor_text": (a.get_text(" ", strip=True) or "")[:1024]})
    return out

def _build_chunks(
    blocks: List[Dict[str, Any]], *,
    chunk_size_tokens: int,
    overlap_fraction: float,
) -> List[Dict[str, Any]]:
    # (Full helper function code from previous version goes here)
    # This is the sophisticated sliding-window chunking logic.
    if not blocks: return []
    
    text_blocks = []
    for b in blocks:
        text = b.get("code") if b["is_code"] else b.get("prose", "")
        if text:
            text_blocks.append(f"## {' > '.join(b['heading_path'])}\n\n{text}")

    # Simplified chunking for this example; you can use your more complex version.
    full_text = "\n\n---\n\n".join(text_blocks)
    # A very basic sliding window chunker
    words = full_text.split()
    chunks = []
    stride = int(chunk_size_tokens * (1 - overlap_fraction))
    
    for i in range(0, len(words), stride):
        chunk_words = words[i:i + chunk_size_tokens]
        if not chunk_words: break
        chunks.append({"chunk_index": len(chunks), "chunk_text": " ".join(chunk_words)})
        if len(chunk_words) < chunk_size_tokens: break # Last chunk
    return chunks