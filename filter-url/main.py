import functions_framework
from urllib.parse import urlparse

def _to_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def _normalize_host(s: str) -> str:
    # strip scheme + www.
    s = s.lower()
    if s.startswith("http://"):
        s = s[7:]
    elif s.startswith("https://"):
        s = s[8:]
    if s.startswith("www."):
        s = s[4:]
    return s

def host_allowed(host: str, bases: list[str]) -> bool:
    """Allow exact host or wildcard base ('*.example.com' or '*example.com')."""
    if not bases:
        return True
    host = _normalize_host(host)
    for base in bases:
        b = _normalize_host(base)
        if b.startswith("*."):
            if host.endswith(b[2:]):
                return True
        elif b.startswith("*"):
            if host.endswith(b[1:]):
                return True
        else:
            if host == b:
                return True
    return False

@functions_framework.http
def filter_http(request):
    """
    POST JSON: { "url": "...", "policy": { 
        "base_urls": [...],
        "require_strings": [...],    # any of these must appear (if provided)
        "exclude_strings": [...],    # any match => block
        "language_excludes": [...]   # raw substrings to block in PATH (e.g. "/fr/", "/es-", "/es-xl/")
    } }
    Returns 'true' or 'false'. Add ?debug=1 for JSON diagnostics.
    """
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    policy = data.get("policy") or {}

    if not url:
        return ("false", 200)

    # Parse & normalize
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()

    # Read policy (plural keys)
    bases    = _to_list(policy.get("base_urls"))
    requires = _to_list(policy.get("require_strings"))
    excludes = _to_list(policy.get("exclude_strings"))
    lang_raw = _to_list(policy.get("language_excludes"))  # treated as RAW substrings in PATH

    # 1) Host scope
    if not host_allowed(host, bases):
        if request.args.get("debug") == "1":
            return ({
                "allowed": False,
                "reason": "host_not_allowed",
                "host": host, "bases": bases, "url": url, "policy": policy
            }, 200)
        return ("false", 200)

    # 2) Exclude strings (anywhere in full URL)
    url_lc = url.lower()
    if excludes and any(x for x in excludes if x and x.lower() in url_lc):
        if request.args.get("debug") == "1":
            return ({
                "allowed": False,
                "reason": "exclude_strings",
                "matched": [x for x in excludes if x and x.lower() in url_lc],
                "url": url, "policy": policy
            }, 200)
        return ("false", 200)

    # 3) Language exclusions (raw substrings in PATH only)
    if lang_raw and any(x for x in lang_raw if x and x.lower() in path):
        if request.args.get("debug") == "1":
            return ({
                "allowed": False,
                "reason": "language_excludes",
                "matched": [x for x in lang_raw if x and x.lower() in path],
                "path": path, "url": url, "policy": policy
            }, 200)
        return ("false", 200)

    # 4) Require strings (must have at least one, if provided)
    if requires and not any(x for x in requires if x and x.lower() in url_lc):
        if request.args.get("debug") == "1":
            return ({
                "allowed": False,
                "reason": "require_strings_missing",
                "requires": requires, "url": url, "policy": policy
            }, 200)
        return ("false", 200)

    if request.args.get("debug") == "1":
        return ({
            "allowed": True,
            "reason": "ok",
            "host": host, "path": path, "url": url, "policy": policy
        }, 200)

    return ("true", 200)