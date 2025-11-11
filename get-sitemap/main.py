# main.py
from flask import Request, jsonify
from discovery import find_root_sitemaps

def find_sitemaps(request: Request):
    """
    HTTP Cloud Function entry point. 
    Use --entry-point find_sitemaps when deploying.
    GET /?site=https://example.com
    Optional:
      include_subdomains=true|false (default false)
      max_index_fetches=<int>       (default 200)
      delay=<float_seconds>         (default 0.03)
    """
    args = request.args or {}
    site = args.get("site")
    if not site:
        return jsonify({"error": "site parameter required"}), 400

    include_subdomains = (args.get("include_subdomains", "false").lower() == "true")
    try:
        max_index_fetches = int(args.get("max_index_fetches", 200))
    except ValueError:
        return jsonify({"error": "max_index_fetches must be an integer"}), 400
    try:
        delay = float(args.get("delay", 0.03))
    except ValueError:
        return jsonify({"error": "delay must be a float"}), 400

    result = find_root_sitemaps(
        site=site,
        include_subdomains=include_subdomains,
        max_index_fetches=max_index_fetches,
        polite_delay=delay,
    )
    return jsonify(result)