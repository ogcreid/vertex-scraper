# Docker Setup for Cloud Run

This project contains **12 separate services**, each requiring its own Dockerfile and Cloud Run deployment.

## Services Overview

### Python Services (11 services)
All Python services use `functions-framework` to run on Cloud Run:

1. **fetch-sql-credentials** - Function: `fetch_global_creds_http`
2. **filter-url** - Function: `filter_http`
3. **find-sitemaps** - Function: `find_sitemaps_for_urls_http`
4. **get-sitemap** - Function: `find_sitemaps`
5. **page-publisher** - Function: `page_publisher`
6. **page-scraper-worker** - Function: `page_scraper_worker` (Cloud Event handler)
7. **rescrape-prep** - Function: `rescrape_prep_http`
8. **reset-pipeline-data** - Function: `reset_pipeline_data`
9. **save-scraped-data** - Function: `save_scraped_data`
10. **save-scraped-data-sql** - Function: `process_scrape_entrypoint`
11. **sitemap-orchestrator** - Function: `sitemap_orchestrator`

### Node.js Service (1 service)
12. **vertex-admin** - Express server (`server.js`)

## Building and Deploying

Each service has its own Dockerfile in its directory. To build and deploy a service:

### Example: Building a Python service
```bash
cd page-scraper-worker
gcloud builds submit --tag us-east4-docker.pkg.dev/vertex-ai-scraper-project/cloud-run-source-deploy/page-scraper-worker:latest
```

### Example: Building the Node.js service
```bash
cd vertex-admin
gcloud builds submit --tag us-east4-docker.pkg.dev/vertex-ai-scraper-project/cloud-run-source-deploy/vertex-admin:latest
```

### Example: Deploying to Cloud Run
```bash
gcloud run deploy page-scraper-worker \
  --image us-east4-docker.pkg.dev/vertex-ai-scraper-project/cloud-run-source-deploy/page-scraper-worker:latest \
  --region us-east4 \
  --platform managed \
  --set-env-vars DB_USER=postgres,DB_PASS=your-password,DB_INSTANCE=vertex-ai-scraper-project:us-east4:zoho-rag \
  --add-cloudsql-instances vertex-ai-scraper-project:us-east4:zoho-rag
```

## Important Notes

1. **Each service is independent** - They must be deployed separately to Cloud Run
2. **Environment variables** - Each service may need different environment variables (check `service.yaml` files for reference)
3. **Cloud SQL connection** - Most services connect to Cloud SQL. Use `--add-cloudsql-instances` flag when deploying
4. **Port configuration** - All services listen on port 8080 (Cloud Run sets the PORT env var automatically)
5. **Cloud Event handler** - `page-scraper-worker` uses Cloud Events (Pub/Sub), others use HTTP

## Dockerfile Locations

Each service directory contains its own `Dockerfile`:
- `fetch-sql-credentials/Dockerfile`
- `filter-url/Dockerfile`
- `find-sitemaps/Dockerfile`
- `get-sitemap/Dockerfile`
- `page-publisher/Dockerfile`
- `page-scraper-worker/Dockerfile`
- `rescrape-prep/Dockerfile`
- `reset-pipeline-data/Dockerfile`
- `save-scraped-data/Dockerfile`
- `save-scraped-data-sql/Dockerfile`
- `sitemap-orchestrator/Dockerfile`
- `vertex-admin/Dockerfile`

