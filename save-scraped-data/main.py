import os
from urllib.parse import urlparse
from google.cloud import storage
import functions_framework

storage_client = storage.Client()
SCRAPED_DATA_PREFIX = 'scraped-data/'

@functions_framework.http
def save_scraped_data(request):
    """
    Receives scraped HTML content and saves it to a GCS bucket.
    """
    data_bucket_name = os.environ.get('DATA_BUCKET')
    if not data_bucket_name:
        return ("Error: DATA_BUCKET env var must be set.", 500)

    # Get the JSON payload from the incoming HTTP request.
    request_json = request.get_json(silent=True)
    
    # Verify that the payload is valid and contains the required keys.
    if not request_json or 'url' not in request_json or 'html' not in request_json:
        return ("Error: Invalid request. JSON must contain 'url' and 'html' keys.", 400)

    # Extract the data from the payload.
    url = request_json['url']
    html_content = request_json['html'] 
    
    # Generate a GCS-safe filename from the URL path.
    path = urlparse(url).path.strip('/')
    if not path:
        return ("Warning: Cannot save page with no URL path.", 200)

    filename = path.replace('/', '_') + '.html'
    destination_path = os.path.join(SCRAPED_DATA_PREFIX, filename)

    try:
        # Get the destination bucket and blob object.
        bucket = storage_client.bucket(data_bucket_name)
        blob = bucket.blob(destination_path)

        print(f"Attempting to upload to gs://{data_bucket_name}/{destination_path}")
        print(f"Content length: {len(html_content)} bytes")

        # Upload the HTML content as a string.
        blob.upload_from_string(html_content, content_type='text/html')

        print("Upload command completed without error.")

        # Verification step: reload metadata from GCS to confirm the update.
        blob.reload() 
        updated_time = blob.updated
        print(f"VERIFICATION SUCCEEDED: GCS reports blob was updated at {updated_time}")

    except Exception as e:
        print(f"VERIFICATION FAILED: An error occurred: {e}")
        return (f"Error during save or verification: {e}", 500)

    return (f"Successfully processed {url}", 200)