#latest version 11/18/25

import os
import psycopg
import requests
import google.oauth2.id_token
import google.auth.transport.requests
import functions_framework

@functions_framework.http
def reset_pipeline_data(request):
    """
    Connects to the database and calls the sp_reset_pipeline_data procedure.
    """
    # 1. Get DB Config from fetch-sql-credentials
    credentials_url = 'https://fetch-sql-credentials-677825641273.us-east4.run.app'
    auth_req = google.auth.transport.requests.Request()
    token = google.oauth2.id_token.fetch_id_token(auth_req, credentials_url)
    response = requests.get(credentials_url, headers={'Authorization': f'Bearer {token}'}, timeout=10)
    creds = response.json()['data']
    
    db_user = creds['user']
    db_pass = creds['password']
    db_instance = creds['db_instance']
    dbname = creds['db_name']

    database_url = f"host='/cloudsql/{db_instance}' dbname='{dbname}' user='{db_user}' password='{db_pass}'"

    # 3. Connect and call the stored procedure
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                print(f"Calling sp_reset_pipeline_data() for database '{dbname}'...")
                cur.execute("CALL sp_reset_pipeline_data();")
            conn.commit() # Commit the transaction to make the changes permanent
        
        message = f"Successfully reset all transactional data for database '{dbname}'."
        print(message)
        return (message, 200)

    except psycopg.Error as e:
        error_message = f"Failed to reset data for database '{dbname}'. Error: {e}"
        print(error_message)
        return (error_message, 500)
