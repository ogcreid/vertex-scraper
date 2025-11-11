import os
import psycopg
import functions_framework

@functions_framework.http
def reset_pipeline_data(request):
    """
    Connects to the database and calls the sp_reset_pipeline_data procedure.
    """
    # 1. Get the target database name from the request
    dbname = request.args.get('dbname')
    if not dbname:
        return ("Error: A 'dbname' URL query parameter is required.", 400)

    # 2. Get DB connection details from environment variables
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_instance = os.environ.get('DB_INSTANCE')

    if not all([db_user, db_pass, db_instance]):
        return ("Error: DB_USER, DB_PASS, and DB_INSTANCE env vars must be set.", 500)

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