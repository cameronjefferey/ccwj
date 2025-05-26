import os
from google.cloud import bigquery

def get_bigquery_client():
    return bigquery.Client()

def query_bigquery(client, query_file, start_date=None, end_date=None):
    """Executes a query from a given SQL file with optional date filters."""
    # Build the path to the SQL file
    query_path = os.path.join(os.path.dirname(__file__), "queries", query_file)

    # Read the SQL query from the file
    with open(query_path, "r") as file:
        query = file.read()

    # Initialize query parameters if provided
    parameters = []
    if start_date:
        parameters.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date))
    if end_date:
        parameters.append(bigquery.ScalarQueryParameter("end_date", "STRING", end_date))

    # Only include job_config if we have parameters
    if parameters:
        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        return client.query(query, job_config=job_config).result()
    else:
        return client.query(query).result()
