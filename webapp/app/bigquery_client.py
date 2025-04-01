import os
from google.cloud import bigquery

def get_bigquery_client():
    return bigquery.Client()

def query_bigquery(client, query_file, start_date=None, end_date=None):
    """Executes a query from a given SQL file with optional date filters."""
    query_path = os.path.join(os.path.dirname(__file__), "queries", query_file)
    
    with open(query_path, "r") as file:
        query = file.read()
    
    # Set up query parameters if dates are provided
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date) if start_date else None,
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date) if end_date else None
        ]
    )

    # Remove None values (if start_date or end_date is None)
    job_config.query_parameters = [param for param in job_config.query_parameters if param]

    return client.query(query, job_config=job_config).result()
