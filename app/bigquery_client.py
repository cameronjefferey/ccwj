import os
from google.cloud import bigquery

def get_bigquery_client():
    # Ensure credentials env var is set
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credential file not found at {credentials_path}")

    return bigquery.Client()

def query_bigquery(client, query_file, start_date=None, end_date=None):
    query_path = os.path.join(os.path.dirname(__file__), "queries", query_file)

    with open(query_path, "r") as file:
        query = file.read()

    parameters = []
    if start_date:
        parameters.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date))
    if end_date:
        parameters.append(bigquery.ScalarQueryParameter("end_date", "STRING", end_date))

    if parameters:
        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        return client.query(query, job_config=job_config).result()
    else:
        return client.query(query).result()
