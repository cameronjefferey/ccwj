import os
from google.cloud import bigquery

def get_bigquery_client():
    return bigquery.Client()

def query_bigquery(client, query_file):
    """Executes a query from a given SQL file and returns results."""
    query_path = os.path.join(os.path.dirname(__file__), "queries", query_file)
    
    with open(query_path, "r") as file:
        query = file.read()
    
    return client.query(query).result()
