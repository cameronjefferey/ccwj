import os
import json
import base64
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bigquery_client():
    json_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if json_path and os.path.exists(json_path):
        # Local dev
        credentials = service_account.Credentials.from_service_account_file(json_path)
    else:
        # Render: decode from env variable
        json_data = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64")
        if not json_data:
            raise EnvironmentError("Missing Google credentials")

        creds_dict = json.loads(
            base64.b64decode(json_data.encode()).decode()
        )

        credentials = service_account.Credentials.from_service_account_info(creds_dict)

    return bigquery.Client(credentials=credentials, project=credentials.project_id)

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
