import os
import json
import base64
from google.cloud import bigquery
from google.oauth2 import service_account


def get_bigquery_client():
    """Create a BigQuery client using the best available credentials.

    Credential resolution order:
      1. GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64  (Render / CI)
      2. GOOGLE_APPLICATION_CREDENTIALS file path    (explicit service-account)
      3. Application Default Credentials             (gcloud auth / GCE / Cloud Run)
    """

    # 1. Render / CI: base64-encoded service-account JSON
    b64_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64")
    if b64_creds:
        creds_dict = json.loads(base64.b64decode(b64_creds).decode())
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 2. Explicit service-account key file
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        credentials = service_account.Credentials.from_service_account_file(sa_path)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 3. Application Default Credentials (gcloud auth application-default login)
    return bigquery.Client(project="ccwj-dbt")
