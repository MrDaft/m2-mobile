import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import os

# settings
credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

df = pd.read_html('https://storage.googleapis.com/play_public/supported_devices.html')[0]
df.columns = ['retail_branding', 'marketing_name', 'device_name', 'model']

df.to_gbq(
    destination_table='mobile_app_data.android_device_codes',
    project_id='m2-main',
    if_exists='replace',
    credentials=g_auth_service,
    table_schema=[{'name': 'retail_branding', 'type': 'STRING'},
                  {'name': 'marketing_name', 'type': 'STRING'},
                  {'name': 'device_name', 'type': 'STRING'},
                  {'name': 'model_name', 'type': 'STRING'},
                  ]
)
