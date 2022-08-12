import pandas as pd
import os
from clickhouse_driver import Client
import json
from time import sleep

# settings
try:
    if os.environ.get('CH_HOST') is not None:
        host = os.environ.get('CH_HOST')
        user = os.environ.get('CH_USER')
        password = os.environ.get('CH_PWD')
        database = os.environ.get('CH_DBAP')
        print('environ', host, user, password, database)
    else:
        with open('/home/nikitindd/creds.json') as f:
            cred = json.load(f)
            host = cred['CH_HOST']
            user = cred['CH_USER']
            password = cred['CH_PWD']
            database = cred['CH_DBAP']
            print('creds', host, user, password, database)
except Exception:
    print('no cred')

client = Client(host=host, port='9000', user=user, password=password, database=database, settings={'use_numpy': True})
c = client.execute(
"DROP TABLE IF EXISTS android_device_codes"
)
sleep(2)

client.execute(
"CREATE TABLE IF NOT EXISTS android_device_codes (retail_branding String, marketing_name Nullable(String), device_name Nullable(String), model Nullable(String)) Engine = MergeTree ORDER BY retail_branding"
)
sleep(5)
df = pd.read_html('https://storage.googleapis.com/play_public/supported_devices.html')[0]
df.columns = ['retail_branding', 'marketing_name', 'device_name', 'model']
print(df.head())

client.insert_dataframe("INSERT INTO android_device_codes VALUES", df)

