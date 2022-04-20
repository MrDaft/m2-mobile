import requests
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from google_play_scraper import app
import os

# pandas settings
pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal

# settings
# credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
credentials = 'm2-main-cd9ed0b4e222.json'
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

android_app_id = {'ru.m2.squaremeter': 'Метр Квадратный',
                  'ru.cian.main': 'Циан',
                  'ru.domclick.mortgage': 'ДомКлик',
                  'com.avito.android': 'Авито'}

ios_app_id = {'1501340734': 'Метр Квадратный',
              '911804296': 'Циан',
              '1143031400': 'ДомКлик',
              '417281773': 'Авито'}

country = 'RU'
os = []
ratings = []
app_name = []
rating_count = []
today = datetime.today().strftime('%Y-%m-%d')

# appstore data
for key, value in ios_app_id.items():
    url = f'http://itunes.apple.com/lookup?id={key}&country={country}'
    r = requests.get(url)
    req = r.json()
    rating = req['results']
    os.append('IOS')
    rating_count.append(rating[0]['userRatingCount'])
    app_name.append(value)
    ratings.append(rating[0]['averageUserRating'])

# googleplay data
for key, value in android_app_id.items():
    print(f'key')
    result = app(key, country=country)
    os.append('ANDROID')
    app_name.append(value)
    rating_count.append(result['ratings'])
    ratings.append(result['score'])

df = pd.DataFrame()
df['os'] = os
df['app'] = app_name
df['rating'] = ratings
df['rating_count'] = rating_count
df['date'] = today

df.to_gbq(
    destination_table='mobile_app_data.rating',
    project_id='m2-main',
    if_exists='append',
    credentials=g_auth_service,
    table_schema=[{'name': 'os', 'type': 'STRING'},
                  {'name': 'app', 'type': 'STRING'},
                  {'name': 'rating', 'type': 'FLOAT'},
                  {'name': 'rating_count', 'type': 'INTEGER'},
                  {'name': 'date', 'type': 'DATE'}]
)
