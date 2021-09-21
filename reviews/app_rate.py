import requests
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from google_play_scraper import app

# settings
credentials = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

android_app_id = 'ru.m2.squaremeter'
ios_app_id = '1501340734'
country = 'RU'
os = []
ratings = []
rating_count = []
today = datetime.today().strftime('%Y-%m-%d')

# IOS store data
r = requests.get(f'http://itunes.apple.com/lookup?id={ios_app_id}&country={country}')
req = r.json()
rating = req['results']
os.append('IOS')
rating_count.append(rating[0]['userRatingCount'])
ratings.append(rating[0]['averageUserRating'])

# Android store data
result = app(android_app_id, country=country)
os.append('ANDROID')
rating_count.append(result['ratings'])
ratings.append(result['score'])

df = pd.DataFrame()
df['os'] = os
df['rating'] = ratings
df['rating_count'] = rating_count
df['date'] = today
df.to_gbq(
    destination_table='mobile_apps.rating',
    project_id='m2-main',
    if_exists='replace',
    credentials=g_auth_service,
    table_schema=[{'name': 'os', 'type': 'STRING'},
                  {'name': 'rating', 'type': 'FLOAT'},
                  {'name': 'rating_count', 'type': 'INTEGER'},
                  {'name': 'date', 'type': 'DATE'}]
)
