import requests
import pandas as pd
from datetime import datetime
from google_play_scraper import app
import os
from clickhouse_driver import Client
import json

# get creds
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

# pandas settings
pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal


client = Client(host=host, port='9000', user=user, password=password, database=database, settings={'use_numpy': True})
client.execute(
"CREATE TABLE IF NOT EXISTS reviews (os String, app String, rating Float64, rating_count Int64, date Date) Engine = MergeTree PARTITION BY date ORDER BY date"
)
android_app_id = {'ru.m2.squaremeter': 'Метр Квадратный',
                  'ru.cian.main': 'Циан',
                  # 'ru.domclick.mortgage': 'ДомКлик',
                  'com.avito.android': 'Авито'
                  }

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

client.insert_dataframe("INSERT INTO reviews VALUES", df)
