from app_store_scraper import AppStore
import pandas as pd
from clickhouse_driver import Client
import os
from google_play_scraper import Sort, reviews_all
import json

# pandas settings
pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal

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
"DROP TABLE IF EXISTS ios_reviews"
)

# ios
app = AppStore(country='ru', app_name="m2.ru", app_id=1501340734)
app.review(sleep=5)
df_ios = pd.json_normalize(app.reviews)
df_ios.drop('developerResponse.id', axis=1, inplace=True)
df_ios.rename(columns={'developerResponse.body': 'developerResponse_body',
                   'developerResponse.modified': 'developerResponse_modified'}, inplace=True)


client.execute(
"CREATE TABLE IF NOT EXISTS ios_reviews (date DateTime64, rating Nullable(INTEGER), userName Nullable(String), isEdited UInt8, title Nullable(String), review Nullable(String), developerResponse_body Nullable(String), developerResponse_modified Nullable(String)) Engine = MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY date"
)

client.insert_dataframe("INSERT INTO ios_reviews VALUES", df_ios)


# android
result = reviews_all(
    'ru.m2.squaremeter',
    sleep_milliseconds=5000,  # defaults to 0
    lang='ru',  # defaults to 'en'
    country='ru',  # defaults to 'us'
    sort=Sort.NEWEST  # defaults to Sort.MOST_RELEVANT
)

c = client.execute(
"DROP TABLE IF EXISTS android_reviews"
)

df_android = pd.json_normalize(result)
df_android.drop(['userImage', 'reviewId'], axis=1, inplace=True)

client.execute(
"CREATE TABLE IF NOT EXISTS android_reviews (at DateTime64, userName Nullable(String), score Nullable(INTEGER), content Nullable(String), thumbsUpCount Nullable(Int32), reviewCreatedVersion Nullable(String), repliedAt Nullable(DateTime64), replyContent Nullable(String)) Engine = MergeTree PARTITION BY toYYYYMMDD(at) ORDER BY at"
)

client.insert_dataframe("INSERT INTO android_reviews VALUES", df_android)


