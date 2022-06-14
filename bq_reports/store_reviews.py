from app_store_scraper import AppStore
import pandas as pd
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import os
from google_play_scraper import Sort, reviews_all

pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal

# settings
credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)


# ios
app = AppStore(country='ru', app_name="m2.ru", app_id=1501340734)
app.review(sleep=5)
df_ios = pd.json_normalize(app.reviews)
df_ios.drop('developerResponse.id', axis=1, inplace=True)
df_ios.rename(columns={'developerResponse.body': 'developerResponse_body',
                   'developerResponse.modified': 'developerResponse_modified'}, inplace=True)

df_ios.to_gbq(
    destination_table='mobile_app_data.ios_reviews',
    project_id='m2-main',
    if_exists='replace',
    credentials=g_auth_service,
    table_schema=[{'name': 'date', 'type': 'STRING'},
                  {'name': 'rating', 'type': 'INTEGER'},
                  {'name': 'userName', 'type': 'STRING'},
                  {'name': 'isEdited', 'type': 'STRING'},
                  {'name': 'title', 'type': 'STRING'},
                  {'name': 'review', 'type': 'STRING'},
                  {'name': 'developerResponse.body', 'type': 'STRING'},
                  {'name': 'developerResponse.modified', 'type': 'STRING'}
                  ]
)

# android
result = reviews_all(
    'ru.m2.squaremeter',
    sleep_milliseconds=5000,  # defaults to 0
    lang='ru',  # defaults to 'en'
    country='ru',  # defaults to 'us'
    sort=Sort.NEWEST  # defaults to Sort.MOST_RELEVANT
)

df_android = pd.json_normalize(result)
df_android.drop(['userImage', 'reviewId'], axis=1, inplace=True)

df_android.to_gbq(
    destination_table='mobile_app_data.android_reviews',
    project_id='m2-main',
    if_exists='replace',
    credentials=g_auth_service,
    table_schema=[{'name': 'at', 'type': 'STRING'},
                  {'name': 'userName', 'type': 'STRING'},
                  {'name': 'score', 'type': 'INTEGER'},
                  {'name': 'content', 'type': 'STRING'},
                  {'name': 'thumbsUpCount', 'type': 'INTEGER'},
                  {'name': 'reviewCreatedVersion', 'type': 'STRING'},
                  {'name': 'repliedAt', 'type': 'STRING'},
                  {'name': 'replyContent', 'type': 'STRING'},
                  ]
)
