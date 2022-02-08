import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import pygsheets
import logging
import os

# pandas settings
pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal

# TODO перенести в настройки в конфиг файл
# settings
# logging.basicConfig(filename='/home/web_analytics/m2-mobile/logging.log')
credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
gc = pygsheets.authorize(service_file=credentials)
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

# spreadsheet
classified_dash = '1RA0dP94Svyw-Xe0rnQMzpxxRir0ylCbS8om95pMgxWE'

# bq views
avg_dau = 'm2-main.mobile_apps.view_avg_weekly_dau'
firebase_installs = 'm2-main.mobile_apps.view_installs_firebase'
m2_rating = 'm2-main.mobile_apps.view_rating_dashboard'
wau = 'm2-main.mobile_apps.view_wau'
wau_rk = 'm2-main.mobile_apps.view_rk_wau'
mau_rk = 'm2-main.mobile_apps.view_rk_mau'


def send_to_gs(spreadsheet, worksheet, dataframe, start_cell):
    sh = gc.open_by_key(spreadsheet)
    wks = sh.worksheet_by_title(worksheet)
    wks.set_dataframe(dataframe, start=start_cell, copy_head=False, extend=True, encoding="utf-8-sig")


def bq_get_view(view_id, client=bq_client):
    view = client.get_table(view_id)
    return view.view_query


def get_gs_len(spreadsheet, worksheet):
    sh = gc.open_by_key(spreadsheet)
    wks = sh.worksheet_by_title(worksheet)
    gs_len = wks.get_as_df(include_tailing_empty=False)
    return gs_len.shape


# new_dashboard DAU
try:
    df = pd.read_gbq(bq_get_view(avg_dau), credentials=g_auth_service)
    send_to_gs(classified_dash, 'DAU', df, f'A{int(get_gs_len(classified_dash,"DAU")[0])+2}')
except Exception as e:
    logging.exception(str(e))


# new_dashboard Firebase installs
try:
    df = pd.read_gbq(bq_get_view(firebase_installs), credentials=g_auth_service)
    send_to_gs(classified_dash, 'Installs', df, f'A{int(get_gs_len(classified_dash,"Installs")[0])+2}')
except Exception as e:
    logging.exception(str(e))


# new_dashboard m2 app rating
try:
    df = pd.read_gbq(bq_get_view(m2_rating), credentials=g_auth_service)
    df['rating'] = df['rating'].astype(str).replace(r'[\.]', ',', regex=True)
    send_to_gs(classified_dash, 'ratings', df, f'A{int(get_gs_len(classified_dash,"ratings")[0])+2}')
except Exception as e:
    logging.exception(str(e))


# new_dashboard wau
try:
    df = pd.read_gbq(bq_get_view(wau), credentials=g_auth_service)
    send_to_gs(classified_dash, 'WAU', df, f'A{int(get_gs_len(classified_dash,"WAU")[0])+2}')
except Exception as e:
    logging.exception(str(e))


# new_dashboard wau_rk
try:
    df = pd.read_gbq(bq_get_view(wau_rk), credentials=g_auth_service)
    send_to_gs(classified_dash, 'WAU MAU RK', df, f'A{int(get_gs_len(classified_dash,"WAU MAU RK")[0])+2}')
except Exception as e:
    logging.exception(str(e))

# new_dashboard wau_rk
try:
    df = pd.read_gbq(bq_get_view(mau_rk), credentials=g_auth_service)
    send_to_gs(classified_dash, 'WAU MAU RK', df, f'D2')
except Exception as e:
    logging.exception(str(e))
