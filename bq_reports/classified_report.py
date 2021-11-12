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
logging.basicConfig(filename='/home/web_analytics/m2-mobile/logging.log')
credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')

gc = pygsheets.authorize(service_file=credentials)
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

# spreadsheets
classified_mobile = '1dojfyD0BHErfUx0YpeqsNna33JEPqABMmBaYceDjsqw'
new_dashboard = '17pX-dnJpKh8-UNyzZl-lWGXOfMIsfo480auUieWwOP8'
sheet_all = 'import_all'
sheet_phone_call = 'bq_Classified'
sheet_apprating = 'app_ratings'

# bq views
all_events = 'm2-main.mobile_apps.view_classified_mobile_all_events'
phone_call = 'm2-main.mobile_apps.view_classified_mobile_show_and_call'


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
    len = wks.get_as_df()
    return len.shape


# import all
try:
    df = pd.read_gbq(bq_get_view(all_events), credentials=g_auth_service)
    send_to_gs(classified_mobile, sheet_all, df, "A2")
except Exception as e:
    logging.exception(str(e))


# bq_classified
try:
    df = pd.read_gbq(bq_get_view(phone_call), credentials=g_auth_service)
    send_to_gs(classified_mobile, sheet_phone_call, df, f'A{int(get_gs_len(classified_mobile,sheet_phone_call)[0])+2}')
except Exception as e:
    logging.exception(str(e))


# new_dashboard
try:
    df = pd.read_gbq("""SELECT date, os, app, round(rating,2) AS rating FROM `m2-main.mobile_apps.rating` where date = current_date()""", credentials=g_auth_service)
    df['rating'] = df['rating'].astype(str).replace(r'[\.]', ',', regex=True)
    send_to_gs(new_dashboard, sheet_apprating, df, f'A{int(get_gs_len(new_dashboard,sheet_apprating)[0])+2}')
except Exception as e:
    logging.exception(str(e))
