import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import pygsheets
import logging

# pandas settings
pd.set_option('display.max_rows', None)  # show all rows in terminal
pd.set_option('display.expand_frame_repr', False)  # show all columns in terminal

# TODO перенести в настройки в конфиг файл
# settings
logging.basicConfig(filename='/home/web_analytics/m2-mobile/logging.log')
credentials = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gc = pygsheets.authorize(service_file=credentials)
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)
# spreadsheets
classified_mobile = '1dojfyD0BHErfUx0YpeqsNna33JEPqABMmBaYceDjsqw'
sheet = 'import_all'
# views
all_events = 'm2-main.mobile_apps.view_classified_mobile_all_events'


def send_to_ss(spreadsheet, worksheet, dataframe, start_cell):
    sh = gc.open_by_key(spreadsheet)
    wks = sh.worksheet_by_title(worksheet)
    wks.set_dataframe(dataframe, start=start_cell, copy_head=False, encoding="utf-8-sig")


def bq_get_view(view_id, client=bq_client):
    view = client.get_table(view_id)
    return view.view_query


def get_gs_len(spreadsheet, worksheet):
    sh = gc.open_by_key(spreadsheet)
    wks = sh.worksheet_by_title(worksheet)
    len = wks.get_as_df()
    return len.shape


try:
    df = pd.read_gbq(bq_get_view(all_events), credentials=g_auth_service)
    send_to_ss(classified_mobile, sheet, df, f'A{int(get_gs_len(classified_mobile,sheet)[0])+2}')
except Exception as e:
    logging.exception(str(e))
