import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
from time import sleep
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import os

# settings
credentials = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
g_auth_service = service_account.Credentials.from_service_account_file(credentials)
bq_client = bigquery.Client(credentials=g_auth_service)

headers = {'User-Agent': 'Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11'}

keywords = ['недвижимость',
            'новостройки',
            'купить квартиру']


f = []

# ios
for keyword in keywords:
    apps = []
    safe_string = urllib.parse.quote_plus(keyword)
    url = f'https://www.apple.com/ru/search/{safe_string}?src=serp'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    results = soup.find_all("div", attrs={"class": re.compile("rf-serp-explore-curated-position.*rf-serp-curated-product")})
    sleep(2)
    for result in results:
        app = result.find(attrs={'class': 'rf-serp-productname'}).text.strip()
        apps.append(app)
    if 'm2.ru - Покупка недвижимости' not in apps:
        f.append(('m2.ru - Покупка недвижимости', 'ios', keyword, '11'))
        break
    else:
        for result in results:
            app = result.find(attrs={'class': 'rf-serp-productname'}).text.strip()
            if app == 'm2.ru - Покупка недвижимости':
                pos = int(re.findall(r'\d+', result.attrs['class'][0])[0])
                f.append((app, 'ios', keyword, pos))
            else:
                pass



# android
for keyword in keywords:
    apps = []
    safe_string = urllib.parse.quote_plus(keyword)
    url = f'https://play.google.com/store/search?q={safe_string}&c=apps&hl=ru&gl=RU'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    results = soup.find_all("div", attrs={"class": "ImZGtf mpg5gc"})
    sleep(2)
    for result in results:
        app = result.find(attrs={'class': 'WsMG1c nnK0zc'}).text.strip()
        apps.append(app)
    if 'M2.ru: недвижимость и квартиры' not in apps:
        f.append(('m2.ru - Покупка недвижимости', 'ios', keyword, '40'))
        break
    else:
        for result in results:
            app = result.find(attrs={'class': 'WsMG1c nnK0zc'}).text.strip()
            if app == 'M2.ru: недвижимость и квартиры':
                pos = int(re.findall(r'(?<=\;).*\d', result.find('c-wiz')['data-node-index'])[0])+1
                f.append((app, 'android', keyword, pos))
            else:
                pass


df = pd.DataFrame(f, columns=['app', 'os', 'keyword', 'keyword_rating'])

print(df)
df.to_gbq(
    destination_table='mobile_app_data.aso_keywords',
    project_id='m2-main',
    if_exists='append',
    credentials=g_auth_service,
    table_schema=[{'name': 'os', 'type': 'STRING'},
                  {'name': 'app', 'type': 'STRING'},
                  {'name': 'keyword', 'type': 'STRING'},
                  {'name': 'keyword_rating', 'type': 'INTEGER'}
                  ]
)