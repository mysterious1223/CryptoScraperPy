#this will be a child of protrix crypto api base

# test calls below.

from bs4 import BeautifulSoup
import requests
import json
import pandas as pd 

url = "https://api.lunarcrush.com/v2?data=assets&key=3kux2awe44ltmqhjw93cep&symbol=SAFEMOON"

cmc = requests.get(url,headers={'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'})
soup = BeautifulSoup(cmc.content, 'html.parser')
oJson = json.loads(soup.text)['data'][0]
print (oJson['tweet_spam_calc_24h_previous'])
#oJson = json.loads(soup.text)['ticker']#["price"]