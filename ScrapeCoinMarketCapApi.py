from bs4 import BeautifulSoup
import requests
import pandas as pd 
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime

import scrapy

import os

class CryptoScraper:

    _CoinMarketCapCryptoSearchUrl = 'https://coinmarketcap.com/currencies/' # followed by crypto name
    _CoinMarketCapBaseUrl = 'https://coinmarketcap.com/'
    #next page - > https://coinmarketcap.com/?page=2
    _tokens = []
    def __init__ (self, delay = 1):
        self.delay = delay

    def ScrapeCryptosFromAllRegisteredSources (self):
        while (1):
            # scrape from CoinMarketCap
            self.ScrapeCryptosFromCoinMarketCap()
            # reset crypto list
            self._tokens.clear()
            break
    def ScrapePageContent(self, url):
        cmc = requests.get(url,headers={'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'})
        soup = BeautifulSoup(cmc.content, 'html.parser')
        return soup
    
    def ScrapeCryptosFromCoinMarketCap (self):
        soup = self.ScrapePageContent (self._CoinMarketCapBaseUrl)
        #get number of pages
        num_pages = soup.find_all ('a', role='button')
        list_of_pages = []
        for num in num_pages:
            page_num = num.text
            if page_num.isnumeric():
                list_of_pages.append(int(page_num))

        max_page_number = max(list_of_pages)

        print (f'Max number of pages {max_page_number}')


        for p in range (1, max_page_number + 1):
            soup = self.ScrapePageContent (f'{self._CoinMarketCapBaseUrl}/?page={p}')
            page_body = soup.find ('tbody')
            page_rows = page_body.find_all ('tr')
         

            #print (page_content)

            for row in page_rows:
                crypto_page = row.find('a')['href'])
                #print (row.text)
                break

            break
         

        pass

scraper = CryptoScraper ()

scraper.ScrapeCryptosFromAllRegisteredSources()
