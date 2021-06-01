from bs4 import BeautifulSoup
import requests
import pandas as pd 
import time
from selenium import webdriver
import selenium
from selenium.webdriver.common.keys import Keys
from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime
from urllib.parse import urlparse
import os

LUNARCRUSH_API_KEY = '3kux2awe44ltmqhjw93cep'
#https://api.lunarcrush.com/v2?data=assets&key=3kux2awe44ltmqhjw93cep&symbol=BTC
Selenium_Firefox_Driver_Location = './geckodriver'

Scan_Holder_Css = 'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_tr_tokenHolders div.row.align-items-center div.col-md-8 div.d-flex.align-items-center div.mr-3'
Scan_Transfer_Css = 'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_trNoOfTxns div.row.align-items-center div.col-md-8'
class LunarCrushAPI:
    def __init__ (self):
        pass
    
class ScannResults:

    def __init__(self, holder, transfer, typename):
        self.holder = holder
        self.transfers = transfer
        self.typename = typename
class Exchange:
    def __init__ (self, ExchangeName):
        self.ExchangeName = ExchangeName

        

class Token:
    TokenScannInfo = None
    def __init__(self, name, symbol, price, type, MarketCap, VolumeTwentyFour, CirculatingSupply, 
        TotalSupply, TimeStamp, Address, AddressUrl, holders, transfers, Exchanges = []):

        self.name = name
        self.symbol = symbol
        self.price = price
        self.MarketCap = MarketCap
        self.VolumeTwentyFour = VolumeTwentyFour
        self.TimeStamp = TimeStamp
        self.Address = Address
        self.AddressUrl = AddressUrl
        self.type = type

        self.CirculatingSupply = CirculatingSupply
        self.TotalSupply = TotalSupply
        self.holders = holders
        self.transfers = transfers

        self.Exchanges = Exchanges
    def AddExchange (self, ExchangeName, Price, TwentyFourHourTradeVolume):
        tExhange = Exchange (ExchangeName, Price, TwentyFourHourTradeVolume)
        self.Exchanges.append (tExhange)

    
class CryptoScraper:

    _CoinMarketCapCryptoSearchUrl = 'https://coinmarketcap.com/currencies/' # followed by crypto name
    _CoinMarketCapBaseUrl = 'https://coinmarketcap.com'
    #next page - > https://coinmarketcap.com/?page=2

    _coinrankingBaseUrl = 'https://coinranking.com'
    _tokens = []
    # FireFox
    driver = None
    CSV_FOLDER = './csv'
    def __init__ (self, delay = 1):
        self.delay = delay

    def ScrapeCryptosFromAllRegisteredSources (self):
        while (1):
            # scrape from CoinMarketCap
            #self.ScrapeCryptosFromCoinMarketCap()
            # reset crypto list
            time.sleep(60 - time.time() % 60)
            print ('Scrape Starting...')
            outfile=self.ScrapeCryptosFromCoinRanking(self.CSV_FOLDER)
            print (f"output CSV saved {self.CSV_FOLDER}/{outfile}")
            self._tokens.clear()
            
    def GenerateCSV (self, dataFrame, location):
        now = datetime.now()
        dt_string = 'CommonTokens' + now.strftime("%d%m%Y_%H%M%S")
        dataFrame.to_csv(f'{location}/{dt_string}.csv', float_format='%.20f')
        return f'{dt_string}.csv'


    def ScrapePageContent(self, url):
        cmc = requests.get(url,headers={'User-Agent': 'Mozilla/5.0 (Platform; Security; OS-or-CPU; Localization; rv:1.4) Gecko/20030624 Netscape/7.1 (ax)'})
        soup = BeautifulSoup(cmc.content, 'html.parser')
        return soup
    def ScrapePageContent_usingDriver(self, url, driver_location, timeout):

        fireFoxOptions = webdriver.FirefoxOptions()
        fireFoxOptions.headless=True
        self.driver = webdriver.Firefox (executable_path=driver_location, options=fireFoxOptions)
        self.driver.get(url)
        time.sleep(timeout)
        self.driver.implicitly_wait(30)
        page_content = self.driver.page_source.encode('utf-8')
        self.driver.quit()

        return page_content


    def ScrapeCryptosFromCoinRanking (self, OutputLocation):
        soup = self.ScrapePageContent (self._coinrankingBaseUrl)
        #print (soup.prettify())
        #pagnation_content = soup.find ('div', class)

        num_pages = soup.find_all ('a', class_='pagination__page')
        list_of_pages = []
        for num in num_pages:
            page_num = num.text
            if page_num.isnumeric():
                list_of_pages.append(int(page_num))
        max_page_number = max(list_of_pages)
        #print (max_page_number)

        for page_num in range(1, max_page_number + 1):
            #start scraping
            #print (page_num)
            
            page_content = self.ScrapePageContent (f'{self._coinrankingBaseUrl}/?page={page_num}')

            # get elements from page
            crypto_rows = page_content.find('tbody',class_='table__body')
            crypto_rows = crypto_rows.find_all('tr', class_='table__row table__row--click table__row--full-width')
            for crypto_row in crypto_rows:
                #print (crypto_row.text)

                token_name = crypto_row.find('a',class_='profile__link').text.strip()
                token_sym = crypto_row.find('span',class_='profile__subtitle').text.strip()

                #find more info:

                token_url = crypto_row.find('a',class_='profile__link')['href']
                token_url  = f'{self._coinrankingBaseUrl}{token_url}'
                token_page_content = self.ScrapePageContent (token_url)
                token_page_items = token_page_content.find_all('td', class_='stats__value')
                

                token_price = token_page_items[0].text.replace('\n','').replace(' ','').replace('$', '').strip()
                token_rank = token_page_items[2].text.strip()
                token_twenty_four_hour_volume = token_page_items[3].text.replace('\n','').replace('$', '').strip()
                token_market_cap = token_page_items[4].text.replace('\n','').replace('$', '').strip()
                #print (f'{token_rank}, {token_name}, {token_sym}, {token_price}, {token_market_cap}, {token_twenty_four_hour_volume}')
                #print (token_url)

                # get supply information!

                #token_rank = crypto_row.find('span', class_='profile__rank').text.strip()
                #token_name = crypto_row.find('a',class_='profile__link').text.strip()
                #token_sym = crypto_row.find('span',class_='profile__subtitle').text.strip()
                #token_price_content = crypto_row.find_all('div', class_='valuta valuta--light')
                #token_price = token_price_content[0].text.replace('\n','').replace(' ','').replace('$', '').strip()
                #token_market_cap = token_price_content[1].text.replace('\n','').replace('$', '').lstrip()
                #token_price = crypto_row.find('div', class_='valuta valuta--light').text.strip()
                #token_market_cap = crypto_row.find('div', class_='valuta valuta--light').text.strip()

                token_page_items_supply = token_page_content.find ('table', class_='supply-information__table')
                token_page_item_supply_rows = token_page_items_supply.find_all('tr', class_='supply-information__table-row')
                #print (token_page_items_supply.prettify())

                token_current_supply_ele = token_page_item_supply_rows[0].find('abbr')
                token_total_supply_ele = token_page_item_supply_rows[1].find('abbr')
                token_circulated_supply = '0'
                token_total_supply = '0'
            
                if token_current_supply_ele != None:
                    token_circulated_supply = token_current_supply_ele.text.replace('\n','').strip()
                if token_total_supply_ele != None:
                    token_total_supply = token_total_supply_ele.text.replace('\n','').strip()

                
                #print (f'{token_circulated_supply}, {token_total_supply}')

                # get holders and transfers
                # we need to get the address from coinmarket cap
                # we will need to use pancakeswap and uniswap api for this
                # we need address. Maybe we can query coin market cap?
                #bscan api or etherscan api? []
                
                address_url = self.GetAddressUrlFromCoinMarketCap (token_name)
                address = ''
                scann_result = ScannResults ('','','')
                if len(address_url) > 1:
                    address = address_url.rsplit ('/')[-1]
                    scann_result = self.GetHolderDetailsFromScannSites (address_url)
                # get Exhanges
                exchanges_url = f'{token_url}/exchanges'
                exchanges = self.CoinRankingGetExchangeInformation(exchanges_url)
                #print (exchanges_url)
                #final
                now = datetime.now()
                token = Token (token_name, token_sym, token_price, scann_result.typename,token_market_cap,token_twenty_four_hour_volume
                    ,token_circulated_supply, token_total_supply,
                        now.strftime("%d/%m/%Y %H:%M:%S"), address, address_url, scann_result.holder, scann_result.transfers, exchanges)
                print (f'Working on : {token.name} {token.Address}')

                #self.DebugOutTokenInformation (token)


                #Export token data to a CSV
                #update the models on C# to handle this datasets

                self._tokens.append (token)
                #break
            #break
        # we need to export a panda list
        #def __init__(self, name, symbol, price, type, MarketCap, VolumeTwentyFour, CirculatingSupply, 
        #TotalSupply, TimeStamp, Address, AddressUrl, holders, transfers, Exchanges = []):
        new_data_array =  [[token.name, token.Address, token.AddressUrl, token.type, token.TimeStamp, token.TotalSupply,
            token.holders, token.transfers, 'N/A', 'Common', token.price, token.MarketCap, token.VolumeTwentyFour, token.CirculatingSupply, self.ExhangesToString(token.Exchanges)] for token in self._tokens]


        new_token_data_panda = pd.DataFrame(data=new_data_array, 
                      columns=['Token Name', "Address", "Address Url", "Crypto Type", "Time Stamp", "Max Supply", "# of Holder", "Transfers", "Decimals", "Token Type"
                      , "Price", "Market Cap", "VolumeTwentyFour", "Circulating Supply", "Exchanges"])

        outfile = self.GenerateCSV (new_token_data_panda, OutputLocation)
        return outfile
    
    def DebugOutTokenInformation (self, token):
        output = f"""
        {token.name}, {token.symbol}, {token.price}, {token.type}, {token.MarketCap}, {token.VolumeTwentyFour}, {token.CirculatingSupply},
        {token.TotalSupply}, {token.TimeStamp}, {token.Address}, {token.AddressUrl}, {token.holders}, {token.transfers}, 
        {self.DebugPrintExhanges(token.Exchanges)}
        """
        print (output)
    def ExhangesToString (self, exchanges):
        out = ''

        for exchange in exchanges:
            out += ',' + exchange.ExchangeName

        out = out[1:]
        return out

    def CoinRankingGetExchangeInformation(self, url):
        page_count = 1
        exchanges = []
        page_contents = [] 
        page_contents.append(self.ScrapePageContent (url))
        page_div=page_contents[0].find('div', class_='pagination__list')
        if page_div != None:
            #more than one page
            #we need to get max page count and loop through
            #finally add a page content to out list
            #print ('more than one page')
            pag_tags = page_div.find_all('a')
            if pag_tags != None:
                pag_tags = pag_tags[-1]
                page_count = int(pag_tags.text)
                for page in range (2, page_count + 1):
                    page_contents.append(self.ScrapePageContent (f'{url}?page={page}'))

        # now loop through page_contents
        #needs improvement, not getting everything *******
        for page_content in page_contents:
            #print (page_content.prettify())
            page_div = page_content.find ('tbody', class_='table__body')
            #print (page_div.prettify())
            if page_div == None:
                #print ('break1')
                break
            page_trs = page_div.find_all('tr')
            #print (page_trs)
            if page_trs == None:
                #print ('break2')
                break
            for tr in page_trs:
                
                exchange_name = tr.find('span', class_='profile__name')
                #print (exchange_name)
                if exchange_name == None:
                    
                    break
                exchange_name = exchange_name.text.replace('\n', '').strip()
                exchanges.append(Exchange(exchange_name))


        return exchanges
        
    def GetHolderDetailsFromScannSites (self, url):
        page_content = BeautifulSoup(self.ScrapePageContent_usingDriver (url,Selenium_Firefox_Driver_Location, 4)
            ,'html.parser')
        #print (url)
        #print (page_content.select('#ContentPlaceHolder1_tr_tokenHolders > div > div.col-md-8 > div > div'))    
        holder_div = page_content.select('#ContentPlaceHolder1_tr_tokenHolders > div > div.col-md-8 > div > div')
        transfer_div = page_content.select ('#totaltxns')

        holder_count = ''
        transfer_count = ''
        if holder_div != None and len(holder_div) > 0:
            holder_count = holder_div[0].text.replace(",","").replace("addresses", "").strip()
            transfer_count = transfer_div[0].text.replace(",","").strip()

        domain_name = urlparse(url).netloc.rsplit('.')[0]
        typename = ''
        if domain_name == 'bscscan':
            typename = 'Binance'
        elif domain_name == 'etherscan':
            typename = 'Etherium'
        else:
            typename = 'Unknown'

        scann_results = ScannResults (holder_count, transfer_count, typename)
        return scann_results

    def GetAddressUrlFromCoinMarketCap (self, tokenname):
        url = f'{self._CoinMarketCapCryptoSearchUrl}{tokenname}'
        page_content = self.ScrapePageContent (url)
        address_url = ''

        content = page_content.find('div', class_='mainChain___3CfU2')
        if content != None:
            address_content = content.find('a')#['href']
            if address_content != None:
                address_url = address_content['href']


        return address_url

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

        for p in range (1, max_page_number + 1):
            #soup = self.ScrapePageContent (f'{self._CoinMarketCapBaseUrl}/?page={p}')
            #page_content = soup.find_all ('table', class_='cmc-table')
            #print (page_content)

            soup = self.ScrapePageContent (f'{self._CoinMarketCapBaseUrl}/?page={p}')
            page_body = soup.find ('tbody')
            page_rows = page_body.find_all ('tr')

            for row in page_rows:
                crypto_page_url = row.find('a')['href']
                print (crypto_page_url)
                token = self.GetTokenDetails (f'{self._CoinMarketCapBaseUrl}{crypto_page_url}')
                break
            #break
        pass
    #CoinMarketCap
    def GetTokenDetails(self, url):
        token = None
        soup = self.ScrapePageContent (url)
        token_name = soup.find('h2', class_='sc-1q9q90x-0')
        token_sym = token_name.find('small')
        token_sym = token_sym.text
        token_name = token_name.text.replace(token_sym, '')



        print (f'{token_name} , {token_sym}')
        return token
        pass 
    def __del__ (self):
        if self.driver != None:
            print ('Clean up')
            self.driver.quit()


scraper = CryptoScraper ()

scraper.ScrapeCryptosFromAllRegisteredSources()