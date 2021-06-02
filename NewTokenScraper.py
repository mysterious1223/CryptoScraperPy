from bs4 import BeautifulSoup
import pandas as pd 
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.firefox import GeckoDriverManager
from datetime import datetime
import os
import json
import Logger
#initial items
pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.precision', 5)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)



#goals


#objects
#holds other infomation for token
class TokenScannInformation:
    def __init__ (self, max_supply, holders, transfers, decimals):
        self.max_supply= max_supply
        self.holders = holders
        self.transfers = transfers
        self.decimals = decimals 
        

class Token:
    TokenScannInfo = None
    def __init__ (self, name, address, address_url ,typename, generated_timestamp, TokenScannInfo = None):
        self.name = name
        self.address = address
        self.address_url = address_url
        #self.time_elapsed = time_elapsed
        self.typename = typename
        self.generated_timestamp = generated_timestamp
        self.TokenScannInfo = TokenScannInfo

        

    def AddTokenScannInfo(self, TokenScannInfo):
        self.TokenScannInfo = TokenScannInfo
class scraper_component:
    supply_cmp = ''
    holder_cmp = ''
    transfer_cmp = ''
    decimal_cmp = ''
    def __init__ (self, supply_cmp, holder_cmp, transfer_cmp, decimal_cmp):
        self.supply_cmp = supply_cmp
        self.holder_cmp = holder_cmp
        self.transfer_cmp = transfer_cmp
        self.decimal_cmp = decimal_cmp


class NewTokenScaper:
    #const
    tokensnifferurl = 'https://tokensniffer.com/tokens/new'
    tokens = []
    binance_text = 'Binance'
    etherium_text = 'Etherium'
    driver = None
    ext_driver = None
    page_wait_time = 0

    CSV_FOLDER = './csv'
    def __init__ (self, frequency = 1, page_wait_time = 5):
        print ('Starting Scraper...')
        self.frequency = frequency
        self.page_wait_time = page_wait_time
        # start the driver
        try:
            self.driver_exe = './geckodriver' #GeckoDriverManager().install()
        except:
            print ('Failed to download driver...')
            raise
        self.etherium_cmp = scraper_component (
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div.row.align-items-center div.col-md-8.font-weight-medium span.hash-tag.text-truncate'
        , 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_tr_tokenHolders div.row.align-items-center div.col-md-8 div.d-flex.align-items-center div.mr-3', 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_trNoOfTxns div.row.align-items-center div.col-md-8'
        , 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6 div.card.h-100 div.card-body div#ContentPlaceHolder1_trDecimals div.row.align-items-center div.col-md-8')
        self.binance_cmp = scraper_component (
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div.row.align-items-center div.col-md-8.font-weight-medium span.hash-tag.text-truncate'
        , 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_tr_tokenHolders div.row.align-items-center div.col-md-8 div.d-flex.align-items-center div.mr-3', 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6.mb-3.mb-md-0 div.card.h-100 div.card-body div#ContentPlaceHolder1_trNoOfTxns div.row.align-items-center div.col-md-8 span#totaltxns'
        , 
        'html body#body div.wrapper main#content div#ContentPlaceHolder1_divSummary.container.space-bottom-2 div.row.mb-4 div.col-md-6 div.card.h-100 div.card-body div#ContentPlaceHolder1_trDecimals div.row.align-items-center div.col-md-8')

        
    def RunScraper (self):
        print (f'Driver starting {self.driver_exe}')
        #self.driver = webdriver.Firefox(executable_path=self.driver_exe)

        fireFoxOptions = webdriver.FirefoxOptions()
        fireFoxOptions.headless=True
        self.driver = webdriver.Firefox (executable_path=self.driver_exe, options=fireFoxOptions)

        if not os.path.exists(self.CSV_FOLDER):
            os.makedirs(self.CSV_FOLDER)
        
        while (1):
            #check frequency
            time.sleep(60 - time.time() % 60)
            print ('Scrape Job Starting')
            
            self.ReloadPageContentFromTokenSniffer(self.driver)


            outfile = self.ProcessScrapedDataToList(self.CSV_FOLDER)
            print (f"output CSV saved {self.CSV_FOLDER}/{outfile}")
            print (f'Scrape Job Sleeping For {self.frequency}')
            #clear token list
            self.tokens.clear()
            

    def ReloadPageContentFromTokenSniffer (self, driver):
        self.driver.refresh()
        time.sleep (self.page_wait_time)
        self.driver.get(self.tokensnifferurl)
        time.sleep (self.page_wait_time)
        self.driver.implicitly_wait(15)
        self.left_page_content = driver.page_source.encode('utf-8')
        try:
            right_button = driver.find_element_by_xpath("/html/body/div/div/main/div[2]/div[1]/span[2]/a")
        except:
            print (f'Error when attemping to pull contents from {self.tokensnifferurl}')
            print ('Retrying...')
            self.ReloadPageContentFromTokenSniffer (driver)
        right_button.click()
        time.sleep (self.page_wait_time)
        self.right_text_item = right_button.text
        self.driver.implicitly_wait(15)
        self.right_page_content = driver.page_source.encode('utf-8')   
    def ProcessScrapedDataToList (self, csv_location):
        if self.right_text_item == self.binance_text:
            soup_binance = BeautifulSoup(self.right_page_content, 'html.parser') 
            soup_etherium = BeautifulSoup(self.left_page_content, 'html.parser')
        else:
            soup_binance = BeautifulSoup(self.left_page_content, 'html.parser') 
            soup_etherium = BeautifulSoup(self.right_page_content, 'html.parser')
        
        data_etherium = soup_etherium.find_all ("tr")
        data_binance = soup_binance.find_all ("tr")
        self.add_data_to_list (data_etherium, self.etherium_text)
        self.add_data_to_list (data_binance, self.binance_text)


        new_data_array = [[token.name, token.address, token.address_url, token.typename, token.generated_timestamp, token.TokenScannInfo.max_supply,
            token.TokenScannInfo.holders, token.TokenScannInfo.transfers, token.TokenScannInfo.decimals, 'New'] for token in self.tokens]
        new_token_data_panda = pd.DataFrame(data=new_data_array, 
                      columns=['Token Name', "Address", "Address Url", "Crypto Type", "Time Stamp", "Max Supply", "# of Holder", "Transfers", "Decimals", "Token Type"])
        outfile = self.GenerateCSV (new_token_data_panda, csv_location)
        return outfile
    def add_data_to_list (self,pagedata, type_str):
        now = datetime.now()
        for element in pagedata:
            try:
                token_name = element.find ('th', class_ = "Home_name__3fbfx").text
                address_url = element.find ('a', class_ = "Home_address__2ERkX")['href']
                address = element.find ('a', class_ = "Home_address__2ERkX").text
                #creation_time_passed = element.find ('td', class_='Home_mono__eWDn4').text
            except:
                print (f'Error on element data extraction: {element}')
                break
            typename = type_str
            generated_timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
            ScannData = TokenScannInformation ('', '', '', '')
            try:
               ScannData = self.GetScannTokenInfo(address_url, token_name, typename)
            except:
                print (f'Error extracting Scann info {token_name} @ {address_url}')
            #print (typename)
            newtoken = Token (token_name, address, address_url, typename, generated_timestamp, ScannData)
            self.tokens.append (newtoken)
            #break
    def GetScannTokenInfo (self, address_url, token_name, typename):
        print (f'Retrieving data for {token_name} @ {address_url}')
        page_content = self.ScrapeAdditionalPageData(address_url)
        #generate the additional information
        if typename == self.etherium_text:
            ScannData = self.get_token_information (page_content, self.etherium_cmp)
        else:
            ScannData = self.get_token_information (page_content, self.binance_cmp)
        return ScannData
    def ScrapeAdditionalPageData(self,url, attempt = 0):
        try:
            fireFoxOptions = webdriver.FirefoxOptions()
            fireFoxOptions.headless=True
            self.ext_driver = webdriver.Firefox(executable_path=self.driver_exe, options=fireFoxOptions)
            self.ext_driver.get(url)
            time.sleep(self.page_wait_time)
            self.ext_driver.implicitly_wait(30)
            page_content = self.ext_driver.page_source.encode('utf-8')
            self.ext_driver.quit()
        except:
            print (f'Error when trying to retrieve page contents {url}')
            if attempt < 10:
                print (f'Retrying... attempt {attempt}')
                time.sleep(self.page_wait_time + attempt)
                self.ScrapeAdditionalPageData(url, attempt + 1)
                
        if self.ext_driver != None:
            self.ext_driver.quit()
        return page_content
    def get_token_information (self,page_content, component):
        page_content = BeautifulSoup(page_content, 'html.parser')
        max_supply = page_content.select (component.supply_cmp)[0].text.replace(",","").strip()
        holders_count = page_content.select(component.holder_cmp)[0].text.replace(",","").replace("addresses", "").strip()
        transfer_count = page_content.select (component.transfer_cmp)[0].text.replace(",","").strip()
        decimal_count = page_content.select(component.decimal_cmp)[0].text.replace(",","").strip()
        return TokenScannInformation (max_supply, holders_count, transfer_count, decimal_count)
    def GenerateCSV (self, dataFrame, location):
        now = datetime.now()
        dt_string = now.strftime("%d%m%Y_%H%M%S")
        dataFrame.to_csv(f'{location}/{dt_string}.csv', float_format='%.20f')
        return f'{dt_string}.csv'
    def __del__(self):
        if self.driver != None:
            self.driver.quit()
        if self.ext_driver != None:
            self.ext_driver.quit()
        pass

#script start


scraper_app = NewTokenScaper()
scraper_app.RunScraper()


# todo
# we will add threading
# create a 