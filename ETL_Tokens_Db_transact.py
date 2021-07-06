#Author: Kevin Singh

import pandas as pd
from pandas.core.frame import DataFrame
import pymssql
import os
import sys
import time
from enum import Enum 

db_config = None
data_lake_location = []

basic_token_headers = ['Token Name', 'Address', 'Address Url', 'Crypto Type', 'Time Stamp', 'Max Supply', '# of Holder', 
'Transfers', 'Decimals', 'Token Type', 
# common fields
'Price', 'Market Cap', 'VolumeTwentyFour', 'Circulating Supply'
#Exchanges
]


class DbConfiguration:
    server_addr = ''
    user = ''
    password = ''
    db_name = ''

def parse_data_lake_args (locations = []) -> int:
    global data_lake_location
    data_lake_location = locations
    return len(locations) + 1

def GenerateDbConfiguration (location) -> DbConfiguration:
    config = DbConfiguration ()
    with open(location) as f:
        contents = f.readlines()
    
    for item in contents:
        items = item.rsplit(':')
        if items[0].strip() == 'server_addr':
            config.server_addr = items[1].replace('\n','').strip()
        if items[0].strip() == 'user':
            config.user = items[1].replace('\n','').strip()
        if items[0].strip() == 'password':
            config.password = items[1].replace('\n','').strip()
        if items[0].strip() == 'db_name':
            config.db_name = items[1].replace('\n','').strip()
    return config

def __main__() -> bool:
    if len(sys.argv) >= 2:
        print (f'Loading log file {sys.argv[1]}')
        global db_config 
        db_config = GenerateDbConfiguration (sys.argv[1])
        if len(sys.argv) >= 3:
            num_locations = parse_data_lake_args (sys.argv [2::])
            return True if num_locations > 0 else False
        else:
            print ('No locations supplied')
            return False
    else:
        print ('Please supply the log file in args')
        return False


#search the datalakes for files needing to be processed.
#if file is on import log. IN the database, skip, else process


#db interactive object
class database_query_handle:
    _db_config = None
    _conn = None
    _cursor = None
    def __init__ (self, db_config):
        self._db_config = db_config
    def connect (self) -> bool:
        self._conn=pymssql.connect(self._db_config.server_addr, self._db_config.user, self._db_config.password, self._db_config.db_name,100000)
        _cursor = self._conn.cursor(as_dict=True)
        print ('DB connection made')
        return True
    def fetch_query (self, query) -> pd.DataFrame: # run a single select and get one result
        try:
            df = pd.read_sql(query, self._conn)
            return df
        except:
            print (f'{query} : failed!')
            return pd.DataFrame()
    def execute_query (self, query) -> bool: # run insert, update, or del
        try:
            self.cursor.execute(query)
            self.conn.commit()
            return True
        except:
            print (f'Execution error on command {query}')
            return False
      
    def execute_many_query (self, query, data_list):
        try:
            self.cursor.execute_many(query, data_list)
            self.conn.commit()
            return True
        except:
            print (f'Execution error on command {query}')
            return False
    def __del__ (self):
        print ('Destruct db connection')
        self._conn.close()
#main applet to connect to db and run app
#ETL
#take data from csv and format accordingly
#we will need to extract the exchanges and store into its own table
#we will need to format the money fields
#we will need to remove special characters

#Load into database. We will have two tables Exchanges and Tokens, TokenExchangeMapper


common_col = 17
new_col = 11

class dataType (Enum):
    new = 2
    common = 1    
    unknown = 0
class data_import_file:
    _filename = ''
    _data = None
    _datatype = dataType.unknown
    def __init__ (self, filename, data, datatype: dataType):
        self._filename = filename
        self._data = data
        self._datatype = datatype
        

class TokenImporter:
    _db_conn = None
    _dl_locations = [] #data lake locations
    def __init__ (self, db_conn, dl_locations = []):
        self._db_conn = db_conn
        self._dl_locations = dl_locations

    def run (self):
        toImportFiles = []
        isWatingMessageTrigger = True
        while (1):
            #time.sleep(60 - time.time() % 60)
            time.sleep(2)
            #print ('looking for new files')
            toImportFiles = self.get_import_data_from_all_datalakes ()
            if len(toImportFiles) > 0:
                isWatingMessageTrigger = True
            #Go through the list and import each file
            #self.ImportDataFileToDb(ToImportFiles)
                self.import_datafiles_to_db (toImportFiles)
            if isWatingMessageTrigger:
                print ('Sleeping...')
            isWatingMessageTrigger = False

    def grab_import_data_from_datalake (self, path):
        importList = []
        files = os.listdir(path)
        files = [x for x in files if x.endswith('.csv')]
        if len (files) == 0:
            return importList
        # go through location
        for filename in files:
            # we need to check if file has been imported!   
            full_file_name = f'{path}/{filename}'
            if not self.check_if_file_has_been_imported_query (filename):
                # we will need to extract the information from the file using pandas!

                import_df = self.extract_info_from_importfile_to_object(full_file_name)
                if import_df is not None:
                    importList.append (import_df)
                else:
                    print (f'{filename} is blank')
            else:
                print (f'{filename} has already been imported')
        return importList

        pass
    #get import data from all data lakes
    def get_import_data_from_all_datalakes(self):
        # loop through list of locations
        curr_data_lake_data = []
        for loc in self._dl_locations:
            curr_data_lake_data = self.grab_import_data_from_datalake (loc)
        return curr_data_lake_data


    ###!!
    def import_datafiles_to_db (self, importList):
        print (f'{len(importList)} files to import')
        for import_data in importList:
            print (f'{import_data._filename} importing')
            #print (import_data._data)
            # for common type token we will import the same way as new tokens, then handle all other columns differently


            #extract the fields required for "token" into a new dataframe
            # after this insert to db we need the ids that corespond to each token
            #returns a data frame containing tokenid and tokenname

            #we need to transform this data first
            basic_token_data_df = import_data._data
            #transform basic_token_data
            imported_tokens_df = self.import_token_data (basic_token_data_df)

            #extract the fields required for the "other" tables into a new dataframe
            #this will neeed a new dataframe containing the token id from the above


            #finally insert import file to database!!!!


        pass

    # import basic information, works for new and common
    def import_token_data (self, data):
        #returns a data frame containing tokenid and tokenname 
        token_info = self.extract_token_info_from_df (data)
        # import token_info to db
        #print (token_info)
        # query and retireve tokenid for each token imported

        #remove dupes from the dataframe
        dupes_df = token_info[token_info.duplicated(subset=['Token Name'])]
        if not dupes_df.empty:
            print ('Dupes found on data sheet')
            print (dupes_df)
            token_info=token_info.drop_duplicates(subset=['Token Name'], keep='first')

        #query the database and return all tokens by name
        all_tokens_from_db_df = self.get_tokens_from_db_query()

        #dedupe
    
        #all_tokens_from_db_df = all_tokens_from_db_df.rename (columns={"TokenName": "Token Name"})
        dedupe_df = pd.concat ([token_info ['Token Name'], all_tokens_from_db_df['TokenName']])
        dedupe_df = dedupe_df.to_frame('Token Name')
        dedupe_df = dedupe_df.drop_duplicates(subset=['Token Name'], keep=False)
        deduped_list = list(set(dedupe_df['Token Name']))
        #print (len(dedupe_df))
        #print (token_info)
        tokens_to_import_df = token_info[token_info['Token Name'].isin(deduped_list)]

        # insert tokens and get list containing tokenids
        imported_tokens_df = self.import_token_dataframe (tokens_to_import_df)



        #print (data)
        #pass
    # import extra information
    
    # pandas transformations
    def extract_info_from_importfile_to_object (self, file):
        df = pd.read_csv(file)
        if df.empty:
            return None
        dtype = dataType.unknown
        num_col = len(df.columns)
        if num_col == new_col:
            dtype = dataType.new
        if num_col == common_col:
            dtype = dataType.common

        return data_import_file (file, df, dtype)

    #extract basic token info from df
    def extract_token_info_from_df (self, data):
        t_data = data[basic_token_headers]
        #transform fields?
        return t_data
    # queries!
    def check_if_file_has_been_imported_query (self, file) -> bool:
        query = f"select * from NewTokenImportLogFiles where filename='{file}'"
        df = db_conn.fetch_query (query)

        if df.empty:
            return False
        else:
            return True
    def get_tokens_from_db_query (self):
        query = f'select TokenId,TokenName from token'
        df = db_conn.fetch_query (query)
        return df
    def import_token_dataframe (self, tokens_to_import_df):
        #we are here 20210705!!!
        #return list containing tokenid and token
        pass
    
if __main__():
    db_conn = database_query_handle (db_config)
    db_conn.connect()
    tokenImporter = TokenImporter (db_conn, data_lake_location)
    #print (db_conn.fetch_single_query('select * from Token'))
    tokenImporter.run()
    pass
else:
    print ('Failed to init')






#TODO####################


#REMEMBER common and new tokens are very different so handle accordingly !!!!!!!!!
 
# finish basic data import. We need to just write the insert function, retireve the tokenid from db and add to df (tokenid, tokenname)
# link df (tokeid, tokenname) to exchangedf (tokenname, exchanges) and insert to exhcnage map and exhcange

