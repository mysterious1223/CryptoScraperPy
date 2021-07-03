#Author: Kevin Singh

import pandas as pd
import pymssql
import os
import sys
import time


from enum import Enum 

db_config = None
data_lake_location = []



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
        pass
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
                importList.append (self.extract_info_from_importfile_to_object(full_file_name))
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
            print (import_data._data)
            # for common type token we will import the same way as new tokens, then handle all other columns differently

            #extract the fields required for "token" into a new dataframe
            #extract the fields required for the "other" tables into a new dataframe
        pass

    # import basic information, works for new and common
    def import_basic_portion (self, data):
        pass
    # import extra information
    
    # pandas transformations
    def extract_info_from_importfile_to_object (self, file):
        df = pd.read_csv(file)
        dtype = dataType.unknown
        num_col = len(df.columns)
        if num_col == new_col:
            dtype = dataType.new
        if num_col == common_col:
            dtype = dataType.common

        return data_import_file (file, df, dtype)

    # queries!
    def check_if_file_has_been_imported_query (self, file) -> bool:
        query = f"select * from NewTokenImportLogFiles where filename='{file}'"
        df = db_conn.fetch_query (query)

        if df.empty:
            return False
        else:
            return True
    
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

# finish class data_import_file
    # All fields that would be on a single import field. Including the data itself should be here
    # make it modular, incase we add new fields to the import sheet
# finish class TokenImporter
    #finish the grab functions
    #create the main loop
    #create the function that will generate the data_import_file obj
    #log to db if file has been imported
    #do data transformations to data
    #we should have two datasets. make sure there is a link between the exchange and its token
    #remove a dupe if record exist on db. use an upsert
    #push to db
