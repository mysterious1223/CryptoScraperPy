import pandas as pd
import pymssql
import sys


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
            return None
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




class data_import_file:
    filename = ''
    filepath = ''


class TokenImporter:
    _db_conn = None
    _dl_locations = [] #data lake locations
    def __init__ (self, db_conn, dl_locations = []):
        self._db_conn = db_conn
        self._dl_locations = dl_locations

        pass
    def grab_csv_from_datalake (self, path):

        pass
    def grab_csv_from_all_datalakes (self) -> bool:
        pass


    
if __main__():
    db_conn = database_query_handle (db_config)
    db_conn.connect()
    tokenImporter = TokenImporter (db_conn, data_lake_location)
    #print (db_conn.fetch_single_query('select * from Token'))

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
