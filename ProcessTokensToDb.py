#Author Kevin Singh
# Upload New / Common tokens to the database
import pandas as pd 
import time
import pymssql
import os
from Logger import Logger

pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.precision', 5)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
# SQL commands
#insert


#const
TokenTypeIdNone_CONST = 3
TokenStatusIdNone_CONST = 4



"""
INSERT INTO [dbo].[Token]
           ([TokenTypeId]
           ,[TokenStatusId]
           ,[NewtokenImportLogFiles]
           ,[TokenName]
           ,[Symbol]
           ,[Address]
           ,[AddressUrl]
           ,[TimeStamp]
           ,[Supply]
           ,[Holders]
           ,[Transfers]
           ,[Decimals]
           ,[CurrentPrice])
     VALUES
           (<TokenTypeId, int,>
           ,<TokenStatusId, int,>
           ,NewtokenImportLogFiles>
           ,<TokenName, nvarchar(max),>
           ,<Symbol, nvarchar(max),>
           ,<Address, nvarchar(max),>
           ,<AddressUrl, nvarchar(max),>
           ,<TimeStamp, nvarchar(max),>
           ,<Supply, float,>
           ,<Holders, float,>
           ,<Transfers, float,>
           ,<Decimals, float,>
           ,<CurrentPrice, float,>) 
"""


# look for CSVs and add to database
# track what CSVs need to be loaded
# store on DB all imported sheets?
# if imported sheet does not exist, import
# how to import?
# when importing we must de dupe
    # pull all (new) tokens from db
    # check against current list and import the ones that are missing
class DbConfiguration:
    server_addr = ''
    user = ''
    password = ''
    db_name = ''



class NewTokensImporter:
    CONFIG_FILE_LOCATION = ''
    def __init__ (self, db_config, path_to_csv_dir):
        self.path_to_csv_dir = path_to_csv_dir 
        self.db_config = db_config
    def RunImporter(self):
        print ('Importer started...')
        self.ConnectToDatabase()
        isWatingMessageTrigger = True

        while (1):
            #time.sleep(60 - time.time() % 60)
            time.sleep(2)
            #print ('looking for new files')
            ToImportFiles = self.FindNextFilesToProcess ()
            if len(ToImportFiles) > 0:
                isWatingMessageTrigger = True
            #Go through the list and import each file
            self.ImportDataFileToDb(ToImportFiles)

            if isWatingMessageTrigger:
                print ('Sleeping...')
            isWatingMessageTrigger = False

    def ImportDataFileToDb (self, ToImportFiles):
        #Import
        for filename in ToImportFiles:
            fullpath  = f'{self.path_to_csv_dir}/{filename}'
            dataframe = pd.read_csv (f'{fullpath}')


            #modify type to string [Supply]
      
            dataframe["Max Supply"] = dataframe["Max Supply"].fillna(0).astype('int64')

            if self.CreateImportLogForFile (fullpath, filename):
                print (f'{filename} was imported successfully!')
                #if successful import the data. We need to DEDUPE!
                ImportId = self.GetNewImportIdFromDb (filename)
                if ImportId == 0:
                    print ('Error finding import')
                else:
                    self.ImportDataFrameToDb (ImportId, dataframe)
            else:
                print (f'{filename} failed to import...')




    def ImportDataFrameToDb (self, NewImportId, dataframe):
        # import data processes !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!-----------------------}}}}}>>>
        tokens_to_import = []


        for index , data in dataframe.iterrows():
           TokenIdFromDb = self.FetchTokenIdFromDb(data[1]) 
           if TokenIdFromDb == 0:
               tokens_to_import.append (data)
           #else:
               #print (f'TokenId {TokenIdFromDb} -> Token {data[1]} exist') 
        
    
        print (f'Importing {len(tokens_to_import)} to database')

        # we need to check if its common or not


        for token_row in tokens_to_import:
            tokenId = self.AddBasicTokenPandaFrameToDb (token_row, NewImportId)
            if tokenId != 0:
                # return the newly created tokenid from above
                #print (f'{token_row[1]} was imported')
                if token_row[10] == 'Common':
                    #this is a common token. Please process
                    print ('This is a common token please process')
                    # we will need to create a function to add the custom fields not on new tokens
                    # we will need to add a function to add exchange information
                    # parse the exchange string on panda frame into database
            else:
                print (f'{token_row[1]} failed to import')

        

    def AddBasicTokenPandaFrameToDb (self, frame, NewImportId):

        tokentype = self.FindTokenTypeIdOnDb (frame[4])
        tokenstatus = self.FindTokenStatusIdOnDb (frame[10])
        if tokentype == None:
            tokentype = TokenTypeIdNone_CONST
        if tokenstatus == None:
            tokenstatus = TokenStatusIdNone_CONST
        query = f"""
            INSERT INTO [dbo].[Token]
            ([TokenTypeId]
           ,[TokenStatusId]
           ,[NewtokenImportLogFilesId]
           ,[TokenName]
           ,[Symbol]
           ,[Address]
           ,[AddressUrl]
           ,[TimeStamp]
           ,[Supply]
           ,[Holders]
           ,[Transfers]
           ,[Decimals]
           ,[CurrentPrice])
     VALUES
            ({tokentype}, {tokenstatus} , {NewImportId}, '{frame[1]}','', '{frame[2]}','{frame[3]}', '{frame[5]}', '{frame[6]}','{frame[7]}','{frame[8]}','{frame[9]}', 0)
                """
        if self.RunExecuteCommand (query):
            return self.FetchTokenIdFromDb (frame[1])
        else:
            return 0
        
    def FindTokenTypeIdOnDb (self, TokenTypeName):
        query = f"select * from TokenType where TokenTypeName='{TokenTypeName}'"
        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
        except:
            return None
        return row ['TokenTypeId']
    def FindTokenStatusIdOnDb (self, TokenStatusName):
        query = f"select * from TokenStatus where TokenStatusName='{TokenStatusName}'"
        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
        except:
            return None
        return row ['TokenStatusId']
    def FetchAllTokensFromDb (self):
        query = "select * from Token"
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        return row
    def FetchTokenIdFromDb (self, tokenname):
        query = f"select * from Token where tokenname = '{tokenname}'"
        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
        except:
            return 0
        if row == None:
            return 0
        return row ['TokenId'] 
    def GetNewImportIdFromDb (self, filename):
        query = f"select * from NewTokenImportLogFiles where filename='{filename}'"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row == None:
            return 0
        return row['NewtokenImportLogFilesId']

    def RunExecuteCommand (self, query):
        time.sleep (.3)
        try:
            self.cursor.execute(query)
            self.conn.commit()
            return True
        except:
            print (f'Execution error on command {query}')
            return False
    def RunExecuteManyCommand (self, query):
        time.sleep (1)
        try:
            self.cursor.executemany(query)
            self.conn.commit()
            return True
        except:
            print (f'Execution error on command')
            return False
    def CreateImportLogForFile (self, full_path_to_file, filename):
        query = f"insert into NewTokenImportLogFiles (FileName, ImportDate) values ('{filename}', GetDate())"
        if self.RunExecuteCommand (query):
            return True
        else:
            return False
        
        
    def FindNextFilesToProcess (self):
        #find file to process. Grab all files then check against the database if its has been processed
        ToImportList = []
        for filename in os.listdir(self.path_to_csv_dir):
            if not self.CheckIfFileHasBeenImported (filename):
                #import
                print (f'File has not been imported {filename} adding to import queue')
                ToImportList.append (filename)
        return ToImportList
    def CheckIfFileHasBeenImported (self, filename):
        query = f"select * from NewTokenImportLogFiles where filename='{filename}'"
        self.cursor.execute(query)
        self.cursor.fetchall()
        if self.cursor.rowcount == 0:
            return False
        return True
    def ConnectToDatabase (self):
        self.conn = pymssql.connect(self.db_config.server_addr, self.db_config.user, self.db_config.password, self.db_config.db_name,100000)
        self.cursor = self.conn.cursor(as_dict=True)
        print ('Connected!')
    def __del__ (self):
        print ('Cleanup...')
        self.conn.close()


def GenerateDbConfiguration (location):
    config = DbConfiguration ()
    print ('Loading configuration info into object...')
    # open config file
    # load config data into config object
    
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


#logger = Logger('.','NewTokensToDB.log')

db_config = GenerateDbConfiguration ('db_config.config')

importer = NewTokensImporter (db_config, './csv')


importer.RunImporter()
