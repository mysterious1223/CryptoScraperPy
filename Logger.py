#logger class
from datetime import datetime
import os
import time
class Logger:
    thisPath = ""
    thisName = ""
    def __init__ (self, _path, name):
        self.thisPath = _path
        self.thisName = name

        self.createFileIfNotExist ()

        print ("[" + str(datetime.today()) + "] " + " Logger created as " + _path + "/" + name)

    def createFileIfNotExist (self):
        f = open (self.thisPath + "/" + self.thisName, 'a')
        f.close()

    def writeLogEntry (self, text):
        f = open (self.thisPath + "/" + self.thisName, 'a')
        f.writelines ("["+str(datetime.today())+"] "+text+ "\n")
        f.close()


#
#L = Logger (os.path.abspath(os.getcwd()), "backup.outlog")

#while True:
 #   time.sleep (2)
  #  print("Kevin was here", end="\r", flush=True)
   # L.writeLogEntry ("Kevin was here")