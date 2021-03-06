# -*- coding: cp1252 -*-.
import logging
from OrangeDb import *
from OrangeFunctions import *
from OrangeParse import *
import difflib
import pypyodbc
import pymysql
import datetime
import re
import inspect
import traceback
import linecache
import logging
import sys
import pprint
import traceback
import sys
import locale
import yaml
locale.setlocale(locale.LC_ALL, '')

# init var
GoogleSource    = 5 # la codifica fissa (in tabella)
N_Ass           = 0
T_Ass           = 0
testrun         = False
debug           = False
trace           = False
Frasi           = []
Drive           = []
Parole          = []
Proxies         = []  # proxy list
Useproxy        = False
CountryISO      = {}
gL.Funzioni     = []
RunDate         = SetNow()
GmapNumcalls    = 0
count           = 0
i               = 0
testurl         = ''
SourceBaseUrl   = ""
assetbaseurl    = ""
MsAcc           = None   # Access
cMsAcc          = None 
MySql           = None   # MySql
cMySql          = None
SqLite          = None   # Sqlite
cLite           = None   # 
RunId           = 0
# database flag YES/NO
YES             = 1
NO              = 0
TRUE            = 1
FALSE           = 0
resetnames      = False
restart         = False
currency        = ""

import yaml
f = open('c:\orange\orange.yaml')
d = yaml.load(f)
Prd_MsAccDsn = d['prd']['MsAccDsn']
Prd_MySqlDb  = d['prd']['MySqlDb']
Prd_MySqlSvr = d['prd']['MySqlSvr']
Prd_MySqlUsr = d['prd']['MySqlUsr']
Prd_MySqlPsw = d['prd']['MySqlPsw']
Tst_MsAccDsn = d['tst']['MsAccDsn']
Tst_MySqlDb  = d['tst']['MySqlDb']
Tst_MySqlSvr = d['tst']['MySqlSvr']
Tst_MySqlUsr = d['tst']['MySqlUsr']
Tst_MySqlPsw = d['tst']['MySqlPsw']
f.close()
  
INFO     = logging.INFO
CRITICAL = logging.CRITICAL
FATAL    = logging.FATAL
DEBUG    = logging.DEBUG
ERROR    = logging.ERROR
CRITICAL = logging.CRITICAL
WARN     = logging.WARN
WARNING  = logging.WARNING

def SetLogger(Typ, RunId, restart):    
    
    logger = logging.getLogger()  # root logger
    if len (logger.handlers) > 0:  # remove all old handlers        
        logger.handlers = []
    
    logger.setLevel(logging.DEBUG)   # default level
     
    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create error file handler and set level to error
    if restart:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+'.err','a', encoding=None, delay="true")
    else:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+'.err','w', encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create debug file handler and set level to debug
    if restart:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+".log","a")
    else:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+".log","w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    gL.log(gL.INFO, 'INIZIO DEL RUN')

    return True



def log(level, *message):
    runmsg = ''
    logger = logging.getLogger()
    if level == DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.debug(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.debug(runmsg)    
        #logging.debug(stack_trace[:-1])

    if level == INFO:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.info(runmsg)    

    if level == WARNING or level == WARN:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.warn(runmsg)
    
    if level == ERROR or level == CRITICAL or level == FATAL:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.error(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.error(runmsg)    
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if exc_type is not None:
            filename = exc_traceback.tb_frame.f_code.co_filename
            lineno = exc_traceback.tb_lineno
            line = linecache.getline(filename, lineno)
            logger.error("--> Riga:%d - %s" % (lineno, line.strip()))
            #for line in pprint.pformat(stack_trace[:-1]).split('\n'):
            #for line in stack_trace:  # per avere tutto lo stack
            #    logging.error(line.replace("\n",""))
        
