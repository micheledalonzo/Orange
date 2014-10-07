        from lxml import html
import collections
import pypyodbc
import datetime
import time
import re
import sys
import locale
# import jabba_webkit as jw
from urllib.parse import urlparse
import OrangeGbl as gL
work_queue = collections.deque()
me = "GOO"

def ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name):
    # parse delle singole pagine degli asset
    gL.dbQueueStatus("START", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che inizio
    Asset = gL.ParseContent(country, assettype, source, starturl, asseturl, name)                                                                      
    if Asset:  # se tutto ok
        gL.cSql.commit()
        gL.dbQueueStatus("END", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
        gL.cSql.commit()
    return True

def RestartParse():
    try:        
        gL.cSql.execute("SELECT * from QParseRestart ORDER BY rnd(queueid)")
        check = gL.cSql.fetchall()   # l'ultima
        gL.T_Ass = len(check)
        msg=('RUN %s: RESTART PARSING, %s Assets IN QUEUE' % (gL.RunId, gL.T_Ass))                
        gL.log(gL.INFO, msg)
        if gL.T_Ass > 0:      
            for row in check:
                gL.assetbaseurl = row['drivebaseurl']  # il baseurl per la tipologia di asset
                language        = row['countrylanguage']  # lingua                                                               
                gL.currency     = row['countrycurr']
                gL.sourcebaseurl= row['sourcebaseurl']    
                source          = row['source']
                name            = row['nome']
                sourcename      = row['sourcename']                
                assettypename   = row['assettypename']                
                assettype       = row['assettype']
                country         = row['country']
                starturl        = row['starturl']
                asseturl        = row['asseturl']
                pageurl         = row['pageurl']        
                SetLocaleString = row['setlocalestring']        
                # gestione della lingua per l'interpretazione delle date
                if not SetLocaleString:
                    gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                    return False
                locale.setlocale(locale.LC_TIME, SetLocaleString)  
                gL.N_Ass = gL.N_Ass + 1
                rc = ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name)
                if not rc:
                    return False
        
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def NormalParse():

    try:
        # ---------------- messaggio con totale asset da esaminare
        gL.cSql.execute("SELECT * FROM QQueue")
        rows = gL.cSql.fetchall()
        gL.T_Ass = str(len(rows))       
        msg=('RUN %s: PARSING %s Assets' % (gL.RunId, gL.T_Ass))
        gL.log(gL.INFO, msg)

        gL.N_Ass = 0
        for row in rows:                        
            gL.assetbaseurl = row['drivebaseurl']  # il baseurl per la tipologia di asset
            language        = row['countrylanguage']  # lingua                                                               
            gL.currency     = row['countrycurr']
            gL.sourcebaseurl= row['sourcebaseurl']    
            source          = row['source']
            name            = row['nome']
            sourcename      = row['sourcename']                
            assettypename   = row['assettypename']                
            assettype       = row['assettype']
            country         = row['country']
            starturl        = row['starturl']
            asseturl        = row['asseturl']
            pageurl         = row['pageurl']        
            SetLocaleString = row['setlocalestring']        
            # gestione della lingua per l'interpretazione delle date
            if not SetLocaleString:
                gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                return False
            locale.setlocale(locale.LC_TIME, SetLocaleString)  
            gL.N_Ass = gL.N_Ass + 1
            if gL.testrun and gL.testurl:      # se e' un giro di test, esamino solo url indicato
                if asseturl != gL.testurl:
                    continue
            #msg ="%s - %s" % ("PARSE", asseturl)
            #gL.log(gL.INFO, msg)
            rc = ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name)                               
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def Main():
    try:
        rc = gL.ParseArgs()
        #---------------------------------------------- M A I N ------------------------------------------------
        # apri connessione e cursori, carica keywords in memoria
        gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)   
        gL.restart == False
        runid = gL.Restart()
        rc = gL.SetLogger("GOO", gL.RunId, gL.restart)            
        gL.log(gL.INFO, gL.Args)

        if  gL.restart == True:
            gL.RunId = runid    
            rc = gL.RunInit()
            if not rc:
                gL.log(gL.ERROR, "RunInit errato")                    
                return False
            
            rc = RestartParse()         # -----------------RESTART----------------------------------
            if not rc:   
                return False
            else:
                #chiudo le tabelle dei run
                rc = gL.RunIdStatus("END")
                rc = gL.sql_UpdDriveRun("END")
                gL.cSql.commit()                                    
    
        # run normale
        if gL.restart == False:
            # controllo la tabella Drive e la leggo
            gL.cSql.execute("SELECT * FROM QDrive ORDER BY rnd(starturlid)")
            gL.Drive = gL.cSql.fetchall()
            if len(gL.Drive) == 0:
                print("Nessun run da eseguire")
            else:
                gL.RunId = gL.RunIdCreate()
                rc = gL.RunIdStatus("START")  
                if not rc:
                    gL.log(gL.ERROR, "RunId errato")        
                rc = gL.SetLogger("GOO", gL.RunId, gL.restart)
                if not rc:
                    gL.log(gL.ERROR, "SetLogger errato")        
                gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))    
                rc = gL.RunInit()    
                if not rc:
                    gL.log(gL.ERROR, "RunInit errato")        
                
                rc = NormalParse()              # -------------------RUN------------------------------------
                if not rc:   
                    gL.log(gL.ERROR, "Run terminato in modo errato")        
                else:
                    #chiudo le tabelle dei run
                    rc = gL.RunIdStatus("END")                
                    gL.cSql.commit()    
            
        # chiudi DB
        gL.CloseConnectionMySql()
        gL.CloseConnectionSqlite()
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)        
        return False
    
if __name__ == "__main__":
       
    rc = Main()
    if not rc:
        gL.log(gL.ERROR, "Run terminato in modo errato")        
        sys.exit(12)
    else:
        gL.log(gL.INFO, "Run terminato in modo corretto")        
        sys.exit(0)
# per ogni asset una call a Google Places
        #gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)
        #gL.cSql.commit()
