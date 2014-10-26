# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# Gambero Rosso 1 Forchetta 
# Gambero Rosso 2 Forchette
# Gambero Rosso 3 Forchette
# Espresso 1 Cappello
# Espresso 2 Cappelli
# Espresso 3 Cappelli
# Michelin 1 Stella
# Michelin 2 Stelle
# Michelin 3 Stelle
# Veronelli 1 Stella
# Veronelli 2 Stelle
# Veronelli 3 Stelle
# Gambero Rosso 1 Gambero
# Gambero Rosso 2 Gamberi
# Gambero Rosso 3 Gamberi
# Guida Michelin 1 Forchetta
# Guida Michelin 2 Forchette
# Guida Michelin 3 Forchette
# Guida Michelin 4 Forchette
# Guida Michelin 5 Forchette
# Guida Touring
# empty list = False
#

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
me = "PRS"

def ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name):
    if gL.trace: gL.log(gL.DEBUG)   
    # parse delle singole pagine degli asset
    gL.dbQueueStatus("START", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che inizio
    Asset = gL.ParseContent(country, assettype, source, starturl, asseturl, name)                           
    if Asset:  # se tutto ok
        gL.dbQueueStatus("END", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
    else:
        return False                                           
    return True

def RestartParse():
    if gL.trace: gL.log(gL.DEBUG)   
    try:        
        gL.cMySql.execute("SELECT * from QParseRestart ORDER BY rand()")
        check = gL.cMySql.fetchall()   # l'ultima
        gL.T_Ass = len(check)
        msg=('RUN %s: RESTART PARSING, %s Assets IN QUEUE' % (gL.RunId, gL.T_Ass))                
        gL.log(gL.INFO, msg)
        if gL.T_Ass > 0:      
            for row in check:
                gL.assetbaseurl = row['DriveBaseUrl']  # il baseurl per la tipologia di asset
                language        = row['CountryLanguage']  # lingua                                                               
                gL.currency     = row['CountryCurr']
                gL.SourceBaseUrl= row['SourceBaseUrl']    
                source          = row['Source']
                name            = row['Nome']
                #sourcename      = row['SourceName']                
                #assettypename   = row['AssetTypeName']                
                assettype       = row['AssetType']
                country         = row['Country']
                starturl        = row['StartUrl']
                asseturl        = row['AssetUrl']
                pageurl         = row['PageUrl']        
                SetLocaleString = row['SetLocaleString']        
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
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        # ---------------- messaggio con totale asset da esaminare
        gL.cMySql.execute("SELECT * FROM Qqueue")  
        rows = gL.cMySql.fetchall()
        gL.T_Ass = str(len(rows))       
        msg=('RUN %s: PARSING %s Assets' % (gL.RunId, gL.T_Ass))
        gL.log(gL.INFO, msg)

        gL.N_Ass = 0
        for row in rows:                        
            gL.assetbaseurl = row['DriveBaseUrl']  # il baseurl per la tipologia di asset
            language        = row['CountryLanguage']  # lingua                                                               
            gL.currency     = row['CountryCurr']
            gL.SourceBaseUrl= row['SourceBaseUrl']    
            source          = row['Source']
            name            = row['Nome']
            sourcename      = row['SourceName']                
            assettypename   = row['AssetTypeName']                
            assettype       = row['AssetType']
            country         = row['Country']
            starturl        = row['StartUrl']
            asseturl        = row['AssetUrl']
            pageurl         = row['PageUrl']        
            SetLocaleString = row['SetLocaleString']        
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
        #gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        #gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)   
        rc = gL.OpenDb()
        if not rc:
            print("Error in opening db")
            return False
        gL.restart == False
        runid = gL.Restart(me)
        rc = gL.SetLogger(me, gL.RunId, gL.restart)            
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
                rc = gL.UpdDriveRun("END")                
    
        # run normale
        if gL.restart == False:
            # controllo la tabella Drive e la leggo
            gL.cMySql.execute("SELECT * FROM QDrive ORDER BY rand()")
            gL.Drive = gL.cMySql.fetchall()
            if len(gL.Drive) == 0:
                print("Nessun run da eseguire")
            else:
                gL.RunId = gL.RunIdCreate(me)
                rc = gL.SetLogger(me, gL.RunId, gL.restart)
                if not rc:
                    print("SetLogger errato")
                rc = gL.RunIdStatus("START")  
                if not rc:
                    gL.log(gL.ERROR, "RunId errato")        
                gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))    
                rc = gL.RunInit()    
                if not rc:
                    gL.log(gL.ERROR, "RunInit errato")        
                
                rc = NormalParse()              # -------------------RUN------------------------------------
                if not rc:   
                    return False
                else:
                    #chiudo le tabelle dei run
                    rc = gL.RunIdStatus("END")                
            
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
