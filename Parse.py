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

def RestartParse():
    try:        
        gL.cSql.execute("SELECT * from QQueue")
        check = gL.cSql.fetchall()   # l'ultima
        if not check:
            pass
        else:
            # stampo i parametri di esecuzione
            gL.T_Ass = str(len(check))
            msg=('RESTART PARSE: %s ASSETS IN QUEUE' % (gL.T_Ass))
            gL.log(gL.INFO, msg)
            for log in check:
                gL.assetbaseurl = log['drivebaseurl']  # il baseurl per la tipologia di asset
                language        = log['countrylanguage']  # lingua                                                               
                gL.currency     = log['countrycurr']
                gL.sourcebaseurl= log['sourcebaseurl']    
                source          = log['source']
                name            = log['nome']
                sourcename      = log['sourcename']                
                assettypename   = log['assettypename']                
                assettype       = log['assettype']
                country         = log['country']
                starturl        = log['starturl']
                asseturl        = log['asseturl']
                pageurl         = log['ultimodipageurl']        
                SetLocaleString = log['setlocalestring']        
                # gestione della lingua per l'interpretazione delle date
                if not SetLocaleString:
                    gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                    return False
                locale.setlocale(locale.LC_TIME, SetLocaleString)  
                gL.N_Ass = gL.N_Ass + 1
                msg=('RUN %s: PARSING %s Assets' % (gL.RunId, gL.T_Ass))
                gL.log(gL.ERROR, err)
                rc = ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name)
                if not rc:
                    return False
        
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def ParseAsset(country, assettype, source, starturl, pageurl, asseturl, name):
    # parse delle singole pagine degli asset
    gL.dbQueueStatus("START", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che inizio
    Asset = gL.ParseContent(country, assettype, source, starturl, asseturl, name)                                                                      
    if Asset:  # se tutto ok
        gL.cSql.commit()
        AssetMatch, AssetRef = gL.StdAsset(Asset)   # controllo se esiste già un asset simile
        if AssetMatch is False: # is evita che 0 sia interpretato come false
            gL.log(gL.WARNING, "StdAsset ha resituito False", asseturl)
            return True
        AAsset = gL.dbAAsset(Asset, AssetMatch, AssetRef)   # creo il record in Asset a partire da SourceAsseId corrente con riferimento al suo simile oppure lo aggiorno
        gL.dbQueueStatus("END", country, assettype, source, starturl, pageurl, asseturl) # scrivo nella coda che ho finito
        # per ogni asset una call a Google Places
        gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)

    return True

def Parse():

    try:
        # FASE DI PARSING
        # ---------------- leggo dalla coda i link creati con il run corrente, in ordine casuale         
        msg='RUN: %s: PARSING %s' % (gL.RunId, drive['starturl'])
        gL.log(gL.INFO, msg)            
        gL.cSql.execute("SELECT * FROM Queue where starturl = ? ORDER BY rnd(queueid)", ([drive['starturl']]))
        rows = gL.cSql.fetchall()

        gL.T_Ass = str(len(rows))       
        msg=('RUN %s: PARSING %s Assets' % (gL.RunId, gL.T_Ass))
        gL.log(gL.INFO, msg)

        gL.N_Ass = 0
        for row in rows:            
            gL.N_Ass = gL.N_Ass + 1              
            pageurl  = row['pageurl']
            assettype= row['assettype']
            asseturl = row['asseturl'] 
            starturl = row['starturl'] 
            name     = row['nome']
            country  = row['country']
            source = row['source']
            #if gL.testrun:      # se è un giro di test, esamino solo url indicato
            #    if asseturl != gL.testurl:
            #        continue
            msg ="%s - %s" % ("PARSE", asseturl)
            gL.log(gL.INFO, msg)
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
        if  gL.restart == True:
            gL.RunId = runid    
            rc = gL.RunInit()
            if not rc:
                gL.log(gL.ERROR, "RunInit errato")                    
                return False
            rc = gL.SetLogger(gL.RunId, gL.restart)            
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
                gL.log(gL.WARNING, "Nessun run da eseguire")
            else:
                gL.RunId = gL.RunIdCreate()
                rc = gL.RunIdStatus("START")  
                if not rc:
                    gL.log(gL.ERROR, "RunId errato")        
                rc = gL.SetLogger(gL.RunId, gL.restart)
                if not rc:
                    gL.log(gL.ERROR, "SetLogger errato")        
                gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))    
                rc = gL.RunInit()    
                if not rc:
                    gL.log(gL.ERROR, "RunInit errato")        
                rc = Parse()              # -------------------RUN------------------------------------
                if not rc:   
                    gL.log(gL.ERROR, "Run terminato in modo errato")        
                else:
                    #chiudo le tabelle dei run
                    rc = gL.RunIdStatus("END")                
                    gL.cSql.commit()    
            
        # chiudi DB
        gL.CloseConnectionMySql()
        gL.CloseConnectionSqlite()

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
