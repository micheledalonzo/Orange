# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

from lxml import html
import argparse
import collections
import pypyodbc
import datetime
import time
import re
import sys
import locale
# import jabba_webkit as jw
from urllib.parse import urlparse
import rangGbl as gL
work_queue = collections.deque()
   

def BuildAssetList(country, assettype, source, starturl, pageurl, runlogid):    
    try:

        #   inizia da starturl e interpreta le pagine di lista costruendo la coda degli asset da esaminare
        #rc = gL.PagesCreate(source, assettype, country, starturl, pageurl)
        #gL.sql_Queue(country, assettype, source, starturl, pageurl)
        work_queue.append((pageurl, ""))

        while len(work_queue):
            pageurl, newpage = work_queue.popleft()            
            msg ="%s - %s" % ("PAGINATE", pageurl)
            gL.log(gL.INFO, msg)
            if newpage == '':
                page = gL.ReadPage(pageurl)
            else:
                page = newpage
            if page is not None:
                # inserisce la pagina da leggere nel runlog
                rc = gL.PagesStatus("START", country, assettype, source, starturl, pageurl)                                
                # legge la pagina lista, legge i link alle pagine degli asset e li inserisce nella queue
                rc = gL.BuildQueue(country, assettype, source, starturl, pageurl, page)
                # aggiorna il log del run con la data di fine esame della pagina
                gL.PagesStatus("END", country, assettype, source, starturl, pageurl)
                gL.cSql.commit()                
                # legge la prossima pagina lista                
                newpageurl, newpage = gL.ParseNextPage(source, assettype, country, pageurl, page)
                if newpageurl:
                    #gL.sql_Queue(country, assettype, source, starturl, newpageurl)    # inserisce nella coda
                    work_queue.append((newpageurl, newpage))
                    gL.PagesCreate(source, assettype, country, starturl, newpageurl)
            
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
    
    return True

def RestartPaginate():
    try:        
        
        gL.cSql.execute("SELECT * from QRestart")   # rileggo tutti i record della tabella pages che non sono stati completati e li processo nuovamente
        check = gL.cSql.fetchall()   # è il record con la data di start maggiore
        if not check:
            pass
        else:
            for log in check:
                gL.assetbaseurl = log['drivebaseurl']  
                language        = log['countrylanguage']  
                gL.currency     = log['countrycurr']
                gL.sourcebaseurl= log['sourcebaseurl']    
                source          = log['source']
                sourcename      = log['sourcename']                
                assettypename   = log['assettypename']                
                assettype       = log['assettype']
                country         = log['country']
                starturl        = log['starturl']
                pageurl         = log['ultimodipageurl']        
                SetLocaleString = log['setlocalestring']        
                # gestione della lingua per l'interpretazione delle date
                if not SetLocaleString:
                    gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                    return False
                locale.setlocale(locale.LC_TIME, SetLocaleString)  
              
                # stampo i parametri di esecuzione
                msg=('RESTART PAGINAZIONE: RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: BOH RESTART: %s' % (gL.RunId, sourcename, assettypename, country, gL.restart))
                gL.log(gL.INFO, msg)

                page = gL.ReadPage(pageurl)  # rileggo l'ultima pagina con data di start massima
                if page is not None:
                    newpageurl, newpage = gL.ParseNextPage(source, assettype, country, pageurl, page)  # leggo se esiste la prossima pagina 
                    if newpageurl:
                        # ---------------- (ri)costruisco la coda
                        msg=('RUN: %s: PAGINAZIONE' % (gL.RunId))
                        gL.log(gL.INFO, msg)
                        rc = BuildAssetList(country, assettype, source, starturl, pageurl, gL.RunId)      # ricostruisco la coda              
                        if not rc:
                            gL.log(gL.WARNING, "PAGINATE KO")
                            return False
        
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def Paginate():
    try:    
        for drive in gL.Drive:              # inserisco gli starturl nel run
            country = drive['country']  
            source = drive['source']
            assettype = drive['assettype']
            refresh = drive['refresh']
            language = drive['countrylanguage']     
            starturl = drive['starturl']     
            pageurl = starturl        
            SetLocaleString = drive['setlocalestring']
            # gestione della lingua per l'interpretazione delle date
            if not SetLocaleString:          
                gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                return False
            locale.setlocale(locale.LC_TIME, SetLocaleString)  
        
            # se richiesto cancello e ricreo la coda, ma solo per le righe dipendenti dallo starturl            
            gL.cSql.execute("Delete * from queue where source = ? and AssetType = ? and Country = ? and StartUrl = ?", (source, assettype, country, starturl))           
            gL.cSql.execute("Delete * from pages where source = ? and AssetType = ? and Country = ? and StartUrl = ?", (source, assettype, country, starturl))
           
            # metto in tabella Pages tutti gli starturl che devo fare            
            rc = gL.PagesCreate(source, assettype, country, starturl, pageurl)    
            gL.cSql.commit()
        
        for drive in gL.Drive:     # leggo ogni record del drive e ricostruisco la coda 
            gL.assetbaseurl = drive['drivebaseurl']  # il baseurl per la tipologia di asset
            language = drive['countrylanguage']  # lingua
            country = drive['country']  # paese
            source = drive['source']
            assettype = drive['assettype']
            refresh = drive['refresh']
            sourcename = drive['sourcename']
            gL.currency = drive['countrycurr']
            assettypename = drive['assettypename']
            rundate = drive['rundate']
            rundate_end = drive['rundate_end']
            starturl = drive['starturl']     
            pageurl = starturl            
            gL.sourcebaseurl = drive['sourcebaseurl']                        
        
            msg=('RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s REFRESH: %s STARTURL: %s' % (gL.RunId, sourcename, assettypename, country, refresh, starturl))
            gL.log(gL.INFO, msg)
                    # FASE DI PAGINAZIONE
            # ---------------- (ri)costruisco la coda
            msg=('RUN: %s: PAGINAZIONE' % (gL.RunId))
            gL.log(gL.INFO, msg)
            rc = BuildAssetList(country, assettype, source, starturl, pageurl, gL.RunId)      # ricostruisco la coda              
            if not rc:                
                return False    
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
            rc = RestartPaginate()         # -----------------RESTART----------------------------------
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
                rc = Paginate()              # -------------------RUN------------------------------------
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
