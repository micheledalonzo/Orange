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
import OrangeGbl as gL
work_queue = collections.deque()
me = "PAG"

def BuildAssetList(country, assettype, source, starturl, pageurl, runlogid):    
    if gL.trace: gL.log(gL.DEBUG)   
    try:

        #   inizia da starturl e interpreta le pagine di lista costruendo la coda degli asset da esaminare
        work_queue.append((pageurl, ""))

        while len(work_queue):
            pageurl, newpage = work_queue.popleft()            
            msg ="%s - %s" % ("PAGINATE", pageurl)
            gL.log(gL.INFO, msg)
            if newpage == '':
                rc, page = gL.ReadPage(pageurl)
            else:
                rc = 0
                page = newpage
            if rc == 0 and page is not None:
                # inserisce la pagina da leggere nel runlog
                rc = gL.PagesStatus("START", country, assettype, source, starturl, pageurl)                                
                # legge la pagina lista, legge i link alle pagine degli asset e li inserisce nella queue
                rc = gL.BuildQueue(country, assettype, source, starturl, pageurl, page)
                # aggiorna il log del run con la data di fine esame della pagina
                gL.PagesStatus("END", country, assettype, source, starturl, pageurl)
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
    if gL.trace: gL.log(gL.DEBUG)   
    try:        
        
        gL.cMySql.execute("SELECT * from QPagesRestart")   # rileggo tutti i record della tabella pages che non sono stati completati e li processo nuovamente
        check = gL.cMySql.fetchall() 
        gL.T_Ass = len(check)
        msg=('RUN %s: RESTART PAGINATE, %s Assets IN QUEUE' % (gL.RunId, gL.T_Ass))                
        gL.log(gL.INFO, msg)
        for log in check:
            gL.assetbaseurl = log['DriveBaseUrl']  
            language        = log['CountryLanguage']  
            gL.currency     = log['CountryCurr']
            gL.SourceBaseUrl= log['SourceBaseUrl']    
            source          = log['Source']
            #sourcename      = log['SourceName']                
            #assettypename   = log['AssetTypeName']                
            assettype       = log['AssetType']
            country         = log['Country']
            starturl        = log['StartUrl']
            pageurl         = log['PageUrl']        
            SetLocaleString = log['SetLocaleString']        
            # gestione della lingua per l'interpretazione delle date
            if not SetLocaleString:
                gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                return False
            locale.setlocale(locale.LC_TIME, SetLocaleString)  
              
            # stampo i parametri di esecuzione
            msg=('RESTART PAGINAZIONE: RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s RESTART: %s' % (gL.RunId, source, assettype, country, gL.restart))
            gL.log(gL.INFO, msg)

            rc, page = gL.ReadPage(pageurl)  # rileggo l'ultima pagina con data di start massima
            if rc == 0 and page is not None:
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

def NormalPaginate():
    if gL.trace: gL.log(gL.DEBUG)   
    try:    
        gL.log(gL.INFO, "Inserimento Starturl")
        for drive in gL.Drive:              # inserisco gli starturl nel run
            country = drive['Country']  
            source = drive['Source']
            assettype = drive['AssetType']
            language = drive['CountryLanguage']     
            starturl = drive['StartUrl']     
            pageurl = starturl            
            SetLocaleString = drive['SetLocaleString']
            # gestione della lingua per l'interpretazione delle date
            if not SetLocaleString:          
                gL.log(gL.ERROR, "SetLocaleString non settata in QDrive")
                return False
            locale.setlocale(locale.LC_TIME, SetLocaleString)  
        
            # cancello e ricreo la coda, ma solo per le righe dipendenti dallo starturl            
            gL.cMySql.execute("Delete from queue where source = %s and AssetType = %s and Country = %s and StartUrl = %s", (source, assettype, country, starturl))           
            gL.cMySql.execute("Delete from pages where source = %s and AssetType = %s and Country = %s and StartUrl = %s", (source, assettype, country, starturl))
           
            # metto in tabella Pages tutti gli starturl che devo fare            
            rc = gL.PagesCreate(source, assettype, country, starturl, pageurl)    
        
        for drive in gL.Drive:     # leggo ogni record del drive e ricostruisco la coda 
            gL.assetbaseurl = drive['DriveBaseUrl']  # il baseurl per la tipologia di asset
            language = drive['CountryLanguage']  # lingua
            country = drive['Country']  # paese
            source = drive['Source']
            assettype = drive['AssetType']
            sourcename = drive['SourceName']
            gL.currency = drive['CountryCurr']
            assettypename = drive['AssetTypeName']
            rundate = drive['RunDate']
            rundate_end = drive['RunDate_End']
            starturl = drive['StartUrl']     
            pageurl = starturl            
            gL.SourceBaseUrl = drive['SourceBaseUrl']                        

            msg=('RUN: %s SOURCE: %s ASSET: %s COUNTRY: %s STARTURL: %s' % (gL.RunId, sourcename, assettypename, country, starturl))
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
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        rc = gL.ParseArgs()
        #---------------------------------------------- M A I N ------------------------------------------------
        # apri connessione e cursori, carica keywords in memoria
        rc = gL.OpenDb()
        if not rc:
            print("Error in opening db")
            return False
        gL.restart == False
        runid = gL.Restart(me)
        if  gL.restart == True:            
            gL.RunId = runid            
            rc = gL.SetLogger(me, gL.RunId, gL.restart)     
            gL.log(gL.INFO, gL.Args)       
            rc = gL.RunInit()
            if not rc:
                gL.log(gL.ERROR, "RunInit errato")                    
                return False
            rc = RestartPaginate()         # -----------------RESTART----------------------------------
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
                rc = gL.SetLogger("PGN", gL.RunId, gL.restart)
                if not rc:
                    print("SetLogger errato")
                rc = gL.RunIdStatus("START")  
                if not rc:
                    gL.log(gL.ERROR, "RunId errato")        
                gL.log(gL.WARNING, "Proxy:"+str(gL.Useproxy))    
                rc = gL.RunInit()    
                if not rc:
                    gL.log(gL.ERROR, "RunInit errato")        
                rc = NormalPaginate()              # -------------------RUN------------------------------------
                if not rc:   
                    gL.log(gL.ERROR, "Run terminato in modo errato")        
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
    if gL.trace: gL.log(gL.DEBUG)   
    rc = Main()
    if not rc:
        gL.log(gL.ERROR, "Run terminato in modo errato")        
        sys.exit(12)
    else:
        gL.log(gL.INFO, "Run terminato in modo corretto")        
        sys.exit(0)
