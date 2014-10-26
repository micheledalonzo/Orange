# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import OrangeGbl as gL
import OrangeDb
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import time
me = "STD"
try:
    rc = gL.ParseArgs()
    #---------------------------------------------- M A I N ------------------------------------------------
    # apri connessione e cursori, carica keywords in memoria
    rc = gL.OpenDb()
    if not rc:
        print("Error in opening db")
        return False   
    runid = gL.Restart(me)
    if not gL.restart:
        gL.RunId = gL.RunIdCreate(me)
    rc = gL.SetLogger(me, gL.RunId, gL.restart)      
    if not rc:
        gL.log(gL.ERROR, "SetLogger errato")
     
    gL.log(gL.INFO, gL.Args)
    gL.N_Ass = 0
    # creo la tabella in memoria
    rc = gL.dbCreateMemTableMemAsset()
    rc = gL.CopyAssetInMemory()
    gL.cMySql.execute("Select * from QAddress order by name")
    rows = gL.cMySql.fetchall()
    gL.T_Ass = len(rows)
    msg=('RUN %s: STDIZE %s Assets' % (gL.RunId, gL.T_Ass))
    gL.log(gL.INFO, msg)
    t1 = time.clock()
    for row in rows:
        gL.N_Ass = gL.N_Ass + 1
        Asset = row['asset']
        # "ALL" rifai tutti daccapo
        msg=('Asset %s - %s(%s)' % (Asset, gL.N_Ass, gL.T_Ass))
        gL.log(gL.INFO, msg)
        AssetMatch, AssetRef = gL.StdAsset(Asset, "ALL") 
        if AssetMatch is False: # is evita che 0 sia interpretato come false
            gL.log(gL.WARNING, "Asset " + str(Asset) + str(AssetMatch) + str(AssetRef))
            continue
        # creo o aggiorno il record in AAsset a partire da SourceAsseId corrente
        AAsset = gL.dbAAsset(Asset, AssetMatch, AssetRef)   
        # cerco le info sull'asset in Google        
        #gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)
        #if gL.N_Ass > 100:
        #    break
    # chiudi DB
    gL.CloseConnectionMySql()
    gL.CloseConnectionSqlite()
    t2 = time.clock()
    print(round(t2-t1, 3))
    sys.exit(0)

except Exception as err:
    gL.log(gL.ERROR, err)
    sys.exit(12)
