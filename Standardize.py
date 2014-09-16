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

try:
    rc = gL.ParseArgs()
    #---------------------------------------------- M A I N ------------------------------------------------
    # apri connessione e cursori, carica keywords in memoria
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)   
    runid = gL.Restart()
    rc = gL.SetLogger(gL.RunId, gL.restart)      
    if not rc:
        gL.log(gL.ERROR, "SetLogger errato")
     
    gL.log(gL.INFO, gL.Args)
    gL.N_Ass = 0
    # creo la tabella in memoria                
    rc = gL.dbCreateMemTableMemAsset()
    rc = gL.CopyAssetInMemory()
    gL.cSql.execute("Select * from QAddress order by name")
    rows = gL.cSql.fetchall()
    gL.T_Ass = len(rows)
    msg=('RUN %s: STDIZE %s Assets' % (gL.RunId, gL.T_Ass))
    gL.log(gL.INFO, gL.Args)
    t1 = time.clock()
    for row in rows:
        gL.N_Ass = gL.N_Ass + 1
        Asset = row['asset']
        # "ALL" rifai tutti daccapo
        msg=('Asset %s(%s)' % (gL.N_Ass, gL.T_Ass))
        gL.log(gL.INFO, gL.Args)
        AssetMatch, AssetRef = gL.StdAsset(Asset, "ALL") 
        if AssetMatch is False: # is evita che 0 sia interpretato come false
            gL.log(gL.WARNING, "Asset " + str(Asset) + str(AssetMatch) + str(AssetRef))
            continue
        # creo o aggiorno il record in AAsset a partire da SourceAsseId corrente
        AAsset = gL.dbAAsset(Asset, AssetMatch, AssetRef)   
        # cerco le info sull'asset in Google        
        #gAsset = gL.ParseGooglePlacesMain(Asset, AAsset)
        gL.cSql.commit()
    t2 = time.clock()
    print(round(t2-t1, 3))
    sys.exit(0)

except Exception as err:
    gL.log(gL.ERROR, err)
    sys.exit(12)