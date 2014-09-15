# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import OrangeGbl as gL
import OrangeDb
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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

    
    # creo la tabella in memoria                
    rc = gL.dbCreateMemTableMemAsset()
    rc = gL.CopyAssetInMemory()
    gL.cSql.execute("Select * from QAddress order by name")
    rows = gL.cSql.fetchall()
    for row in rows:
        Asset = row['asset']
        # "ALL" rifai tutti daccapo
        AssetMatch, AssetRef = gL.StdAsset(Asset, "ALL") 
        if AssetMatch is False: # is evita che 0 sia interpretato come false
            gL.log(gL.WARNING, "Asset " + str(Asset) + str(AssetMatch) + str(AssetRef))
            continue
        gL.log(gL.INFO, "Asset " + str(Asset) + " AssetMatch " + str(AssetMatch) + " AssetRef " + str(AssetRef))
        AAsset = gL.dbAAsset(Asset, AssetMatch, AssetRef)   
        # creo o aggiorno il record in AAsset a partire da SourceAsseId corrente
    sys.exit(0)

except Exception as err:
    gL.log(gL.ERROR, err)
    sys.exit(12)