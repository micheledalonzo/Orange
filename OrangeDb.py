# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# tutti gli accessi SQL 
import sqlite3
import pypyodbc
import pymysql
import OrangeGbl as gL

def OpenDb():
    try:
        if gL.testrun:
            gL.MsAcc  = pypyodbc.connect(gL.Tst_MsAccDsn)
            gL.MySql  = pymysql.connect(host=gL.Tst_MySqlSvr, port=3306, user=gL.Tst_MySqlUsr, passwd=gL.Tst_MySqlPsw, db=gL.Tst_MySqlDb, use_unicode=True, charset='utf8')    
        else:
            gL.MsAcc  = pypyodbc.connect(gL.Prd_MsAccDsn)
            gL.MySql  = pymysql.connect(host=gL.Prd_MySqlSvr, port=3306, user=gL.Prd_MySqlUsr, passwd=gL.Prd_MySqlPsw, db=gL.Prd_MySqlDb, use_unicode=True, charset='utf8')
    
        gL.cMsAcc = gL.MsAcc.cursor()    
        gL.MySql.autocommit(True)
        gL.cMySql = gL.MySql.cursor(pymysql.cursors.DictCursor)
        #xMySql = MySql.cursor(pymysql.cursors.SSCursor)

        gL.SqLite = sqlite3.connect(':memory:')
        gL.cLite = gL.SqLite.cursor()

        return True
    
    except Exception as err:
        print(err)
        return False


def OpenConnectionMySql(dsn):
    if gL.trace: gL.log(gL.DEBUG)   
    if not gL.MySql:
        gL.MySql = pypyodbc.connect(dsn)
        gL.cMySql = gL.MySql.cursor()
    return gL.MySql, gL.cMySql

def OpenConnectionMsAcc(dsn):
    if gL.trace: gL.log(gL.DEBUG)   
    if not gL.MsAcc:
        gL.MsAcc = pypyodbc.connect(dsn)
        gL.cAcc = gL.MsAcc.cursor()
    return gL.MsAcc, gL.cAcc

def OpenConnectionSqlite():
    if gL.trace: gL.log(gL.DEBUG)   
    if not gL.SqLite:
        gL.SqLite = sqlite3.connect(':memory:')
        gL.cLite = gL.SqLite.cursor()
    
    return gL.SqLite, gL.cLite

def CloseConnectionMySql():
    if gL.trace: gL.log(gL.DEBUG)   
    if gL.MySql:
        gL.MySql.close()
    return True


def LoadProxyList():
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        gL.cMySql.execute("Select * from RunProxies where Active = %s", ([gL.YES]) )
        proxies = gL.cMySql.fetchall()
        if len(proxies) == 0:       
            return False
        for proxy in proxies:
            gL.Proxies.append(proxy[0])
        return True
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False

def CloseConnectionSqlite():
    if gL.trace: gL.log(gL.DEBUG)   
    if gL.SqLite:
        gL.SqLite.close()
    return True

def UpdDriveRun(startend):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if startend == "START":
            gL.cMySql.execute("Update Drive set RunDate = %s where active = True", ([gL.RunDate]))
        if startend == "END":        
            gL.cMySql.execute("Update Drive set RunDate_end = %s where active = True", ([gL.SetNow()]))    
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False

def RunIdStatus(startend):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if startend == "START":
            gL.cMySql.execute("Update Run set Start = %s where RunId = %s ", (gL.SetNow(), gL.RunId)) 
            
        if startend == "END":
            gL.cMySql.execute("Update Run set End = %s where RunId = %s ", (gL.SetNow(), gL.RunId)) 
            
        return True
    except Exception as err:        
        #gL.log(gL.ERROR, err)
        return False

def RunIdCreate(RunType):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        runid = 0
        gL.cMySql.execute("Insert into Run (Start, RunType) Values (%s, %s)", (gL.SetNow(), RunType))                
        run = gL.cMySql.lastrowid  # recupera id autonum generato
        if run is None:
            raise Exception("Get autonum generato con errore")        
        return run
    except Exception as err:        
        return False


def PagesCreate(source, assettype, country, starturl, pageurl):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if pageurl == None or pageurl == '':
            pageurl = starturl    # se c'è gia resetto le date
        a = gL.cMySql.execute("Update Pages set Start = %s, End = 0 where source = %s and assettype = %s and country = %s and starturl = %s and pageurl = %s", \
                                    (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if a == 0:
            # inserisci il record        
            gL.cMySql.execute("Insert into Pages(Source, AssetType, Country, StartUrl, Pageurl, RunId) \
                            values (%s,%s,%s,%s,%s,%s)", \
                            (source, assettype, country, starturl, pageurl, gL.RunId))    
        return True
    except Exception as err:
        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)
        return False
    

def PagesStatus(startend, country, assettype, source, starturl, pageurl):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if startend == "START":       
            gL.cMySql.execute("Update Pages set Start = %s, End = 0 where source = %s and assettype = %s and country = %s and starturl = %s and pageurl = %s", \
                                                (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if startend == "END":
            gL.cMySql.execute("Update Pages set End   = %s where source = %s and assettype = %s and country = %s and starturl = %s and pageurl = %s", \
                                        (gL.SetNow(), source, assettype, country, starturl, pageurl))
        return True
    except Exception as err:
        gL.log(gL.ERROR, startend, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)
        return False
    

def sql_RestartUrl(country, assettype, source, rundate, starturl="", pageurl=""):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        # se richiesto il restart prendo l'ultimo record di paginazione creato nel run precedente
        gL.cMySql.execute( ("SELECT StartUrl, PageUrl, max(InsertDate) FROM Queue where \
                            country = %s and assetTypeId = %s and Source = %s and RunDate = %s and StartUrl is NOT NULL and PageUrl IS NOT NULL and AssetUrl='' \
                            group by starturl, pageurl order by InsertDate desc"),\
                            (country, assettype, source, rundate) )
        a = gL.cMySql.fetchone()    
        if a is not None:
            starturl = a['StartUrl']
            pageurl = a['PageUrl']
            return starturl, pageurl
        else:
            return False
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbAssetTag(Asset, tag, tagname):
    if gL.trace: gL.log(gL.DEBUG)    
    try:
        # cancella e riscrive la classificazione dell'asset     
        if len(tag)>0:
            tag = list(set(tag))     # rimuovo duplicati dalla lista        
            #gL.cMySql.execute("Delete from AssetTag where Asset = %s and TagName = %s", (Asset, tagname))
            for i in tag:
                i = gL.StdCar(i)
                if len(i) < 2:
                    continue
                gL.cMySql.execute("Select * from AssetTag where Asset=%s and TagName=%s and Tag=%s", (Asset, tagname, i))
                a = gL.cMySql.fetchone()
                if a is None:
                    gL.cMySql.execute("Insert into AssetTag(Asset, TagName, Tag) Values (%s, %s, %s)", (Asset, tagname, i))

        return True

    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def dbAssetReview(Asset, r):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if len(r) == 0:
            return True
        gL.cMySql.execute("Delete from AssetReview where Asset = %s", ([Asset]))
        for a in r:
            nreview = int(a[0])
            punt    = int(a[1])
            gL.cMySql.execute("Insert into AssetReview(Asset, EvalPoint, EvalNum) Values (%s,%s,%s)", (Asset, punt, nreview))
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbAssetPrice(Asset, PriceList, currency):
    if gL.trace: gL.log(gL.DEBUG)   
    try:     
        # cancella e riscrive la classificazione dell'asset  
        if len(PriceList)>0:                
            PriceCurr = ""
            PriceFrom = 0
            PriceTo = 0
            PriceAvg = 0
            for i in PriceList:
                if i[0] == 'PriceCurr':
                    PriceCurr = i[1]
                if i[0] == 'PriceFrom':
                    PriceFrom = i[1]
                if i[0] == 'PriceTo':
                    PriceTo = i[1]
                if i[0] == 'PriceAvg':
                    PriceAvg = i[1]
            if PriceCurr == '':
                PriceCurr = currency
            if PriceFrom == 0 and PriceTo == 0 and PriceAvg == 0:
                pass
            else:
                gL.cMySql.execute("Delete from AssetPrice where Asset = %s ", ([Asset]))
                gL.cMySql.execute("Insert into AssetPrice(Asset, PriceCurrency, PriceFrom, PriceTo, PriceAvg) Values (%s, %s, %s, %s, %s)", (Asset, PriceCurr, PriceFrom, PriceTo, PriceAvg))
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbEnqueue(country, assettype, source, starturl, pageurl, asseturl, name):
    if gL.trace: gL.log(gL.DEBUG)   
    try:    
        # inserisce un url alla coda oppure lo aggiorna con la data del parsing e col numero del run     
        if pageurl is None or pageurl == '':
            pageurl = starturl
        a = gL.cMySql.execute("Update queue set Start=0, End=0, InsertDate=%s, RunId=%s where Country=%s and AssetType=%s and Source=%s and Starturl=%s and Pageurl=%s and AssetUrl=%s", \
                                            (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if a == 0:
            gL.cMySql.execute("Insert into queue(Country, AssetType, Source, StartUrl, PageUrl, AssetUrl, InsertDate, Nome, RunId) \
                                                Values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                                                (country, assettype, source, starturl, pageurl, asseturl, gL.SetNow(), name, gL.RunId))

            return True
    except Exception as err:
        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl + asseturl + name)
        gL.log(gL.ERROR, err)
        return False
    

def dbQueueStatus(startend, country, assettype, source, starturl, pageurl, asseturl):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if startend == "START":
            gL.cMySql.execute("Update queue set Start=%s, End=0, RunId=%s where Country=%s and AssetType=%s and Source=%s and Starturl=%s and Pageurl=%s and AssetUrl=%s", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if startend == "END":
            gL.cMySql.execute("Update queue set End=%s, RunId=%s where Country=%s and AssetType=%s and Source=%s and Starturl=%s and Pageurl=%s and AssetUrl=%s", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
    
    except Exception as err:        
        gL.log(gL.ERROR, (str(source)+ str(assettype) + country + starturl + pageurl + asseturl), err)
        return False
    return True


def dbAssetOpening(Asset, orario):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        gL.cMySql.execute("Delete from AssetOpening where Asset = %s ", ([Asset]))
        for j in orario:
            x = j[1][:2]+":"+j[1][2:]
            y = j[2][:2]+":"+j[2][2:]
            gL.cMySql.execute("Insert into AssetOpening(Asset, WeekDay, OpenFrom, OpenTo) Values (%s, %s, %s, %s)", \
                    (Asset, j[0], x, y))   
        return True

    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def dbAssettAddress(Asset, AddrList):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        AddrStreet = ""
        AddrCity = ""
        AddrCounty = ""
        AddrZIP = ""
        AddrPhone = ""
        AddrPhone1 = ""
        AddrWebsite = ""
        AddrLat = 0
        AddrLong = 0
        AddrRegion = ""
        Address = ""
        FormattedAddress = ""
        AddrValidated= ""

        if 'AddrValidated' in AddrList and AddrList['AddrValidated']:
            AddrValidated = AddrList['AddrValidated']
        else:
            AddrValidated = gL.NO           
        if 'Address' in AddrList and AddrList['Address']:
            Address = AddrList['Address']
        if 'AddrStreet' in AddrList and AddrList['AddrStreet']:
            AddrStreet = AddrList['AddrStreet']
        if 'AddrCity' in AddrList and AddrList['AddrCity']:
            AddrCity = AddrList['AddrCity']
        if 'AddrCounty' in AddrList and AddrList['AddrCounty']:
            AddrCounty = AddrList['AddrCounty']
        if 'AddrZIP' in AddrList and AddrList['AddrZIP']:
            AddrZIP = AddrList['AddrZIP']
        if 'AddrPhone' in AddrList and AddrList['AddrPhone']:
            AddrPhone = AddrList['AddrPhone']
        if 'AddrPhone1' in AddrList and AddrList['AddrPhone1']:
            AddrPhone1 = AddrList['AddrPhone1']
        if 'AddrWebsite' in AddrList and AddrList['AddrWebsite']:
            AddrWebsite = AddrList['AddrWebsite'].lower()
        if 'AddrLat' in AddrList and AddrList['AddrLat']:
            AddrLat = AddrList['AddrLat']
        if 'AddrLong' in AddrList and AddrList['AddrLong']:
            AddrLong = AddrList['AddrLong']
        if 'AddrRegion' in AddrList and AddrList['AddrRegion']:
            AddrRegion = AddrList['AddrRegion']
        if 'FormattedAddress' in AddrList and AddrList['FormattedAddress']:
            FormattedAddress = AddrList['FormattedAddress']            
        if 'Address' in AddrList and AddrList['Address']:
            Address = AddrList['Address']            

        gL.cMySql.execute("Select * from AssetAddress where Asset = %s", ([Asset]))
        CurAsset = gL.cMySql.fetchone()
        if CurAsset is not None:          
            # controlla se ci sono dati cambiati
            if (       AddrStreet != CurAsset['AddrStreet'] or AddrCity        != CurAsset['AddrCity']          \
                    or AddrCounty != CurAsset['AddrCounty'] or AddrZIP         != CurAsset['AddrZIP']           \
                    or AddrPhone  != CurAsset['AddrPhone']  or AddrLat         != CurAsset['AddrLat']           \
                    or AddrLong   != CurAsset['AddrLong']   or AddrRegion      != CurAsset['AddrRegion']        \
                    or AddrPhone1 != CurAsset['AddrPhone1'] or FormattedAddress!= CurAsset['FormattedAddress']  \
                    or Address != CurAsset['Address']    ):
                    gL.cMySql.execute("Update AssetAddress set  \
                                                            AddrStreet=%s, AddrCity=%s, AddrZip=%s, AddrCounty=%s,                  \
                                                            AddrPhone=%s,  AddrPhone1=%s, AddrLat=%s,  AddrLong=%s, AddrWebsite=%s,  \
                                                            FormattedAddress=%s, AddrRegion=%s, AddrValidated=%s, Address=%s        \
                                                      where Asset=%s",  \
                                                          ( AddrStreet,   AddrCity,   AddrZIP,   AddrCounty,   \
                                                            AddrPhone,    AddrPhone1,    AddrLat,    AddrLong,  \
                                                            AddrWebsite,  FormattedAddress, AddrRegion,  AddrValidated, Address, \
                                                            Asset))
            else:
                pass
        else:
            gL.cMySql.execute("Insert into AssetAddress(AddrStreet, AddrCity,   AddrZip, AddrCounty,               \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong, AddrWebsite, \
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset)    \
                                                      Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",         \
                                                     (AddrStreet, AddrCity,   AddrZIP,  AddrCounty,            \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong,  AddrWebsite,\
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset))

    except Exception as err:        
        gL.log(gL.ERROR, "Asset:"+str(Asset), err)
        return False

    return True

def SaveContent(url, content):
    if gL.trace: gL.log(gL.DEBUG)   
    CurContent = ''
    sql = "Select * from AssetContent where Url = '" + url + "'"
    gL.cMySql.execute(sql)
    check = gL.cMySql.fetchone()
    try:
        if check is not None:
            CurContent = check['Content']
            if CurContent != content:
                gL.cMySql.execute("Update AssetContent set Content=%s, RunId=%s where url=%s", (content, gL.RunId, url))
        else:
            gL.cMySql.execute("Insert into AssetContent(Url, Content, RunId) Values (%s, %s, %s)", \
                    (url, content, gL.RunId))

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
 
    return True




def DumpGoogleResults(Asset, name, indirizzo, chk):
    if gL.trace: gL.log(gL.DEBUG)   

    if len(chk) == 0:
        return
    for item in chk:       
        gL.cMySql.execute("Delete from Debug_GoogleResults where Asset = %s", ([Asset]))             
        break
    for item in chk:
        gL.cMySql.execute("Insert into Debug_GoogleResults(Asset, AssetName, AssetAddress, GblRatio, Nome, Address, NameRatio, StreetRatio) \
                         Values (%s, %s, %s, %s, %s, %s, %s, %s)",        \
                       ( Asset, name, indirizzo, item[0], item[2], item[3], item[4], item[5]))      
    return True

def dbAsset(country, assettype, source, name, url, AAsset=0, GooglePid=''):
    if gL.trace: gL.log(gL.DEBUG)   
    try:    
        tag = []
        msg = "%s %s(%s) - %s - %s" % ('Asset:', gL.N_Ass, gL.T_Ass, name.encode('utf-8'), url.encode('utf-8'))
        gL.log(gL.INFO, msg)
        if GooglePid == '':
            gL.cMySql.execute("Select * from Asset where Url = %s", ([url]))
            CurAsset = gL.cMySql.fetchone()
        else:
            gL.cMySql.execute("Select * from Asset where GooglePid = %s", ([GooglePid]))
            CurAsset = gL.cMySql.fetchone()
       
        if CurAsset is not None:   # se e' gia' presente lo aggiorno
            Asset = int(CurAsset['Asset'])       
            gL.cMySql.execute("Update Asset set Name=%s, Updated=%s where Asset=%s", (name, gL.SetNow(), Asset))
        else:          # se no lo inserisco
            gL.cMySql.execute( "Insert into Asset(Source, AssetType, Country, Url, Name, Created, Updated, Active, GooglePid, AAsset) \
                              Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                            ( source, assettype, country, url, name, gL.RunDate, gL.SetNow(), gL.YES, GooglePid, AAsset))
            
            a = gL.cMySql.lastrowid()
            if a is None:
                raise Exception("Get autonum errato")
            Asset = int(a[0])
             
        return Asset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return 0


def dbLastReviewDate(Asset, LastReviewDate):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        # aggiorna la data di ultima recensione
        gL.cMySql.execute("select LastReviewDate from Asset where Asset=%s", ([Asset]))
        row = gL.cMySql.fetchone()
        if row is None:
            raise Exception("Errore: Asset non trovato")
        CurLastReviewDate = row['LastReviewDate']
        if CurLastReviewDate is None or (CurLastReviewDate < LastReviewDate):
            gL.cMySql.execute("Update Asset set LastReviewDate=%s where Asset=%s", (LastReviewDate, Asset))
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False




def dbAAsset(Asset, AssetMatch, AssetRef):
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        if AssetMatch == 0:   # devo inserire me stesso
            gL.cMySql.execute("select * from asset where asset = %s", ([Asset]))
             # inserisce asset con info standardizzate     
            gL.cMySql.execute("Insert into AAsset (Updated) values (%s)" , ([gL.RunDate]))
            gL.cMySql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            lstrec = gL.cMySql.fetchone()
            if lstrec is None:
                raise Exception("Errore get autonum")
            AAsset = int(lstrec[0])
            gL.cMySql.execute("Update Asset set AAsset=%s where Asset=%s", (AAsset, Asset))
        else:
            AAsset = AssetRef
            gL.cMySql.execute("Update Asset set AAsset=%s where Asset=%s", (AssetRef, Asset))  # ci metto il record di rif 
        
        return AAsset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False




