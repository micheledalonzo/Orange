# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python

# tutti gli accessi SQL 
import sqlite3
import pypyodbc
import OrangeGbl as gL

def OpenConnectionMySql(dsn):
    if not gL.MySql:
        gL.MySql = pypyodbc.connect(dsn)
        gL.cSql = gL.MySql.cursor()
    return gL.MySql, gL.cSql

def OpenConnectionSqlite():
    if not gL.SqLite:
        gL.SqLite = sqlite3.connect(':memory:')
        gL.cLite = gL.SqLite.cursor()
    
    return gL.SqLite, gL.cLite

def CloseConnectionMySql():
    
    if gL.MySql:
        gL.MySql.close()
    return True

def LoadProxyList():
    try:
        gL.cSql.execute("Select * from RunProxies where Active = ?", ([gL.YES]) )
        proxies = gL.cSql.fetchall()
        if len(proxies) == 0:       
            return False
        for proxy in proxies:
            gL.Proxies.append(proxy[0])
        return True
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False

def CloseConnectionSqlite():
    if gL.SqLite:
        gL.SqLite.close()
    return True

def UpdDriveRun(startend):
    try:
        if startend == "START":
            gL.cSql.execute("Update Drive set RunDate = ? where active = True", ([gL.RunDate]))
        if startend == "END":        
            gL.cSql.execute("Update Drive set RunDate_end = ? where active = True", ([gL.SetNow()]))    
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False

def RunIdStatus(startend):
    try:
        if startend == "START":
            gL.cSql.execute("Update Run set Start = ? where RunId = ? ", (gL.SetNow(), gL.RunId)) 
            
        if startend == "END":
            gL.cSql.execute("Update Run set End = ? where RunId = ? ", (gL.SetNow(), gL.RunId)) 
            
        return True
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False

def RunIdCreate(RunType):
    try:
        runid = 0
        gL.cSql.execute("Insert into Run (Start, RunType) Values (?, ?)", (gL.SetNow(), RunType))
        gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
        run = gL.cSql.fetchone()
        if run is None:
            raise Exception("Get autonum generato con errore")
        runid = run[0]    
        return runid
    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def PagesCreate(source, assettype, country, starturl, pageurl):
    try:
        if pageurl == None or pageurl == '':
            pageurl = starturl    # se c'è gia resetto le date
        a = gL.cSql.execute("Update Pages set Start = ?, End = 0 where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                    (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if a.rowcount == 0:
            # inserisci il record        
            gL.cSql.execute("Insert into Pages(Source, AssetType, Country, StartUrl, Pageurl, RunId) \
                            values (?,?,?,?,?,?)", \
                            (source, assettype, country, starturl, pageurl, gL.RunId))    
        return True
    except Exception as err:
        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)
        return False
    

def PagesStatus(startend, country, assettype, source, starturl, pageurl):
    try:
        if startend == "START":       
            gL.cSql.execute("Update Pages set Start = ?, End = 0 where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                                (gL.SetNow(), source, assettype, country, starturl, pageurl))
        if startend == "END":
            gL.cSql.execute("Update Pages set End   = ? where source = ? and assettype = ? and country = ? and starturl = ? and pageurl = ?", \
                                        (gL.SetNow(), source, assettype, country, starturl, pageurl))
        return True
    except Exception as err:
        gL.log(gL.ERROR, startend, str(source)+ str(assettype) + country + starturl + pageurl)
        gL.log(gL.ERROR, err)
        return False
    

def sql_RestartUrl(country, assettype, source, rundate, starturl="", pageurl=""):
    try:
        # se richiesto il restart prendo l'ultimo record di paginazione creato nel run precedente
        gL.cSql.execute( ("SELECT StartUrl, PageUrl, max(InsertDate) FROM Queue where \
                            country = ? and assetTypeId = ? and Source = ? and RunDate = ? and StartUrl is NOT NULL and PageUrl IS NOT NULL and AssetUrl='' \
                            group by starturl, pageurl order by InsertDate desc"),\
                            (country, assettype, source, rundate) )
        a = gL.cSql.fetchone()    
        if a is not None:
            starturl = a['starturl']
            pageurl = a['pageurl']
            return starturl, pageurl
        else:
            return False
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbAssetTag(Asset, tag, tagname):
     
    try:
        # cancella e riscrive la classificazione dell'asset     
        if len(tag)>0:
            tag = list(set(tag))     # rimuovo duplicati dalla lista        
            #gL.cSql.execute("Delete * from AssetTag where Asset = ? and TagName = ?", (Asset, tagname))
            for i in tag:
                i = gL.StdCar(i)
                if len(i) < 2:
                    continue
                gL.cSql.execute("Select * from AssetTag where Asset=? and TagName=? and Tag=?", (Asset, tagname, i))
                a = gL.cSql.fetchone()
                if a is None:
                    gL.cSql.execute("Insert into AssetTag(Asset, TagName, Tag) Values (?, ?, ?)", (Asset, tagname, i))

        return True

    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def dbAssetReview(Asset, r):
    try:
        if len(r) == 0:
            return True
        gL.cSql.execute("Delete * from AssetReview where Asset = ?", ([Asset]))
        for a in r:
            nreview = int(a[0])
            punt    = int(a[1])
            gL.cSql.execute("Insert into AssetReview(Asset, EvalPoint, EvalNum) Values (?,?,?)", (Asset, punt, nreview))
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbAssetPrice(Asset, PriceList, currency):

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
                gL.cSql.execute("Delete * from AssetPrice where Asset = ? ", ([Asset]))
                gL.cSql.execute("Insert into AssetPrice(Asset, PriceCurrency, PriceFrom, PriceTo, PriceAvg) Values (?, ?, ?, ?, ?)", (Asset, PriceCurr, PriceFrom, PriceTo, PriceAvg))
        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbEnqueue(country, assettype, source, starturl, pageurl, asseturl, name):
    try:    
        # inserisce un url alla coda oppure lo aggiorna con la data del parsing e col numero del run     
        if pageurl is None or pageurl == '':
            pageurl = starturl
        a = gL.cSql.execute("Update queue set Start=0, End=0, InsertDate=?, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                            (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if a.rowcount == 0:
            gL.cSql.execute("Insert into queue(Country, AssetType, Source, StartUrl, PageUrl, AssetUrl, InsertDate, Nome, RunId) \
                                                Values (?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                                                (country, assettype, source, starturl, pageurl, asseturl, gL.SetNow(), name, gL.RunId))

            return True
    except Exception as err:
        gL.log(gL.ERROR, str(source)+ str(assettype) + country + starturl + pageurl + asseturl + name)
        gL.log(gL.ERROR, err)
        return False
    

def dbQueueStatus(startend, country, assettype, source, starturl, pageurl, asseturl):
    try:
        if startend == "START":
            gL.cSql.execute("Update queue set Start=?, End=0, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
        if startend == "END":
            gL.cSql.execute("Update queue set End=?, RunId=? where Country=? and AssetType=? and Source=? and Starturl=? and Pageurl=? and AssetUrl=?", \
                                              (gL.SetNow(), gL.RunId, country, assettype, source, starturl, pageurl, asseturl))
    
    except Exception as err:        
        gL.log(gL.ERROR, (str(source)+ str(assettype) + country + starturl + pageurl + asseturl), err)
        return False
    return True


def dbAssetOpening(Asset, orario):
    try:
        gL.cSql.execute("Delete * from AssetOpening where Asset = ? ", ([Asset]))
        for j in orario:
            x = j[1][:2]+":"+j[1][2:]
            y = j[2][:2]+":"+j[2][2:]
            gL.cSql.execute("Insert into AssetOpening(Asset, WeekDay, OpenFrom, OpenTo) Values (?, ?, ?, ?)", \
                    (Asset, j[0], x, y))   
        return True

    except Exception as err:        
        gL.log(gL.ERROR, err)
        return False


def dbAssettAddress(Asset, AddrList):

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

        gL.cSql.execute("Select * from AssetAddress where Asset = ?", ([Asset]))
        CurAsset = gL.cSql.fetchone()
        if CurAsset is not None:          
            # controlla se ci sono dati cambiati
            if (       AddrStreet != CurAsset['AddrStreet'] or AddrCity        != CurAsset['AddrCity']          \
                    or AddrCounty != CurAsset['AddrCounty'] or AddrZIP         != CurAsset['AddrZIP']           \
                    or AddrPhone  != CurAsset['AddrPhone']  or AddrLat         != CurAsset['AddrLat']           \
                    or AddrLong   != CurAsset['AddrLong']   or AddrRegion      != CurAsset['AddrRegion']        \
                    or AddrPhone1 != CurAsset['AddrPhone1'] or FormattedAddress!= CurAsset['FormattedAddress']  \
                    or Address != CurAsset['Address']    ):
                    gL.cSql.execute("Update AssetAddress set  \
                                                            AddrStreet=?, AddrCity=?, AddrZip=?, AddrCounty=?,                  \
                                                            AddrPhone=?,  AddrPhone1=?, AddrLat=?,  AddrLong=?, AddrWebsite=?,  \
                                                            FormattedAddress=?, AddrRegion=?, AddrValidated=?, Address=?        \
                                                      where Asset=?",  \
                                                          ( AddrStreet,   AddrCity,   AddrZIP,   AddrCounty,   \
                                                            AddrPhone,    AddrPhone1,    AddrLat,    AddrLong,  \
                                                            AddrWebsite,  FormattedAddress, AddrRegion,  AddrValidated, Address, \
                                                            Asset))
            else:
                pass
        else:
            gL.cSql.execute("Insert into AssetAddress(AddrStreet, AddrCity,   AddrZip, AddrCounty,               \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong, AddrWebsite, \
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset)    \
                                                      Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",         \
                                                     (AddrStreet, AddrCity,   AddrZIP,  AddrCounty,            \
                                                      AddrPhone,  AddrPhone1, AddrLat,  AddrLong,  AddrWebsite,\
                                                      FormattedAddress, AddrRegion, AddrValidated, Address, Asset))

    except Exception as err:        
        gL.log(gL.ERROR, "Asset:"+str(Asset), err)
        return False

    return True

def SaveContent(url, content):
    CurContent = ''
    sql = "Select * from AssetContent where Url = '" + url + "'"
    gL.cSql.execute(sql)
    check = gL.cSql.fetchone()
    try:
        if check is not None:
            CurContent = check['content']
            if CurContent != content:
                gL.cSql.execute("Update AssetContent set Content=?, RunId=? where url=?", (content, gL.RunId, url))
        else:
            gL.cSql.execute("Insert into AssetContent(Url, Content, RunId) Values (?, ?, ?)", \
                    (url, content, gL.RunId))

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False
 
    return True

def DumpTabratio(tabratio):
    if len(tabratio) == 0:
        return
    for item in tabratio:\
        #tabratio.append((gblratio, asset['name'], rows[j]['name'], rows[j]['asset'], rows[j]['aasset'], nameratio, streetratio, cityratio, zipratio, webratio, phoneratio ))                  
        gL.cSql.execute("Delete from Debug_TabRatio where Asset = ?", ([item[1]])) 
        break
    for item in tabratio:        
        gL.cSql.execute( "Insert into Debug_TabRatio(Asset, Assetref, AAssetref, Name, Nameref, Gblratio, Nameratio, Streetratio, Cityratio, Zipratio, Webratio, Phoneratio, Nameratio_ratio, Nameratio_partial, Nameratio_set) \
                          Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                         (item[1], item[4],item[5],item[2],item[3],item[0],item[6],item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14]))                            

    return

def DumpNames(Asset, name, NameSimple):
    try:
        gL.cSql.execute("Delete from Debug_Names where Asset = ?", ([Asset]))             
        gL.cSql.execute("Insert into Debug_Names(Asset, Name, Newname) \
                         Values (?, ?, ?)", \
                       ( Asset, name, NameSimple))
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

    return True


def DumpGoogleResults(Asset, name, indirizzo, chk):
    if len(chk) == 0:
        return
    for item in chk:       
        gL.cSql.execute("Delete from Debug_GoogleResults where Asset = ?", ([Asset]))             
        break
    for item in chk:
        gL.cSql.execute("Insert into Debug_GoogleResults(Asset, AssetName, AssetAddress, GblRatio, Nome, Address, NameRatio, StreetRatio) \
                         Values (?, ?, ?, ?, ?, ?, ?, ?)",        \
                       ( Asset, name, indirizzo, item[0], item[2], item[3], item[4], item[5]))      
    return True

def dbAsset(country, assettype, source, name, url, AAsset=0, GooglePid=''):
    
    try:    
        tag = []
        msg = "%s %s(%s) - %s - %s" % ('Asset:', gL.N_Ass, gL.T_Ass, name.encode('utf-8'), url.encode('utf-8'))
        gL.log(gL.INFO, msg)
        if GooglePid == '':
            gL.cSql.execute("Select * from Asset where Url = ?", ([url]))
            CurAsset = gL.cSql.fetchone()
        else:
            gL.cSql.execute("Select * from Asset where GooglePid = ?", ([GooglePid]))
            CurAsset = gL.cSql.fetchone()
       
        if CurAsset is not None:   # se e' gia' presente lo aggiorno
            Asset = int(CurAsset['asset'])       
            gL.cSql.execute("Update Asset set Name=?, Updated=? where Asset=?", (name, gL.SetNow(), Asset))
        else:          # se no lo inserisco
            gL.cSql.execute( "Insert into Asset(Source, AssetType, Country, Url, Name, Created, Updated, Active, GooglePid, AAsset) \
                              Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                            ( source, assettype, country, url, name, gL.RunDate, gL.SetNow(), gL.YES, GooglePid, AAsset))
            gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            a = gL.cSql.fetchone()
            if a is None:
                raise Exception("Get autonum errato")
            Asset = int(a[0])
             
        return Asset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return 0


def dbLastReviewDate(Asset, LastReviewDate):
    try:
        # aggiorna la data di ultima recensione
        gL.cSql.execute("select LastReviewDate from Asset where Asset=?", ([Asset]))
        row = gL.cSql.fetchone()
        if row is None:
            raise Exception("Errore: Asset non trovato")
        CurLastReviewDate = row[0]
        if CurLastReviewDate is None or (CurLastReviewDate < LastReviewDate):
            gL.cSql.execute("Update Asset set LastReviewDate=? where Asset=?", (LastReviewDate, Asset))
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbCreateMemTableMemAsset():
    try:
        cmd_create_table = """CREATE TABLE if not exists
                    MemAsset (
                                Asset            STRING,
                                Country          STRING,
                                Aasset           INTEGER,
                                Name             STRING,
                                Source           INTEGER,
                                NameSimple       STRING,
                                Assettype        INTEGER,
                                AddrStreet       STRING,
                                AddrCity         STRING,
                                AddrZIP          STRING,
                                AddrCounty       STRING,
                                AddrPhone        STRING,
                                AddrWebsite      STRING,
                                AddrRegion       STRING,
                                FormattedAddress STRING,
                                Namesimplified   INTEGER
                            );"""
        gL.SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def dbCreateMemTableKeywords():
    try:
        cmd_create_table = """CREATE TABLE if not exists 
                  keywords (
                            assettype   STRING,
                            language    STRING,
                            keyword     STRING,
                            operatore   STRING,
                            tipologia1  STRING,
                            tipologia2  STRING,
                            tipologia3  STRING,
                            tipologia4  STRING,
                            tipologia5  STRING,
                            replacewith STRING,
                            numwords    INTEGER
        );"""
        gL.SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def dbAAsset(Asset, AssetMatch, AssetRef):
    try:
        if AssetMatch == 0:   # devo inserire me stesso
            gL.cSql.execute("select * from asset where asset = ?", ([Asset]))
             # inserisce asset con info standardizzate     
            gL.cSql.execute("Insert into AAsset (Updated) values (?)" , ([gL.RunDate]))
            gL.cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            lstrec = gL.cSql.fetchone()
            if lstrec is None:
                raise Exception("Errore get autonum")
            AAsset = int(lstrec[0])
            gL.cSql.execute("Update Asset set AAsset=? where Asset=?", (AAsset, Asset))
        else:
            AAsset = AssetRef
            gL.cSql.execute("Update Asset set AAsset=? where Asset=?", (AssetRef, Asset))  # ci metto il record di rif 
        
        return AAsset

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def CopyAssetInMemory():
    
    try:
        gL.log(gL.INFO, "Loading assets....")
        gL.cSql.execute("Select * from QAddress order by Name")
        memassets = gL.cSql.fetchall()
        count = 0
        for asset in memassets:
            count = count + 1
            AAsset      = asset['aasset']
            Asset       = asset['asset']
            Country     = asset['country']
            Source      = asset['source']
            Name        = asset['name']
            NameSimple  = asset['namesimple']
            NameSimplified = asset['namesimplified']
            AddrStreet  = asset['addrstreet']
            AddrCity    = asset['addrcity']
            AddrZIP     = asset['addrzip']
            AddrCounty  = asset['addrcounty']
            AddrPhone   = asset['addrphone']
            AddrWebsite = asset['addrwebsite']
            Assettype   = asset['assettype']
            AddrRegion  = asset['addrregion']
            FormattedAddress =  asset['formattedaddress']
            gL.cLite.execute("insert into MemAsset \
                            (aasset, asset, assettype, country, name, namesimple, namesimplified, addrstreet, addrcity, addrzip, addrcounty, addrphone, addrwebsite, addrregion, formattedaddress, source) \
                                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (AAsset, Asset, Assettype, Country, Name, NameSimple, NameSimplified, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress, Source))
        gL.log(gL.INFO, str(count) + " asset in memory")
        return True
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def CopyKeywordsInMemory():
    
    gL.cSql.execute("Select * from Assetkeywords order by keyword")
    ks = gL.cSql.fetchall()
    for k in ks:
        assettype   = k['assettype']
        language    = k['language']
        keyword     = k['keyword']
        operatore   = k['operatore']
        tipologia1  = k['tipologia1']
        tipologia2  = k['tipologia2']
        tipologia3  = k['tipologia3']
        tipologia4  = k['tipologia4']
        tipologia5  = k['tipologia5']
        replacewith = k['replacewith']
        kwdnumwords = k['kwdnumwords']
        numwords    = len(keyword.split())
        gL.cLite.execute("insert into keywords (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords) values (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords))
    return
