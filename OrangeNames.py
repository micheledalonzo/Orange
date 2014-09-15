# -*- coding: cp1252 -*-.
import OrangeGbl as gL
import phonenumbers
#import difflib
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# fanne solo limita, di nomi
limita = 0


def NameSimplify(lang, assettype, nome):
    try:
        # connect to db e crea il cursore
        gL.SqLite, gL.C = gL.OpenConnectionSqlite()
        gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)
        chg    = False
        chgwrd = False
        chgfra = False
        namesimple = ""
        typ = []
        cuc = []
        
        if not nome or not lang:
            gL.log(gL.ERROR, "Errore nella chiamata di NameSimplify")
            return chg, '', ''

        # cerco le kwywords da trattare - frasi
        idxlist=[]; newname=""

        if len(gL.Frasi) == 0:
            sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords > 1 order by numwords desc"
            gL.cLite.execute(sql)
            gL.Frasi = gL.cLite.fetchall()
        for frase in gL.Frasi:
            keyword     = frase[2]
            operatoreF  = frase[3]
            typ1        = frase[4]
            typ2        = frase[5]
            cuc1        = frase[6]
            cuc2        = frase[7]
            cuc3        = frase[8]
            replacewith = frase[9]
            numwords    = frase[7]
                                              
            trovato, chgfra, newname, idxlist = gL.CercaFrase(keyword, nome, operatoreF, replacewith)
            if trovato:
                if typ1 is not None:
                    typ.append(typ1)
                if typ2 is not None:
                    typ.append(typ2)
                if cuc1 is not None:
                    cuc.append(cuc1)
                if cuc2 is not None:
                    cuc.append(cuc2)
                if cuc3 is not None:
                    cuc.append(cuc3)
                #print("Frase trattata:", nome.encode('utf-8'), "trasformata in", operatoreF, newname.encode('utf-8'))
                # mi fermo alla prima trovata
                break
            else:
                newname = nome
        
        # cerco le kwywords da trattare - parole
        if len(gL.Parole) == 0:
            sql = "SELECT * from keywords where language = '" + lang + "' and assettype = " + str(assettype) + " and numwords = 1 "
            gL.cLite.execute(sql)
            gL.Parole = gL.cLite.fetchall()        
       
        tmpname = newname; numdel = 0; y = []; yy = []; 
        yy = tmpname.split()

        for idx, y in enumerate(yy[:]):  # per ogni parola della stringa, : fa una copia della lista
                      
            for parola in gL.Parole:      # per ogni kwd 
                keyword     = parola[2]
                operatoreW  = parola[3]
                xtyp1       = parola[4]
                xtyp2       = parola[5]
                xcuc1       = parola[6]
                xcuc2       = parola[7]
                xcuc3       = parola[8]
                replacew    = parola[9]
                numwords    = parola[7]

                # se ho una frase che deve essere preservata devo saltare le sue parole, i cui indici sono in idxlist
                if trovato and operatoreF == "Keep":
                    if idx in idxlist:
                        continue
                            
                if  y == "'":
                    yy.remove(y)
                    numdel = numdel + 1
                    if xtyp1 is not None:
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    chgwrd   = True
               
                if  keyword == y and operatoreW == "Replace":
                    yy.replace(y, replacew)
                    if xtyp1 is not None:
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    chgwrd   = True

                if  keyword == y and operatoreW == "Delete":
                    yy.remove(y)
                    numdel = numdel + 1
                    chgwrd   = True
                    if xtyp1 is not None:
                        typ.append(xtyp1)
                    if xtyp2 is not None:
                        typ.append(xtyp2)
                    if xcuc1 is not None:
                        cuc.append(xcuc1)
                    if xcuc2 is not None:
                        cuc.append(xcuc2)
                    if xcuc3 is not None:
                        cuc.append(xcuc3)
                    break        
        
        if chgwrd:
            newname = " ".join(yy)
        
        # se ho eliminato tutte le parole del nome ripristino il nome stesso
        if (chgwrd or chgfra) and len(yy) == 0:        
            newname = nome

        if nome != newname:
            #msg = "[NOME MODIFICATO] [" + nome + "] [" + newname + "]"
            chg = True            
    
    except Exception as err:
        gL.log(gL.ERROR, nome)
        gL.log(gL.ERROR, err)

    return chg, newname, typ, cuc


def AllNameSimplify():    
    sql = "SELECT * from MemAsset where NameSimplified = " + str(gL.NO) + " order BY name "
    gL.cLite.execute(sql)
    assets = gL.cLite.fetchall() 
    if not assets:
        return False
    
    for asset in assets:
        tag = []
        asset = asset[1]
        name = str(asset[4])        
        city = asset[7]
        assettype = asset[2]
        country = asset[0]
        # reperisco la lingua corrente
        gL.cSql.execute("select CountryLanguage from Country where country =?", ([country]))
        w = gL.cSql.fetchone()
        lang = w['countrylanguage']
        chg, newname, tag, cuc = NameSimplify(lang, assettype, name)
        if chg:
            gL.cSql.execute("Update Asset set NameSimple = ?, NameSimplified = ? where Asset = ?", (newname, gL.YES, asset))

    return True

def ManageName(name, country, assettype):
    # tabella delle lingue per paese
    CountryLang = {}
    language = CountryLang.get(country) 
    if language is None:
        gL.cSql.execute("select CountryLanguage from T_Country where Country = ?", ([country]))
        row = gL.cSql.fetchone()
        if row:
            language = row['countrylanguage']           
            CountryLang[country] = language
    if language is None:
        gL.log(gL.ERROR, "Lingua non trovata")
        return False
    
    # gestisco il nome e la tipologia del locale definita dal nome    
    NameSimplified = gL.NO; NameSimple = ''
    chg, newname, tag, cuc = gL.NameSimplify(language, assettype, name)
    if chg:
        #print("Frase trattata:", name.encode('utf-8'), "trasformata in", newname.encode('utf-8'))
        NameSimple = newname
        NameSimplified = gL.YES

    return NameSimple, NameSimplified, tag, cuc

def StdAsset(Asset, Mode):

    try:
        tabratio = []
        # il record corrente
        gL.cSql.execute("select Asset, Assettype, Source, AAsset, Country, Name, NameSimple, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress from qaddress where asset =  ?", ([Asset]))
        Curasset = gL.cSql.fetchone() 
        if not Curasset:
            gL.log("ERROR", "Asset non trovato in tabella")
            return False
        if Mode == "NEW":
            if Curasset['aasset'] != 0:   # se e' gia'  stato battezzato non lo esamino di nuovo
                return Asset, Curasset['aasset']
            # tutti i record dello stesso tipo e paese ma differenti source, e che hanno gia  un asset di riferimento (aasset)
        gL.cLite.execute("select * from MemAsset where \
                          Asset <> ? and source <> ? and country = ? and assettype = ? and AAsset <> 0 order by name", (Asset, Curasset['source'], Curasset['country'], Curasset['assettype']))
        rows  = gL.cLite.fetchall()     
        if len(rows) == 0:   # non ce ne sono
            return 0,0   #inserisco l'asset corrente

        for j in range (0, len(rows)):
            asset           = str(rows[j][0])
            country         = str(rows[j][1])
            aasset          = str(rows[j][2])
            name            = str(rows[j][3])
            source          = str(rows[j][4])
            namesimple      = str(rows[j][5])
            assettype       = str(rows[j][6])
            addrstreet      = str(rows[j][7])
            addrcity        = str(rows[j][8])
            addrzip         = str(rows[j][9])  # viene caricata come intero ?
            addrcountry     = str(rows[j][10])
            addrphone       = str(rows[j][11])
            addrwebsite     = str(rows[j][12])
            addrregion      = str(rows[j][13])
            formattedaddress= str(rows[j][14])
            namesimplified  = str(rows[j][15])

            # se hanno esattamente stesso sito web o telefono o indirizzo sono uguali
            if Curasset['addrwebsite'] and addrwebsite and (Curasset['addrwebsite'] == addrwebsite):
                return asset, aasset
            if Curasset['addrphone'] and addrphone and (Curasset['addrphone'] == addrphone):
                return asset, aasset
            if Curasset['addrcity'] and Curasset['addrroute']:   # se c'e' almeno la strada e la citta', se l'indirizzo è uguale sono uguali
                if Curasset['formattedaddress'] and formattedaddress and (Curasset['formattedaddress'] == formattedaddress):
                    return asset, aasset
            # se non hanno lo stesso paese, regione, provincia, salto
            if Curasset['country'] and country and (Curasset['country'] != country):
                continue
            if Curasset['addrregion'] and addrregion and (Curasset['addrregion'] != addrregion):
                continue

            nameratio=nameratio_ratio=nameratio_set=nameratio_partial=0           
            streetratio=streetratio_set=streetratio_partial=streetratio_ratio=0
            cityratio_ratio=cityratio_set=cityratio_partial=cityratio=0             
            webratio=phoneratio=zipratio=0
            # se c'e' uso il nome parziale
            name = cfrname = city = cfrcity = street = cfrstreet = zip = cfrzip = ''
            gblratio = 0; quanti = 0; 
            A = Curasset['namesimplified']
            B = namesimplified
            if A: 
                name = A.title(); 
            else: 
                name = Curasset['name'].title(); 
            if B: 
                cfrname = B.title()
            else: 
                cfrname = name.title()            
            nameratio_ratio = fuzz.ratio(name, cfrname)
            nameratio_partial = fuzz.partial_ratio(name, cfrname)
            nameratio_set = fuzz.token_set_ratio(name, cfrname)
            nameratio = nameratio_set+ nameratio_partial + nameratio_ratio
            if nameratio_partial > 70:
                quanti = quanti + 1
            else:
                continue
                #print(name+","+cfrname+","+str(nameratio)+","+str(fuzz.ratio(name, cfrname))+","+str(fuzz.partial_ratio(name, cfrname))+","+str(fuzz.token_sort_ratio(name, cfrname))+","+str(fuzz.token_set_ratio(name, cfrname)))
            if Curasset['addrcity'] and addrcity:
                city = Curasset['addrcity'].title() 
                cfrcity = addrcity.title()
                cityratio_ratio = fuzz.ratio(city, cfrcity)
                cityratio_partial = fuzz.partial_ratio(city, cfrcity)
                cityratio_set = fuzz.token_set_ratio(city, cfrcity)
                cityratio = cityratio_set + cityratio_partial + cityratio_ratio
                if cityratio > 75:
                    quanti = quanti + 1                
                else:
                    cityratio = 0
            if Curasset['addrstreet'] and addrstreet:
                street = Curasset['addrstreet'].title()             
                cfrstreet = addrstreet.title()                               
                streetratio_ratio = fuzz.ratio(street, cfrstreet)
                streetratio_partial = fuzz.partial_ratio(street, cfrstreet)
                streetratio_set = fuzz.token_set_ratio(street, cfrstreet)
                streetratio = streetratio_set + streetratio_partial + streetratio_ratio
                if streetratio > 75:
                    quanti = quanti + 1 
                else:
                    streetratio = 0 
            if Curasset['website'] and website:
                web = Curasset['website'].title() 
                cfrweb = website.title()                
                webratio = fuzz.ratio(web, cfrweb)
                if webratio > 90:
                    quanti = quanti + 1
                else:
                    webratio = 0
            if Curasset['addrphone'] and addrphone:
                pho = Curasset['addrphone'].title() 
                cfrpho = addrphone.title()                
                phoneratio = fuzz.ratio(pho, cfrpho)
                if phoneratio > 90:
                    quanti = quanti + 1
                else:
                    phoneratio = 0
            if Curasset['addrzip'] and addrzip:
                zip = addrzip.title()
                cfrzip = addrzip.title()
                zipratio = fuzz.ratio(zip, cfrzip)
                if zipratio > 90:
                    quanti = quanti + 1
                else:
                    zipratio = 0
            if nameratio > 100:  # da modificare quando ho capito come fare
                # peso i match 0,6 sufficiente, 
                namepeso = 2
                streetpeso = 1.5
                citypeso = 1
                zippeso = 1
                webpeso = 1
                phonepeso = 1
                gblratio =( ((nameratio     * namepeso) +             \
                             (streetratio   * streetpeso) +           \
                             (cityratio     * citypeso) +             \
                             (zipratio      * zippeso) +              \
                             (webratio      * webpeso) +              \
                             (phoneratio    * phonepeso))             \
                             /
                             (quanti)  )                     
                tabratio.append((gblratio, Curasset['asset'], Curasset['name'], name, asset, aasset, nameratio, streetratio, cityratio, zipratio, webratio, phoneratio, nameratio_ratio, nameratio_partial, nameratio_set))
            
        if len(tabratio) > 0:
            tabratio.sort(reverse=True, key=lambda tup: tup[0])
            if gL.debug:
                gL.DumpTabratio(tabratio)
            if tabratio[0][0] > 400:   # global                
                msg = ("[ASSET MATCH] [%s-%s] [%s-%s] [%s]" % (tabratio[0][3], tabratio[0][1], tabratio[0][4], tabratio[0][2], tabratio[0][0]))
                gL.log(gL.WARNING, msg)
                return tabratio[0][3], tabratio[0][4]  # Asset, AAsset

        return 0,0

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False

def NameInit(country=None, source=None, assettype=None):
    
    # connect to db e crea il cursore
    gL.SqLite, gL.C = gL.OpenConnectionSqlite()
    gL.MySql, gL.Cursor = gL.OpenConnectionMySql(gL.Dsn)
    
    # Create database table in memory
    #gL.dbCreateMemTablemem_asset()
    gL.dbCreateMemTableAssetmatch()
    # popola con i dati
    rc = gL.sql_CopyAssetInMemory(country, source, assettype)    
    rc = gL.dbCreateMemTableKeywords()
    rc = gL.sql_CopyKeywordsInMemory()
    return True

if __name__ == "__main__":    
    rc = NameInit()
    #rc = gL.StdAsset()
    rc = gL.cSql.commit()
    
    #rc = gL.AllNameSimplify()
    #rc = gL.cSql.commit()
