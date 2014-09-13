# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import datetime
from pygeocoder import Geocoder
from pygeolib import GeocoderError
import re
import Gbl as gL
import phonenumbers
import inspect
import urllib
import argparse

def ParseArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', action='store_true', default=False,
                    dest='test',
                    help='Decide se il run è di test')
    parser.add_argument('-url', action='store', default='',
                    dest='testurl',
                    help="Esamina solo l'url")

    args = parser.parse_args()
    if args.test:
        gL.testrun = True
        gL.Dsn = gL.DsnTest
        print("RUN DI TEST!!!!")
    else:
        gL.testrun = False
        gL.Dsn = gL.DsnProd
        print("RUN EFFETTIVO")
    if args.testurl:
        gL.testurl = args.testurl
    return True

def Restart():
    gL.restart = False
    # determino se devo restartare - prendo l'ultimo record della tabella run    
    gL.cSql.execute("SELECT RunId, Start, End FROM Run GROUP BY RunId, Start, End ORDER BY RunId DESC")
    check = gL.cSql.fetchone()
    if check:   # se esiste un record in Run
        runid = check['runid']
        end   = check['end']
        start = check['start']
        if end is None or end < start:
            gL.restart = True
            return runid
    return 0

def RunInit():        
    try:

        rc = gL.dbCreateMemTableKeywords()
        rc = gL.sql_CopyKeywordsInMemory()
        rc = gL.LoadProxyList()
        if not rc:       
            gL.Useproxy = False        
    
        # fill lista delle funzioni per il trattamento delle pagine
        gL.cSql.execute("select source, assettype, country, NextPageFn, QueueFn, ParseFn from Drive")
        gL.Funzioni = gL.cSql.fetchall()
        # controllo la congruenza
        if not gL.OkParam():
            return False        

        return True

    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def SetNow():
    # data corrente del run
    wrk = datetime.datetime.now()
    return str(wrk.replace(microsecond = 0))

def StdCar(stringa):
    
    if isinstance(stringa, list):
        clean = stringa[0]
    else:
        clean = stringa
    CaratteriVietati = ['#', '(', ')', '/', '.', '-', ';',  '"']
    for ch in CaratteriVietati:
        if ch in clean:
            clean = clean.replace(ch, "")
    clean = " ".join(clean.split())
    stringa = clean.strip()
    return stringa

def StdName(stringa):
    
    stringa = gL.StdCar(stringa)    
    return stringa.title()

def StdPhone(stringa, country):
    try:
        
        test = stringa.split(' - ')   # due numeri di tel separati da trattino
        if len(test) > 1:
            stringa = test[0]
    
        ISO = gL.CountryISO.get(country) 
        if ISO is None:
            gL.cSql.execute("select CountryISO2 from T_Country where Country = ?", ([country]))
            row = gL.cSql.fetchone()
            if row:
                ISO = row['countryiso2']           
                gL.CountryISO[country] = ISO

        if ISO is None:
            gL.log(gL.ERROR, "Lingua non trovata")
            return False
    except:
        gL.log(gL.ERROR, stringa)
        gL.log(gL.ERROR, err)
        return False, False
    
    # formatta telefono
    try:
        newphone = '' ; newphone1 = '' ; idx = 0
        numeri = phonenumbers.PhoneNumberMatcher(stringa, ISO)
        while numeri.has_next():
            idx = idx + 1
            match = numeri.next()
            #print(phonenumbers.format_number(b.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
            if idx == 1:
                newphone = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                newphone = newphone.replace('(','')
                newphone = newphone.replace(')','')
            if idx == 2:
                #match = phonenumbers.parse(stringa, ISO)
                newphone1 = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                #newphone = phonenumbers.format_number(y, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                newphone1 = newphone1.replace('(','')
                newphone1 = newphone1.replace(')','')    
    except:
        msg ="%s - %s" % ("Phone stdz error", stringa)
        gL.log(gL.ERROR, msg)
        newphone = stringa
        return False, False
    
    return (newphone, newphone1)

def GetFunzione(tipo, source, assettype, country):
    for k in gL.Funzioni:
        if k['source'] == source and k['assettype'] == assettype and k['country'] == country: 
            if tipo == "PARSE":
                return k['parsefn']
            if tipo == "QUEUE":
                return k['queuefn']
            if tipo == "NEXT":
                return k['nextpagefn']
    return False

def StdZip(stringa):   
    stringa = gL.StdCar(stringa) 
    # formatta ZIP
    return stringa

def CercaFrase(frase, stringa, operatore, replacew):
    try:        
        mod = False
        stringa = str(stringa)
        newstringa = stringa
        a = []
        b = []
        idx = []
        out = []
        wrk = []
        a = stringa.split()
        b = frase.split()
        if replacew:
            c = replacew.split()
        maxa = len(a)
        maxb = len(b)
        conta = 0; 
        trovato = False

        if a and b and maxa >= maxb: 
            for i, x in enumerate(a):
                conta = 0
                #idx = a.index(x)
                if x == b[0]:
                    idx.append(i)
                    out.append(x)
                    trovato = True
                    for z in b[1:]:
                        conta = conta + 1
                        if conta+i < maxa:
                            if z != a[conta+i]:
                                trovato = False
                                idx = []
                                out = []
                            else:
                                idx.append(conta+i)
                                out.append(z)
                    if trovato:
                        break

        if operatore == "Keep":
            return trovato, mod, newstringa, idx
    
        if trovato:
            if operatore == "Delete" or operatore == "Replace":
                conta = 0
                for zz in a:
                    if not conta in idx:           
                        wrk.append(zz)
                        mod = True
                    conta = conta + 1
                newstringa = " ".join(wrk)        

            if operatore == "Replace":
                conta = 0
                if idx[0] == 0:
                    ind = 0
                    indrpc = 0
                else:
                    ind = idx[0] + 1
                    indrpc = idx[0]
                if ind > len(wrk):   # se sono alla fine della stringa                
                    for rplc in c:
                        wrk.append(rplc)
                        mod = True
                else:
                    for rplc in reversed(c):
                        wrk.insert(indrpc, rplc)
                        mod = True

                newstringa = " ".join(wrk)

        return trovato, mod, newstringa, idx

    except Exception as err:

        gL.log(gL.ERROR, err)
        return False, False, False, False


def xstr(s):
    
    if s is None:
        return ''
    return str(s)

def OkParam():
    return True


def StdAddress(AddrStreet, AddrZIP, AddrCity, AddrCountry, indirizzo=''):    
    gL.GmapNumcalls = gL.GmapNumcalls + 1
    
    AddrRegion = ''
    AddrLat    = 0
    AddrCounty = ''
    AddrLong   = 0
    FormattedAddress = ''

    if indirizzo == '':
        indirizzo = xstr(AddrStreet) + " " + xstr(AddrZIP) + " " + xstr(AddrCity) + " " + xstr(AddrCountry) 

    try:
        while True:
            results = Geocoder.geocode(indirizzo)
            if results is None:
                msg = "Indirizzo: " + indirizzo + " non trovato"
                gL.log(gL.WARNING, msg)
                return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')

            if results.count > 0:
                result = results[0]   # solo il primo valore ritornato
                AddrCounty = ""
                for component in result.current_data['address_components']:
                    a = component['types']
                    if a:
                        if a[0] == "administrative_area_level_2":                    
                            AddrCounty = component['short_name']     
                            break            
                if result.route and result.street_number:
                    AddrStreet = result.route + " " + result.street_number
                AddrCity = result.locality
                AddrZIP =  result.postal_code            
                if result.coordinates[0]:
                    AddrLat  = result.coordinates[0]
                if result.coordinates[1]:
                    AddrLong = result.coordinates[1]
                if result.administrative_area_level_1:
                    AddrRegion = result.administrative_area_level_1
                if result.formatted_address:
                    FormattedAddress = result.formatted_address
                return True, AddrStreet, AddrCity, AddrZIP, AddrLat, AddrLong, AddrRegion, AddrCounty, FormattedAddress                   
            else:
                return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')
    except GeocoderError as err:   
        if err.status == "ZERO_RESULT":
            indirizzo = nome + " " + indirizzo                               
        return (False, AddrStreet, AddrCity, AddrZIP, 0, 0, '', '', '')

