# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import datetime
from pygeocoder import Geocoder
from pygeolib import GeocoderError
import re
import OrangeGbl as gL
import phonenumbers
import inspect
import urllib
import argparse

def ParseArgs():
    if gL.trace: gL.log(gL.DEBUG)   
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', action='store_true', default=False,
                    dest='test',
                    help='Decide se il run e di test, e cambia il DNS del database in DsnTest')
    parser.add_argument('-url', action='store', default='',
                    dest='testurl',
                    help="Esamina solo l'url")
    parser.add_argument('-debug', action='store_true', default='',
                    dest='debug',
                    help="Dump tabelle interne su Db")
    parser.add_argument('-trace', action='store_true', default='',
                    dest='trace',
                    help="Traccia sul log tutte le chiamate alle funzioni")
    parser.add_argument('-resetnames', action='store_true', default='',
                    dest='resetnames',
                    help="Inizializza tutti i nomi standard prima di una nuova standardizzazione dei nomi. Esclusi i nomi modificati a mano")

    args = parser.parse_args()
    if args.test:
        gL.testrun = True
        gL.Dsn = gL.Tst_MsAccDsn
        print("RUN DI TEST!!!!")
    else:
        gL.testrun = False
        gL.Dsn = gL.Prd_MsAccDsn
        print("RUN EFFETTIVO")
    if args.testurl:
        gL.testurl = args.testurl
    if args.debug:
        gL.debug = True
    if args.trace:
        gL.trace = True
    if args.resetnames:
        gL.resetnames = True
    
    gL.Args = args

    return True

def Restart(RunType):
    try:
        if gL.trace: gL.log(gL.DEBUG)   
        gL.restart = False
        # determino se devo restartare - prendo l'ultimo record della tabella run    
        gL.cMySql.execute("SELECT RunId, Start, End FROM Run where RunType = %s GROUP BY RunId, Start, End ORDER BY RunId DESC", ([RunType]))
        check = gL.cMySql.fetchone()
        if check:   # se esiste un record in Run
            runid = check['RunId']
            end   = check['End']
            start = check['Start']
            if end is None or end < start:
                gL.restart = True
                return runid
        return 0

    except Exception as err:
        return False

def RunInit():
    if gL.trace: gL.log(gL.DEBUG)        
    
    try:
        #rc = gL.CreateMemTableKeywords()
        #rc = gL.CopyKeywordsInMemory()
        rc = gL.LoadProxyList()
        if not rc:       
            gL.Useproxy = False        
    
        # fill lista delle funzioni per il trattamento delle pagine
        gL.cMySql.execute("select source, assettype, country, NextPageFn, QueueFn, ParseFn from Drive")
        gL.Funzioni = gL.cMySql.fetchall()
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
    CaratteriVietati = ['#', '(', ')', '/', '.', '-', ';', '"', ',', '|']
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
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        
        test = stringa.split(' - ')   # due numeri di tel separati da trattino
        if len(test) > 1:
            stringa = test[0]
    
        ISO = gL.CountryISO.get(country) 
        if ISO is None:
            gL.cMySql.execute("select CountryIso2 from T_Country where Country = %s", ([country]))
            row = gL.cMySql.fetchone()
            if row:
                ISO = row['CountryIso2']           
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
    if gL.trace: gL.log(gL.DEBUG)   
    try:
        for k in gL.Funzioni:
            if k['source'] == source and k['assettype'] == assettype and k['country'] == country: 
                if tipo == "PARSE":
                    return k['ParseFn']
                if tipo == "QUEUE":
                    return k['QueueFn']
                if tipo == "NEXT":
                    return k['NextPageFn']
    except Exception as err:
        gL.log(gL.ERROR, err)
        return False


def StdZip(stringa):   
    stringa = gL.StdCar(stringa) 
    # formatta ZIP
    return stringa


def xstr(s):
    if s is None:
        return ''
    return str(s)

def OkParam():
    return True


def StdAddress(AddrStreet, AddrZIP, AddrCity, AddrCountry, indirizzo=''):    
    if gL.trace: gL.log(gL.DEBUG)   
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


