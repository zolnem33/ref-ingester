# INGESTING Raw Acuisition Logger Data to PostgreSQL/TimescaleDB
# Version: 0.18BETA - 2022-02-08 14:40
# Author: zome@noc.ac.uk 
# Requirements: Python3.7 (external modules: configparser, shutil, psutil, psycopg2, pytz, twisted, getpass, keyboard)
# r2i-postgres.ini file
# PostgreSQL server version 10+
# changeLog:
# 0.02BETA: Because of the performance issue the DtabaseSize Querys commented out
# 0.03BETA: sensor message with multiple sentences but only one timestamp using the first timestamp
# 0.03BETA: WINCH nmea case added (message with just comma in the end) 
# 0.11BETA: postgreSQL postgres user password request from terminal added (import getpass; getpass.getpass())
# 0.12BETA: savefilename path derived from savefile_folder from ini file
# 0.14BETA: flag columns to the end -> go through list_fields ", flag+'_'+fieldnames[i]+', smallint DEFAULT -10 "
# 0.15BETA: code edited following PEP 8 -- Style Guide for Python Code 
# 0.16BETA: code edited simplified sensornames e.g. posmv_pos just posmv etc.
# 0.17BETA: Postgres password from environment variable instead of getpass (pgroot=os.environ.get('PG_PASSWD')
# 0.18BETA: If the sensorconfig file (columname) fieldNumber or (dataType) ParseAs changed old table renamed to
#           tablename_yyyymmddhhmmss and the table altered with the new columname and/or columntype

__version__ = '0.18BETA'

import nmeacase     # external nmeacase.py in the path or same folder than this script!
import os, sys
import keyboard     # controlled stop if user press 's' key!
import glob
import json
import pprint
import datetime
import time
import shutil
import psutil
import psycopg2 
import zipfile
import getpass
from pytz import timezone
from twisted.internet import task
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from configparser import ConfigParser
from psycopg2 import Error
from psycopg2.extensions import AsIs

class EchoUDP(DatagramProtocol):
    def datagramReceived(self, data, address):
        port = self.transport.getHost()
        self.transport.write(data, address)
        #p = self.transport.logPrefix()
        processData(data,port)
        

def keyCheck(key, arr, default):
    if key in arr.keys():
        return arr[key]
    else:
        return default

def process_message(sensor, messageid, message, tspg):
    # inputs sensorname (like CNAV), messageid (like GPGGA), message like(x,y,z), tspg like(2019-06-20 13:40:04.333) 
    global COUNTER # global COUNTER
    global file_test # sql outputs stored in this file
    global error_log # error file
    global roco # postgreSQL postgres (root) connection object
    global usco # postgreSQL user (rvdas) connection object
    #d = {} # returning dictionary object (for test purpose) 
    
    l_message = message.split(',') # create a comma separated list from message

    # get fieldnames from sensorfile_dict to d_fields and login to InfluxDB
    # empty dictionary for tags 
    h = len(sensorfiles_dict) # how many sensors we have in the dictionary
    dt = {}
    d_fields = {} # the actual message fieldnames value par dictionary
    tags = {} # messageid
    l_parseAs = [] # datatypes list
    l_values = [] # values list


    for i in range (h):
        dt = sensorfiles_dict[i]
        #search sensor "id" in dt
        if sensor == dt[fi_id]:
            # sensor found
            sh = len(dt[fi_sentences])
            # length of the sentences
            for j in range (sh):
                

                sta = dt[fi_sentences][j][fi_talkId] # talkerID like (GP)
                smi = dt[fi_sentences][j][fi_messageId] # messageID like (RMC)
                sm = sta + smi # join them like (GPRMC)
                if messageid == sm:
                    # messageid found in dt[]
                    tags[fi_messageId] = sm # add to messageid tags{}
                    fh = len(dt[fi_sentences][j][fi_field]) # number of field defined
                    l_mh = len(l_message) # number of field in the message.
                    # add the first three field time, sensorid, messageid to d_fileds{} and l_values[] and l_parseAs[]
                    d_fields['time']=tspg
                    l_values.append(tspg)
                    l_parseAs.append('timestamptz')
                    d_fields['sensorid']=sensor
                    l_values.append(sensor)
                    l_parseAs.append('string')
                    d_fields['messageid']=sm
                    l_values.append(sm)
                    l_parseAs.append('string')
                    if l_mh < fh:
                        #more field defined in the sensorfiles than what we got some GSA message
                        # shorter so it's not definetly error  
                        fh = l_mh # number of field in the actual sentence.
                    
                    if l_mh > fh:
                        #less field defined in the sensorfiles than what we got, need to investigate
                        print("\n\n\n********* WARNING! Message longer than fieldNumbers!!! ******\n")

                    for q in range (fh):
                        # go through the fields
                        fieldNumber = dt[fi_sentences][j][fi_field][q][fi_fieldNumber]
                        parseAs = dt[fi_sentences][j][fi_field][q][fi_parseConfig][fi_parseAs]
                        l_parseAs.append(parseAs)

                        #ptest.write("\nfiledNumber={}\n".format(fieldNumber))
                        d_fields[fieldNumber] = l_message[q]
                        l_values.append(l_message[q])    
                        

            ph = len(l_values)    
            tablename = sensor + '_' + messageid # tablename is sensor_messageid like: posmv_pos_gpgga
            tablename = tablename.lower() # convert tablename to lowercase (postgreSQL like this way)
            cu = usco.cursor() # cursor object from the user connection object
            # create the INSERT INTO tablename fieldname part
            qry = "INSERT INTO {} (" # {} part of the string not format specifier in this case!
            for columnid,v in d_fields.items():
                qry = qry + columnid + ', '
            qry = qry + ') VALUES ('
            
            # create INSERT INTO SQL sentence VALUES part.

            for i in range (ph):
                # checking the value and it's format
                # if value ='' and format float value=NULL
                # if value ='' and format integer value=NULL
                # if value = string or text value = 'value' wrapped between single quotes

                if l_parseAs[i] == 'timestamptz':
                    qry = qry + '\'' + str(l_values[i]) + '+00' + '\', '
              

                if l_parseAs[i] == 'integer':
                    # integer case
                    if l_values[i] == '':
                        l_values[i] = 'NULL'
                        qry = qry + str(l_values[i]) + ', '
                    else:
                        qry = qry + str(l_values[i]) + ', '

                
                if l_parseAs[i] == 'float':
                    # float case
                    if l_values[i] == '':
                        l_values[i] = 'NULL'
                        qry = qry + str(l_values[i])+', '
                    else:
                        qry = qry + str(l_values[i])+', '

                if l_parseAs[i] == 'string' or l_parseAs[i] == 'text' or l_parseAs[i] == 'char':
                    # string text case
                    qry = qry + '\'' + l_values[i] + '\', '

            qry = qry + ');'
            qry = qry.replace(', )',')')
            qry = qry.replace('{}',tablename) # replace {} with tablename
            qry = qry.lower()

            print ("SQL QRY={}\n".format(qry))
            cu.execute(qry) # inserting data to tablename table
            #d = point
            file_test.write("{}\n".format(qry))


            COUNTER += 1 # increment global counter
            # test QUERY's commented out...
            '''
            qry="SELECT pg_size_pretty(pg_database_size('DY999'));"
            #print ("qry={}\n".format(qry))
            cu.execute(qry)
            dbsize=cu.fetchone()
            print("database size:{} Ingested:{} rows\n".format(dbsize[0],COUNTER))
            qry="SELECT pg_size_pretty(pg_relation_size('{}'));".format(tablename)
            #print ("qry={}\n".format(qry))
            cu.execute(qry)
            tablesize=cu.fetchone()
            qry="SELECT count(time) from {};".format(tablename)
            #print ("qry={}\n".format(qry))
            cu.execute(qry)
            tablerows=cu.fetchone()
            qry="SELECT min(time) from {};".format(tablename)
            #print ("qry={}\n".format(qry))
            cu.execute(qry)
            tablestartedat=cu.fetchone()
            
            print("table:{} size:{} rows:{} started at:{}\n".format(tablename,tablesize[0],tablerows[0],tablestartedat[0]))
            '''            
            break #break j




    return (qry)

def processData(data, address):
    # processing the UDP datagram
    # data is binary 
    global dt #datetime object
    global file_test
    global error_log
    global tsprev
    global usco
    global roco
    sensor = '' # sensorname init
    port1 = str(address) # portnumber in the address field
    portnumber=0 # init portnumber
    ts = 0 # init timestamp
    #tsprev init to year start
    tsprev = '01/01/2019 00:00:00.000'

    nmea = "None" # init nmea
    a_ts = {}
    a_tsunix = {}
    a_nmea = {}
    a_messageid = {}
    a_message = {}
    ads,portnumber = port1.split('port=',1) # split address and portnumber - quicker than port1=address[1]
    portnumber = portnumber.rstrip(')') # remove the closing bracket
    portint = int(portnumber) # convert to integer
    sensor = portid.get(portint) # get the sensor name based by it's portnumber
       
    print("\nINFO: port={}\tsensor={}\n".format(str(portint),sensor))

    data_utf8 = data.decode('utf-8') # convert data to UTF8
    #split all lines
    #sor = len(data_utf8.split('\n')) # count how many rows
    sorok = data_utf8.split('\n') # split the rows 
    # filter empty string from the list 
    sorok = list(filter(None,sorok)) # remove empty lines

    lensorok = len(sorok) # how many rows in the data
    # open a log file daily
    currentday = dt.day # get current day
    if datetime.datetime.now().day != currentday:
        # new day require new daily log file!
        # close the old one
        file_test.close
        
        dt = datetime.datetime.now() # get the current datetime
        savefilename = savefile_folder+'/r2i-log-%04d-%02d-%02d.txt' % (dt.year, dt.month, dt.day) # setup the new filename
        file_test = open(savefilename, "a+") # open for append if exists and create if not exists.
        



    for i in range (lensorok):
        # exception here if we have no \t eg. the next line just a NMEA $... message without timestamp (like FUGRO_GPS in DY)
        # save timestamp and add to the next line
        
       
        if sorok[i].find('\t') == -1:
            # no TAB mean no timestamp
            # use the previous timestamp tsprev
            #print("\nINFO: no timestamp in {}. {}\n".format(i,sorok[i]))
            if tsprev != '01/01/2019 00:00:00.000':
                sorok[i] = tsprev + '\t' + sorok[i]

            #sorok[i] = tsprev + '\t' + sorok[i]
        

                
        ts,nmea = sorok[i].split('\t',1) # split to timestamp and nmea
        tspg = dmy2ymd(ts) # convert timestampt to postgresql format 
        datetime_naive = datetime.datetime.strptime(ts,timeformat)
        datetime_local = timezone('UTC').localize(datetime_naive)
        timestamp = unix_time_millis(datetime_local) * 1000000
        a_ts[i] = ts
        tsprev = ts
        a_tsunix[i] = timestamp
        a_nmea[i] = nmea
        print ("INFO: sensor={} timestamp={} tsprev={} ts={} tspg={} nmea={}\n".format(sensor,timestamp,tsprev,ts,tspg,nmea))
        print ("INFO: ts={} a_nmea[i]={}\n".format(ts,a_nmea[i]))

        
        # cases nmea first char $ normal NMEA
        # nmea first char t SBE45
        # nmea first char * Magnetometer data
        a_nmea[i] = nmeacase.nmeacase(a_nmea[i], sensor, timestamp)
        #a_nmea[i] = nmeacase.nmea_case(a_nmea[i],sensor,timestamp)
        
        if sensor is None:
            sensor = "UNKNOWN"
        # split message
        messageid,message = a_nmea[i].split(',',1) # split the message messageid is the part before the firs comma
        a_messageid[i] = messageid 
        a_message[i] = message
        # get checksum if exists 
        if message.find('*') != -1:
            ml,checksum = a_message[i].split('*',1)
        else:
            # no * mean no checksum
            ml = message
            checksum = '0'

        
        #
        ml_list = ml.split(',')
        ml_length = len(ml_list)
        
        # Call process message - ingesting data to postgreSQL database
        # INPUT: sensor - sensorname (like CNAV_GPS)),
        # messageid (like GPGGA),
        # ml - message (like 1,2,3),
        # tspg the converted timestamp (like 2019-10-31 12:04:32.045)
        # OUTPUT: the INSERT INTO sentence   
        qry = process_message(sensor, messageid, ml, tspg)
    
    # Add here later to check any message arrived from RVDAS? 
    #     
    return (0)    



        

def dmy2ymd(ts):
    # convert 24/06/2019 12:04:32.344 to 2019-06-24 12:04:32.344
    year = ts[6:10]
    month = ts[3:5]
    day = ts[0:2]
    hpart = ts[11:]
    tspg=year + '-' + month + '-' + day + ' ' + hpart
    return (tspg)



def search_port(sensorId,rvdasId):
    # return the sensor broadcasting portnumber  
    #ptest.write("\nsensorId={}\n".format(sensorId))
    port = 0 # init to 0
    channels_hossz = len(sensorId['channels'])
    for i in range (channels_hossz):
        if sensorId['channels'][i]['id'] == rvdasId :
            port = sensorId['channels'][i]['bcPort']
    return (port)

def load_rvdas_appconfig(location):
    # get the RVDAS appconfig json file from the location folder
    # and create a dictionary  
    # INPUT: folder location 
    # OUTPUT: a dictionary with the json file content.
    rvdas_dict = {} # init dictionary
    rvdas_file = os.path.join(location, 'appconfig.json')
    if os.path.isfile(rvdas_file):
        with open(rvdas_file, 'r') as f:
            rvdas_dict = json.load(f)
    ptest.write(str(rvdas_dict)) # write the loaded dictionary to the test file 
    return (rvdas_dict)

def load_sensorfiles(location):
    # get the sensorfiles from the location folder
    # and create a list of the dictionaries
    # using glob module
    # INPUT: folder location (like /home/rvdas/r2i-run/sensorfiles/dy)
    # OUTPUT: list of the dictionaries
    sf_dict = {} 
    list_sf_dict = []
    s_file = glob.glob(os.path.join(location, '*.json'))
    assert len(s_file) > 0, "Could not find *.json files in provided directory"
    s_file_hossz = len(s_file)
    for i in range(s_file_hossz):
        if os.path.isfile(s_file[i]):
            with open(s_file[i], 'r') as f:
                sf_dict = json.load(f)
                list_sf_dict.append(sf_dict)
    ptest.write("\nlist_sf_dict={}".format(list_sf_dict)) # write the list to the test file
    return (list_sf_dict)

# d_t the actual timestamp from message (ts)
def unix_time_millis(d_t):
    # convert the d_t timestamp to integer
    return int((d_t - epoch).total_seconds() * 1000)



def postgres_connect(host, port, user, password, dbname):
    # connect to postgreSQL database
    # setup the user database and setup the data database and privileges
    # INPUT: host postgreSQL server hostnumber or hostname (like 192.168.62.238 or pgsql.discovery.local),
    # port - portnumber (default: 5432)
    # user - username from the .ini file (like rvdas)
    # password - password
    # dbname - database like (DY107 or JC118)
    # OUTPUT: success - boolean flag
    # roco - admin user connection object
    # usco - user connection object
    global file_test
    global error_log
    
    success = False
    rootUser = 'postgres' # hardcoded admin user name here (default postgres)
    #rootPassword = 'rvdas' # hardcoded admin password here (must be created with psql before )
    rootpassword = os.environ.get('PG_PASSWD')
    #rootPassword = getpass.getpass("Enter {} password here ".format(rootUser))
    # input rootPassword HERE!
    rootDbname = 'postgres' # hardcoded admin database 
    try:
        # connect as admin user to admin database
        roco = psycopg2.connect(host=host, port=port, user=rootUser, password=os.environ.get('PG_PASSWD'), dbname=rootDbname)
        roco.autocommit = True
        curoot = roco.cursor()
        # checking the extensions
        qry = "SELECT * from pg_extension;"
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        resext = curoot.fetchall()
        print("INFO: extensions={}\n".format(resext))
        
        # set timescaledb extension
        qry = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" # create timescaledb extension if not exists
        curoot.execute(qry)
        file_test.write("{}\n".format(qry))
        
        # create user with password
        # query - checking user already exists or not
        qry = "SELECT * FROM pg_user WHERE usename='{}'".format(user)
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        res = curoot.fetchone()
        if res == None:
            # create user with password
            qry = "CREATE USER {} WITH ENCRYPTED PASSWORD '{}';".format(user,password)
            curoot.execute(qry) 
            file_test.write("{}\n".format(qry))
        
            print("INFO: {} - user:{} created\n".format(qry,user))
        else:
            print("INFO: {}\nuser: {} already exists!".format(res, user))
        
        # if not already exists create user database the databasename same than the username
        qry = "SELECT * FROM pg_database WHERE datname = '{}';".format(user)
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        userdatabaseexists = curoot.fetchone()
        if userdatabaseexists == None:
            qry = "CREATE DATABASE \"{}\";".format(user) # double quote for uppercase 
            curoot.execute(qry)
            file_test.write("{}\n".format(qry))
            print("INFO: Database: {} CREATED!".format(user))
        else:
            print("{}\nINFO: database: {} already exists!\n".format(userdatabaseexists, user))
        # if not already exists create dbname database (like DY107)
        qry = "SELECT * FROM pg_database WHERE datname = '{}';".format(dbname)
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        databaseexists = curoot.fetchone()
        if databaseexists == None:
            qry = "CREATE DATABASE \"{}\";".format(dbname)
            file_test.write("{}\n".format(qry))
            curoot.execute(qry)
            print("Database: {} CREATED!".format(dbname))
        else:
            print("{}\ndatabase: {} already exists!\n".format(databaseexists, dbname))
        # give right to user to use dbname database
        qry = "GRANT ALL PRIVILEGES ON DATABASE \"{}\" TO {};".format(dbname, user)
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        # ALTER USER rvdas WITH SUPERUSER;
        qry = "ALTER USER {} WITH SUPERUSER;".format( user)
        file_test.write("{}\n".format(qry))
        curoot.execute(qry)
        
        # initial setup done 
        # create user connection and tableinfo and log table (datetime, event)
        usco = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        usco.autocommit = True
        cu = usco.cursor()
        # create log table if not exists
        qry = "SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = 'logta');"
        file_test.write("{}\n".format(qry))
        cu.execute(qry)
        logtable = cu.fetchone()
        if (str(logtable)) == "(False,)":
            qry = "CREATE TABLE logta (logtime timestamp, event text, number integer);"
            cu.execute(qry)
            file_test.write("{}\n".format(qry))
            print("Table: {} CREATED!".format('logta'))
        else:
            print("{}\ndatabase: {} already exists!\n".format(logtable, 'logta'))
    except (Exception, psycopg2.DatabaseError) as error:
        print ("PostgreSQL ERROR:{}\n".format(error))
        error_log.write("PostgreSQL ERROR:{}\n".format(error))
    finally:
        # return objects roco usco
        success = True

    return (success, roco, usco)




def tableLogInsert(usco,insertText, number):
    # insert inserText event and a number to user logta logtable with the current timestamp
    lc = usco.cursor()
    tt = datetime.datetime.now() # get the actual datetime value
    tt_string = tt.strftime("%Y-%m-%d %H:%M:%S.%f")
    qry = "INSERT INTO logta (\"logtime\",\"event\",\"number\") VALUES('{}','{}','{}');".format(tt_string, insertText, number)
    file_test.write("{}\n".format(qry))
    lc.execute(qry)
    lc.close

def comparecolumns(fn1,ft1,fn2,ft2):
    # compare the table column names and types against the postgres columns and types
    # in a already exists table
    # return (True) if they are the same, return (False) if it's any difference!
    
    # check we have a same number of items fn2 length = 
    #fn1 = fn1.lower()
    #ft1 = ft1.lower()
    
    fn1 = [item.lower() for item in fn1]
    ft1 = [item.lower() for item in ft1]
    
    fn1_l = len(fn1)
    fn2_l = len(fn2)
    ft1_l = len(ft1)
    ft2_l = len(ft2)
    
    #print(f"fn1_l={fn1_l}\nfn2_l={fn2_l}\nft1_l={ft1_l}\nft2_l={ft2_l}\n")

    fn2_vl = ((fn1_l*2)+3)-fn2_l # 0 if same size
    ft2_vl = ((ft1_l*2)+3)-ft2_l # 0 if same size
    #print(f"fn2_vl={fn2_vl}\nft2_vl={ft2_vl}\n")
    
    ft4 = []
    
    #print (f"fn1={fn1}\nfn2={fn2}\nft1={ft1}\nft2={ft2}\n")
    fn3 = fn2[3:len(fn1)+3]
    ft3 = ft2[3:len(ft1)+3]
    #print (f"fn1={fn1}\nfn3={fn3}\nft1={ft1}\nft3={ft3}\n")
    for ii in range (len(ft3)):
        pg = pgtosc(ft3[ii])
        ft4.append(pg)
    #print (f"fn1={fn1}\nfn3={fn3}\nft1={ft1}\nft4={ft4}\n")
    # compare fn1 with fn3
    # compare ft1 with ft4
    fn_diff=[] # list for the differences if any
    ft_diff=[]
    fn_diff = list_diff(fn1, fn3) # call to compare the fieldNumbers
    ft_diff = list_diff(ft1, ft4) # call to compare the types
    #print (f"difference(s) fn_diff={fn_diff} n:{len(fn_diff)}\ndifference(s) ft_diff={ft_diff} n:{len(ft_diff)}\n")    
    
    fndiff_l = len(fn_diff)
    ftdiff_l = len(ft_diff)
    
    if ((fndiff_l == 0) and (ftdiff_l == 0)):
        #print ("both the same!")
        return(True)
    else:
        print ("fndiff_l AND ftdiff_l different!")
        return(False)

    print ("end of function")        
    return(True) # True defaulted for testing now.

def list_diff(list1, list2):
    return (list(list(set(list1)-set(list2)) + list(set(list2)-set(list1))))
     


def initTable(sensor, messageid, list_fields, list_parseAs, usco):
    # create table sensor+messageid in dbname database
    # INPUT: sensor - sensorname (like NMF_SURFMET)
    # messageid - (like HEHDT)
    # list_fields - list contain all the fieldnames
    # list_parseAs - list contain all the datatypes
    # usco - the user connection object 
    # fields time timestamp, sensor varchar(30), messageid varchar(30), list_fields[0] list_parseAs[0], ...
    global COUNTER
    tablename=sensor + '_' + messageid
    tablename = tablename.lower() # tablename must be lowercase because of PostgreSQL
    print ("INFO: tablename={}".format(tablename))
    # check tablename table is exists already?
    # if yes altertable if any changes
    # if no createtable
    cu = usco.cursor()
    information = usco.get_dsn_parameters()
    print ("INFO dsn={}\n".format(information))
    qry = "SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = '{}');".format(tablename)
    file_test.write("{}\n".format(qry))
    cu.execute(qry)
    datatable = cu.fetchone()
    if (str(datatable)) == "(False,)":
        # table not exist
        # create the table and the relevant hypertable
        # set timescaledb extension
        qry = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" # create timescaledb extension if not exists
        cu.execute(qry)
        file_test.write("{}\n".format(qry))

        str_definition = "CREATE TABLE \"{}\" ( time timestamptz, sensorid varchar, messageid varchar, ".format(tablename)
        for i in range (len(list_fields)):
            if i == 0:
                str_field = str(list_fields[i])
                str_parseAs = sctopg(str(list_parseAs[i])).lower()
                str_definition = str_definition+str_field+" "+str_parseAs
            if i > 0:
                str_field = str(list_fields[i])
                str_parseAs = sctopg(str(list_parseAs[i])).lower()
                str_definition = str_definition + ', ' + str_field + " " + str_parseAs
                print ("INFO: i={}\t name={}\ttype={}\n".format(i, list_fields[i], list_parseAs[i]))
        for f in range (len(list_fields)):
            str_field = str(list_fields[f])
            str_definition = str_definition + ", flag_" + str_field + " smallint DEFAULT -10 "
                # flag columns to the end -> go through list_fields ", flag+'_'+fieldnames[i]+', smallint DEFAULT -10 "
        str_definition = str_definition+");" # CREATE TABLE SENTENCE CLOSED
        print ("INFO: defs={}\n".format(str_definition))
        file_test.write("{}\n".format(str_definition))
        cu.execute(str_definition) # creating tables HERE from CODE!!!! 
        logMessage = "Table: {} CREATED!".format(tablename)
        COUNTER += 1
        tableLogInsert(usco, logMessage, COUNTER)
        columnlist = []
        datatypelist = []
        # get the columnlist and the datatype list
        qry = "SELECT * FROM {} LIMIT 0;".format(tablename)
        file_test.write("{}\n".format(qry))
        cu.execute(qry)
        columnlist = [desc.name for desc in cu.description]
        datatypelist = [desc.type_code for desc in cu.description]
        print("INFO: columnlist={}\ndatatypelist={}\n".format(columnlist,datatypelist))
        # create_hypertable HERE!
        #qry = "SELECT create_hypertable('{}','time', if_not_exists=>TRUE);".format(tablename)
        qry = "SELECT create_hypertable('{}','time');".format(tablename)
        cu.execute(qry)
        file_test.write("{}\n".format(qry))
        # add the created table details to tableinfo table
        # tableid int, tablename varchar, sensor varchar, messageid varchar, fields array
        # insert into tableinfo () tableTableinfoInsert(usco,COUNTER,tablename,list_fields,list_parseAs,sensor,messageid)
        
        return(True)
        

    else:
        # if (str(datatable)) ALREADY EXISTS
        print("INFO database: {} already exists!\n".format(tablename))
        # check the fields and ALTER TABLE if neccessary ...
        # fields must be time, sensorId, messageId, field1..N
        # select COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = "sensor+"_"+messageid";
        
        columnlist = []
        datatypelist = []
        qry = "SELECT * FROM {} LIMIT 0;".format(tablename)
        file_test.write("{}\n".format(qry))
        
        cu.execute(qry)
        columnlist = [desc.name for desc in cu.description]
        datatypelist = [desc.type_code for desc in cu.description]
        print("columnlist={}\ndatatypelist={}\n".format(columnlist,datatypelist))
        # convert back to list_field.lower() and list_parseAs.lover()
        # comparecolumns(list_field,list_parseAs,columnlist,datatypelist)
        rs = comparecolumns(list_fields,list_parseAs,columnlist,datatypelist)
        print (f"Columns and types comparision: {rs}\n")
        if rs != True: # 
            # the original table columnames or types are different from the new sensorconfig metadata
            # 1. ALTER TABLE IF EXISTS tablename RENAME TO new_tablename;
            # 3. CREATE TABLE newtablename with the new fields ...
            ###############
            dt1 = datetime.datetime.now()
            dStr = dt1.strftime("%Y%m%d%H%M%S")
            newtablename = tablename+'_'+dStr
            qry = "ALTER TABLE IF EXISTS \"{}\" RENAME TO \"{}\";".format(tablename,newtablename)
            print (f"table rename SQL={qry}\n")
            cu.execute(qry) # execute table rename query
            #### create table 
            str_definition = "CREATE TABLE \"{}\" ( time timestamptz, sensorid varchar, messageid varchar, ".format(tablename)
            for i in range (len(list_fields)):
                if i == 0:
                    str_field = str(list_fields[i])
                    str_parseAs = sctopg(str(list_parseAs[i])).lower()
                    str_definition = str_definition+str_field+" "+str_parseAs
                if i > 0:
                    str_field = str(list_fields[i])
                    str_parseAs = sctopg(str(list_parseAs[i])).lower()
                    str_definition = str_definition + ', ' + str_field + " " + str_parseAs
                    print ("INFO: i={}\t name={}\ttype={}\n".format(i, list_fields[i], list_parseAs[i]))
            for f in range (len(list_fields)):
                str_field = str(list_fields[f])
                str_definition = str_definition + ", flag_" + str_field + " smallint DEFAULT -10 "
                # flag columns to the end -> go through list_fields ", flag+'_'+fieldnames[i]+', smallint DEFAULT -10 "
            str_definition = str_definition+");" # CREATE TABLE SENTENCE CLOSED
            print ("INFO: defs={}\n".format(str_definition))
            file_test.write("{}\n".format(str_definition))
            cu.execute(str_definition) # creating tables HERE from CODE!!!! 
            logMessage = "Table: {} CREATED!".format(tablename)
            COUNTER += 1
            tableLogInsert(usco, logMessage, COUNTER)
            columnlist = []
            datatypelist = []
            # get the columnlist and the datatype list
            qry = "SELECT * FROM {} LIMIT 0;".format(tablename)
            file_test.write("{}\n".format(qry))
            cu.execute(qry) # commentes for test
            columnlist = [desc.name for desc in cu.description]
            datatypelist = [desc.type_code for desc in cu.description]
            print("INFO: columnlist={}\ndatatypelist={}\n".format(columnlist,datatypelist))
            # create_hypertable HERE!
            qry = "SELECT create_hypertable('{}','time', if_not_exists=>TRUE);".format(tablename)
            #qry = "SELECT create_hypertable('{}','time');".format(tablename)
            cu.execute(qry) # uncommented for test
            file_test.write("{}\n".format(qry))
            # add the created table details to tableinfo table
            # tableid int, tablename varchar, sensor varchar, messageid varchar, fields array
            # insert into tableinfo () tableTableinfoInsert(usco,COUNTER,tablename,list_fields,list_parseAs,sensor,messageid)

     
            
        
        return (True)

        

def sctopg(str):
    # convert datatype to postgresql datatype
    # return converted
    pg = str
    if str == 'float':
        pg = 'double precision'
    elif str == 'string':
        pg = 'text'
    elif str == 'bool':
        pg = 'boolean'
    elif str == 'char':
        pg = 'character varying'
    elif str == 'int':
        pg = 'integer'
    elif str == 'integer':
        pg = 'integer'
    elif str == 'time':
        pg = 'time without time zone'
    elif str == 'date':
        pg = 'date'
    else:
        # store as text
        pg = 'text'


    return(pg)

def pgtosc2(str):
    # convert postgresql datatype name to sensorconfig parseAs name
    # return converted
    pg = str
    if str == 'double precision':
        pg = 'float'
    elif str == 'text':
        pg = 'string'
    elif str == 'boolean':
        pg = 'bool'
    elif str == 'character varying':
        pg = 'char'
    elif str == 'integer':
        pg = 'integer'
    elif str == 'int':
        pg = 'integer'
    elif str == 'time without time zone':
        pg = 'time'
    elif str == 'date':
        pg = 'date'
    else:
        # everything else as text
        pg = 'text'


    return(pg)


    
def pgtosc(OIDcode):
    # convert postgreSQl OIDcode datatype to netcdf type in sensorconfig file
    # return converted
    #str_sc = ''
    if OIDcode == 701:
        pg = 'float'
    elif OIDcode == 25: # check code of text 25
        pg = 'string'
    elif OIDcode == 16:
        pg = 'bool'
    elif OIDcode == 1043:
        pg = 'varchar'
    elif OIDcode == 1042:
        pg = 'char'
    elif OIDcode == 23:
        pg = 'integer'
    elif OIDcode == 1186:
    # changed to interval type from time
        pg = 'interval'
    elif OIDcode == 1082:
        pg = 'date'
    elif OIDcode == 1083:
        pg = 'time'
    elif OIDcode == 21:
        pg = 'smallint'
    elif OIDcode == 20:
        pg = 'bigint'
    elif OIDcode == 700:
        pg = 'real'
    elif OIDcode == 790:
        pg = 'money'
    elif OIDcode == 1266:
        pg = 'time with time zone'
    elif OIDcode == 1114:
        pg = 'timestamp'
    elif OIDcode == 1184:
        pg = 'timestamptz'
    else:
        # not sure just give back text
        pg = 'text'


    return(pg)

def stop_ingester():
    if keyboard.read_key() == 's':
        stopreactor = False
        reactor.stop()
        reactor.callFromThread(reactor.stop)
        return False
    else:
        return True

print ("*************MAIN***************************")



# moduletest
print("psycopg2 version={}".format(psycopg2.__version__))

# global variable to stop
global stopreactor 


#constants

config = ConfigParser()
config.read('ingester.ini')

postgres_host = config.get('r2i','postgres_host')
postgres_port = config.get('r2i','postgres_port')
postgres_user = config.get('r2i','postgres_user')
postgres_password = config.get('r2i','postgres_password')
postgres_dbname = config.get('r2i','postgres_dbname')
sensorfiles_folder = config.get('r2i','sensorfiles_folder')
rvdas_appconfig_folder = config.get('r2i','rvdas_appconfig_folder')
savefile_folder = config.get('r2i', 'savefile_folder')



COUNTER = 0
# global tsprev init
tsprev = '01/01/2019 00:00:00.000'

#timeformat = '%%d/%%m/%%Y %%H:%%M:%%S.%f'
timeformat = '%d/%m/%Y %H:%M:%S.%f'
timeformatpg = '%Y-%m-%d %H:%M:%S.%f'

epoch_naive = datetime.datetime.utcfromtimestamp(0)
epoch = timezone('UTC').localize(epoch_naive)

# fieldnames fi_name
fi_channels = 'channels'
fi_bcPort = 'bcPort'
fi_id = 'id'
fi_sentences = 'sentences'
fi_talkId = 'talkId'
fi_messageId = 'messageId'
fi_field = 'field'
fi_fieldNumber = 'fieldNumber'
fi_parseConfig = 'parseConfig'
fi_parseAs = 'parseAs'
fi_unit = 'unit'

# init lists
list_sensor = []
list_messageid = []

list_fieldNumber = []
list_parseAs = []
list_unit = []
# init dt today datetime for logfile and errorlog name
dt = datetime.datetime.now()
savefilename = savefile_folder+'/r2i-log-%04d-%02d-%02d.txt' % (dt.year, dt.month, dt.day)
errorlog_name = 'r2i-'+postgres_dbname+'-errors.log'
error_log = open(errorlog_name,"a+")
# open logfile and testfile
file_test = open(savefilename,"a+")
ptest = open('codetest.txt','w+')

#CALL postgres_connect(host, port, user, password, dbname)
#output success - boolean, 
success,roco,usco = postgres_connect(postgres_host, postgres_port, postgres_user, postgres_password, postgres_dbname)

if success:
    #sys.exit()
    file_test.write("Server:{}:{} connected user:{} - database={}\n".format(postgres_host, postgres_port, postgres_user, postgres_dbname))
#dbclient created, connected!!!

sensorfiles_dict = load_sensorfiles(sensorfiles_folder) # sensor description dictionary 

rvdas_dict = load_rvdas_appconfig(rvdas_appconfig_folder)

i = 0
print("rvdas-dict={}\n".format(rvdas_dict))
channels_hossz = len(rvdas_dict[fi_channels])
sensor_bcport = []
sensor_id = []
portid = {}
list_bcPort = []


for i in range(channels_hossz):
    sensor_bcport.append(rvdas_dict[fi_channels][i][fi_bcPort])
    list_bcPort.append(rvdas_dict[fi_channels][i][fi_bcPort])
    ptest.write(str(rvdas_dict[fi_channels][i][fi_bcPort]))
    sensor_id.append(rvdas_dict[fi_channels][i][fi_id])
    ptest.write(str(rvdas_dict[fi_channels][i][fi_id]))

sensorfiles_hossz = len(sensorfiles_dict)
ptest.write("sensorfiles_hossz={} channels_hossz={}\n".format(sensorfiles_hossz,channels_hossz))
i = 0
# dictionary {"sensorId":"SHIPS_GYRO", "nmea" : "HEHDT", ""
#print (sensorfiles_dict['sentences'])
# the last sensor is the special RVDAS-TEST 
for i in range (sensorfiles_hossz):
    sentences_hossz = len(sensorfiles_dict[i][fi_sentences])
    tmpid = sensorfiles_dict[i][fi_id]
    port2 = search_port(rvdas_dict,tmpid)
    portid[port2] = tmpid
    ptest.write("\nportid={}\n".format(portid))
    s_id = tmpid
    list_sensor.append(s_id)
    print ("id:{}\t port2:{}".format(sensorfiles_dict[i][fi_id],port2))
    for j in range (sentences_hossz):
        field_hossz = len(sensorfiles_dict[i][fi_sentences][j][fi_field])
        print ("talkId:{} messageId:{} \n".format(sensorfiles_dict[i][fi_sentences][j][fi_talkId],sensorfiles_dict[i][fi_sentences][j]['messageId']))
        ptest.write("talkId:{} messageId:{} \n".format(sensorfiles_dict[i][fi_sentences][j][fi_talkId],sensorfiles_dict[i][fi_sentences][j]['messageId']))
        nmeatmp = sensorfiles_dict[i][fi_sentences][j][fi_talkId]+sensorfiles_dict[i][fi_sentences][j][fi_messageId]
        print("nmea:{}\n".format(nmeatmp))
        list_messageid.append(nmeatmp)
        list_fieldNumber = []
        list_parseAs = []
        list_unit = []
        for q in range (field_hossz):
            fieldnumbers= sensorfiles_dict[i][fi_sentences][j][fi_field][q][fi_fieldNumber]
            parseAs = sensorfiles_dict[i][fi_sentences][j][fi_field][q][fi_parseConfig][fi_parseAs]
            #units = sensorfiles_dict[i][fi_sentences][j][fi_field][q][fi_unit]
            # replace comma to semicolon in units if any...
            #units = units.replace(',',';')
            print("\n{}\t{}\t{}\t{}\n".format(s_id, nmeatmp, parseAs, sensorfiles_dict[i][fi_sentences][j][fi_field][q][fi_fieldNumber]))
            # get all variable and in j call createtable(list_sensor,list_messageid,list_parseAs,list_filedNumbers,usco)    
            #ptest.write("sensorfiles_dict[i][fi_sentences][j][fi_field][q][fi_fieldNumber]={}".format(fieldnumbers))
            list_parseAs.append(parseAs)
            list_fieldNumber.append(fieldnumbers)
            #list_unit.append(units)
        print ("XX sensor={}\tmessageid={}\tfields={}\tfiledtypes={}\tunit={}\n".format(s_id,nmeatmp,list_fieldNumber,list_parseAs,list_unit))
        # func_createtables call here 
        # parameters: sensor, messageid, filedNumber list, parseAs list, unit list 
        # table1 name = sensor+messageid (timestamp, sensor, messageid, fields)
        # 
        if port2 > 0:
            #initTable just run if sensor not in appconfig (port2=0)
            initTable(s_id, nmeatmp, list_fieldNumber, list_parseAs, usco) 

print("\n{}\n{}\n{}\n{}\n{}\n{}\n".format(list_bcPort, list_sensor, list_messageid, list_parseAs, list_fieldNumber, list_unit))
u_bcPort = list (set(list_bcPort))
u_sensor = list (set(list_sensor))
u_messageid = list (set(list_messageid))
u_parseAs = list (set(list_parseAs))
u_fieldNumber = list (set(list_fieldNumber))
u_unit = list (set(list_unit))
print("\n{}\n{}\n{}\n{}\n{}\n{}\n".format(u_bcPort, u_sensor, u_messageid, u_parseAs, u_fieldNumber, u_unit))
print ("{}\n".format(portid))

#sys.exit()
# channels_hossz -1
for i in range (len(rvdas_dict[fi_channels])):
    print(rvdas_dict[fi_channels][i][fi_bcPort])
    l_port = rvdas_dict[fi_channels][i][fi_bcPort]
    reactor.listenUDP(l_port,EchoUDP())

lc = task.LoopingCall(stop_ingester)
lc.start(1) # call every second
reactor.run()
# reactor stopped with ctrl c
ptest.write(f"ingester stopped at {datetime.datetime.now}\n")
ptest.close()

if stop_ingester:
    reactor.stop()

