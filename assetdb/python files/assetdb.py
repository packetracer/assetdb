import sys
import os, glob
import mysql.connector
from mysql.connector import Error
##INIT CONNECTON TO DATABASE##
try:
    connection = mysql.connector.connect(host='localhost',
                             database='python_DB',
                             user='root',
                             password='ROOT_PASSWORD')
    if connection.is_connected():
       cursor = connection.cursor()
       cursor.execute("select database();")
       record = cursor.fetchone()
except Error as e :
    print ("Error while connecting to MySQL", e)
###########################
#Returns ID value from SQL Query
##########################
def getID(record):
        return (str(record).split(',')[0])[1:]
###########################
#INSERTS RECORD IF NOT
#EXIST, THEN RETURN ID
######################3###
def checkID(tbl,col,val):
        sql_insertif_query = "INSERT INTO {0} ({1}) SELECT * FROM (SELECT '{2}') AS tmp WHERE NOT EXISTS ( SELECT {3} from {4} WHERE {5} = '{6}') LIMIT 1;".format(tbl,col,val,col,tbl,col,val)
        cursor.execute(sql_insertif_query)
        result = cursor.execute("SELECT id FROM {0} WHERE {1}='{2}'".format(tbl,col,val))
        record = cursor.fetchone()
        return getID(record)
###########################
#INSERT SERIAL NUMBERS INTO
#DB REFERENCING THE ID OF HOST
###########################
def insertSerial(serial,hostID):
        sql_insert_query = """ INSERT INTO SERIAL
                               (SERIAL,HOSTID) VALUES
(%s,%s)"""
        insert_tuple = (serial,hostID)
        try:
                result = cursor.execute(sql_insert_query,insert_tuple)
                cursor.fetchone()
                connection.commit()
                print "Success - serial " + serial + " added with Host ID: " + hostID
        except mysql.connector.Error as error:
                connection.rollback()
                print("Fail: {}".format(error))
###########################
#INSERT HOST INTO DATABASE
###########################
def insertHost(host):
        sql_insert_query = """ INSERT INTO HOST
                               (facID,host,modelID,versionID) VALUES
(%s,%s,%s,%s)"""
        facID = checkID('FACILITY','facility',host[0])
        modelID = checkID('MODEL','model',host[2])
        versionID = checkID('VERSION','version',host[len(host)-1])
        insert_tuple = (facID,host[1],modelID,versionID)
        try:
                result = cursor.execute(sql_insert_query,insert_tuple)
                cursor.fetchone()
                connection.commit()
                print "Success- host "+host[1]+" created"
                return checkID('HOST','host',host[1])
        except mysql.connector.Error as error:
                connection.rollback()
                print("Fail: {}".format(error))
###########################
#COLLECT LIST OF SERIALS FOR HOST
###########################
def getSerials(host):
        serials = []
        for i in range(3,len(host)-1):
                serials.append(host[i])
        return serials
###########################
#PERFORM SEARCH ON DATABASE GIVEN CONDITIONS
##########################
def checkTable():
        sql_join_query = """ SELECT H.host, F.facility, V.version, M.MODEL, S.SERIAL FROM HOST H INNER JOIN VERSION V ON (H.versionID = V.id) INNER JOIN FACILITY F ON (H.facID = F.id) INNER JOIN MODEL M ON (H.modelID = M.id) INNER JOIN SERIAL S ON (H.id = S.HOSTID) WHERE (F.facility = 'TEST-AGH') AND (M.model='TEST-3750')"""
        result = cursor.execute(sql_join_query)
        records = cursor.fetchall()
        i = 1
        for record in records:
                i+=1
##############
#BUILD DATA
###########3##
def addDevice(name):
        path = '/ansible/serial_nums/{}'.format(name)
        for file in glob.glob(os.path.join(path,'*.*')):
                host = []
                #Add facility name to host
                host.append(name)
                f = open(file, 'r')
                info = f.read().splitlines()
                length = len(info)
                i=1
                if length > 1:
                        rspan = length-4
                        sdone = False
                        for line in info:
                                if i == 2:
                                        #Add hostname to host
                                        host.append(line)
                                if i == 3:
                                        #Add model to host
                                        host.append(line)
                                if i > 3 and sdone == False:
                                        #Add serial numbers to host
                                        for j in range(3,length-1):
                                                host.append(info[j])
                                        sdone = True
                                if i == length:
                                        #Add version to host
                                        host.append(line)
                                i+=1
                if len(host) > 1:
                        #If valid entry, collect serials from host list
                        serials = getSerials(host)
                        #Add device to HOST table
                        hostID = insertHost(host)
                        for serial in serials:
                                #Add each serial number to SERIAL table
                                insertSerial(serial,hostID)
#COMPILE LIST OF FACILITIES
FACILITIES = ['ROUTERS','SITE1SWITCHES','SITE2SWITCHES','SITE3SWITCHES','SITE4SWITCHES','SITE5SWITCHES']
#THE ENTRIES IN THE LIST NEED TO CORRESPOND TO THE ANSIBLE INVENTORY GROUP NAMES THAT YOU WISH TO ADD TO DB
#MY INVENTORY IS GROUPED BY FACILITY
#SUCH AS:
#[ROUTERS]
#SITE1ROUTER
#SITE2ROUTER
#etc...
#[SITE1]
#SWITCH1
#SWITCH2
#etc...
#ADJUST YOUR SCHEMA AS NECESSARY 
for facility in FACILITIES:
        #CREATE RECORDS FOR DEVICES
        addDevice(facility)
