import pyodbc
import csv
import sys
import getopt
import json
import os
from csv import DictReader
import pandas as pd
import numpy as np

def main(argv):
    inputfile = ''
    buswarelogdir = ''
    directory = ''
    odbc = ''
    configpath = ''
    config = ''

    if len(argv) == 0:
        print(
            'Missing required argument of source file. Command format is: main.py -i <csv file> -d <Busware log directory>')
        sys.exit(2)
    try:
        opts, args = getopt.getopt(argv, "c:dhi:", ["ifile=", "dir=", "config="])
    except getopt.GetoptError:
        print('There was an error.  main.py -i <csv file>')
        print(getopt.GetoptError.msg)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("Configuration Parameters")
            print('main.py -i <csv file>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-d"):
            cleardb = True
        elif opt in ("-c"):
            configpath = arg

    config = LoadConfiguration(configpath)
    cnn = pyodbc.connect('Server=' + config['DatabaseServer'] + ';DRIVER={SQL Server};database=' + config['DatabaseName'] + ';UID=' + config['DatabaseLogin'] + ';PWD=' + config['DatabasePW']+';Trusted_Connection=no')
    cr = cnn.cursor()
    if cleardb ==True:
        print ("Deleting all records from tblDiscards")
        cr.execute("truncate table tblDiscards")
        cr.commit()
    #directory = os.fsencode(config["FileDirectory"])
    for logfile in os.listdir(config["FileDirectory"]):
        #get the agency ID
        a = logfile.split('-')
        agency = a[0]
        print ("loading files for: " + agency)
        LoadRollups(config["FileDirectory"]+'\\'+logfile, cr, agency)
        LoadData(config["FileDirectory"]+'\\'+logfile, cr, agency)

        #with open(config["FileDirectory"]+'\\'+logfile, newline='') as csvfile:
        #    spamreader = csv.reader(csvfile, delimiter=',')

def LoadRollups(spamreader, cr, agency):
    print("Loading Roll ups")
    found = False
    #create an array of the roll ups, compare it to what is in the database and insert the new ones

    f  =pd.read_csv(spamreader)
    keep_col = ['ROLLUP']
    u = f[keep_col]
    t = u["ROLLUP"].unique()

    #get the list of roll ups and only add those that don't exist
    sql = """ select rollup_name from tblRollups"""
    rows = cr.execute(sql).fetchall()

    for dfRow in t:
       # print(dfRow)
        for row in rows:
            if dfRow==row:
                found = True
        if found == False:
        #add the roll up to the table
            sql_update = "insert into watcher.dbo.tblRollups(rollup_name) values("+dfRow.astype(str)+ ')'
            cr.execute(sql_update)
            cr.commit()
        if found ==True:
            found = False


def LoadDiscardReasons(spamreader, cnn, agency):
    print("Loading Discard Reasons")

def LoadData(spamreader, cr, agency):
    print("Loading Data")
    rollup_name = ''
    rollup_id = ''
    #f = pd.read_csv(spamreader)
    #keep_col = ['ROLLUP', 'VEHICLE', 'TRIP_KEY', 'XDISCARD_REASON', 'XROUTE', 'XSCHEDULE',
    #            'XSURVEY_DATE', 'XBUSTOOLS_VER', 'XBUSWARES_VER']
    #u = f[keep_col]

    #grab the data in the roll table so we can match the ids with the name

    sql = """insert into tblDiscards(rollup_id,vehicle_id,trip_key,discard_reason,route_id,schedule_name, survey_date,bustools_version,busware_version,agency) values ( """

    with open(spamreader) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                #print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                line_count += 1
                sql2 = row[0]+","+row[1]+","+row[4]+",'"+row[8]+"','"+row[10]+"','"+row[17]+"','"+row[18]+"','"+row[44]+"','"+row[45]+"','"+agency+"')"
                sql3 = sql+sql2
                cr.execute(sql3)
                cr.commit()
        print(f'Processed {line_count} lines.')

def LoadConfiguration(configpath):
    config = ''
    with open(configpath) as json_file:
        config = json.load(json_file)
    print ("Configuration Information")
    print('Database name: ' +config['DatabaseName'])
    print('Database server: ' + config['DatabaseServer'])
    print('Database Login: ' + config['DatabaseLogin'])
    print('Database PW: ' + config['DatabasePW'])
    return config


if __name__ == "__main__":
    print("starting program")
    main(sys.argv[1:])

