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
        opts, args = getopt.getopt(argv, "c:d:hi:", ["ifile=", "dir=", "config="])
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
        elif opt in ("-d", "--dir"):
            buswarelogdir = arg
        elif opt in ("-c"):
            configpath = arg



    config = LoadConfiguration(configpath)
    cnn = pyodbc.connect('Server=' + config['DatabaseServer'] + ';DRIVER={SQL Server};database=' + config['DatabaseName'] + ';UID=' + config['DatabaseLogin'] + ';PWD=' + config['DatabasePW']+';Trusted_Connection=no')
    cr = cnn.cursor()

    #directory = os.fsencode(config["FileDirectory"])
    for logfile in os.listdir(config["FileDirectory"]):

        LoadRollups(config["FileDirectory"]+'\\'+logfile, cr, agency="NJT")

        with open(config["FileDirectory"]+'\\'+logfile, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')

            LoadDiscardReasons(spamreader, cnn, agency="NJT")
            LoadData(spamreader, cnn, agency="NJT")


def LoadRollups(spamreader, cr, agency):
    print("Loading Roll ups")
    found = False
    #create an array of the roll ups, compare it to what is in the database and insert the new ones
   
    f  =pd.read_csv(spamreader)
    keep_col = ['ROLLUP']
    u = f[keep_col]
    print (u)
    t = u["ROLLUP"].unique()
    print (t)

    #get the list of roll ups and only add those that don't exist
    sql = """ select rollup_name from tblRollups"""
    rows = cr.execute(sql).fetchall()

    for dfRow in t:
        print(dfRow)
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

    found = False
    #create an array of the roll ups, compare it to what is in the database and insert the new ones

    f  =pd.read_csv(spamreader)
    keep_col = ['ROLLUP']
    u = f[keep_col]
    print (u)
    t = u["ROLLUP"].unique()
    print (t)

    #get the list of roll ups and only add those that don't exist
    sql = """ select rollup_name from tblRollups"""
    rows = cr.execute(sql).fetchall()

    for dfRow in t:
        print(dfRow)
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

def LoadData(spamreader, cnn, agency):
    print("Loading Data")

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

