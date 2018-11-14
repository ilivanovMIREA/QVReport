#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import environ
import json
import pyodbc
import os,sys
import string,types
reload(sys)
sys.setdefaultencoding('utf8')
import csv
current_env = environ.get("APPLICATION_ENV", 'production')


with open('config/%s/config.%s.json' % (current_env, current_env)) as f:
    config = json.load(f)

class Connect(object):

    def __init__(self, db=None):
        if db:
            try:
                TYPE=db
                DSN = config["File_Storage"]["DSN"][TYPE]
                DATABASE = config["File_Storage"][DSN]["DATABASE"]
                UID = config["File_Storage"][DSN]["UID"]
                PWD = config["File_Storage"][DSN]["PWD"]    
            except KeyboardInterrupt:
                pass
            self.odbc = pyodbc.connect('DSN=%s;DATABASE=%s;UID=%s;PWD=%s' % (DSN, DATABASE,UID,PWD))
            self.cursor = self.odbc.cursor()
            print "Good connection DB"
        else:
            raise Exception("DB not selected")

    def Execute(self, sql, fetch='none', csvcheck = 'none', name = 'none'):
        #
        # EXECUTE SQL QUERY ON THE SELECTED AS/400
        #
        # Parameters:
        #  sql   - String variable containing the SQL statement
        #          (ie. "Select Count(*) From MYLIB/MYFILE")
        #  fetch - Either of "fetchall", "fetchone" or "none" if left blank
        #          "fetchall" retrieves all records
        #          "fetchone" retrieves only a single record
        #          "none" or not specified is for UPDATE and INSERT statements typically.
        #          (A variation of this parm might be to specify a method for "fetchmany")
        #
        try:
            self.cursor.execute(sql)
        except:
            return 'error','Your SQL Statement Returned an error: %s\n\n%s'\
                           % (sys.exc_info()[0],sys.exc_info()[1])
        if fetch == 'none':
            return 'Ok',''
        elif fetch == 'fetchone':
            result    =    self.cursor.fetchone()
        elif fetch == 'fetchall':
            result    =    self.cursor.fetchall()

        if csvcheck == True:
            with open("Data/%s%s" % (name, ".csv"), "wb") as csv_file:              # Python 2 version
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([i[0] for i in self.cursor.description]) # write headers
                for row in result:
                    csv_writer.writerow(list(row))
            return
        else:
            x = 0
            col = []
            
            for each in self.cursor.description:
                col[string.upper(each[0])] = x
                x += 1
            return result,col
        #
        # "result" contains an array of the returned records
        # "col" is a dictionary containing an array index for the column names
        #
        # Example:
        #    If a single row is: ["Joe","Smith",31,12.05,"Full Time"]
        #    And the columns are:[fname,lname,age,wage,jobtype]
        #    Then "col" contains:  { "fname":0,"lname":1,"age":2,"wage":3,"jobtype":4 }
        #
        #    Fields within the returned rows are then accessed like this:
        #    firstname = result[col['fname']]
        
        

    def query(self, sql):
        try:
            data = self.cursor.execute(sql)
        except:
            return 'error','Your SQL Statement Returned an error: %s\n\n%s'\
                           % (sys.exc_info()[0],sys.exc_info()[1])
        return data, self.cursor.description


    def GetCSV(self):
        with open("Data/%s%s" % (name, '.csv')) as csv_file:              # Python 2 version
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(col) # write headers
            n = 0
            for row in result:
                n = n + 1
                print n
                csv_writer.writerow(list(row))

    def __del__(self):
        print("Close connect DB")
        self.cursor.close()
        self.odbc.close()