# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 10:37:18 2018

@author: pqian
"""

import os
import re
import subprocess
import pyodbc
from datetime import datetime
import sys
import getpass
import pandas as pd

class SQLLoader:
    
    def __init__(self):
        self.ctr_str = \
        """OPTIONS (SKIP = {str_skiprows})
        LOAD DATA
        INFILE '{str_infile}'
        BADFILE '{str_badfile}'
        DISCARDFILE '{str_discardfile}'
        
        INTO TABLE "{str_schema}"."{str_table}"
        {str_mode}
        FIELDS TERMINATED BY '{str_sep}'
        TRAILING NULLCOLS
        ({str_headers} )
        """
        self.v_time = datetime.today().strftime('%Y%m%d')
#        self.schemaname = schemaname
#        self.uid = uid
#        self.psw = psw
#        self.foldername = foldername
#        self.tblName = tblName
#        self.mode = mode
#        self.sep = sep
#        self.skiprows = skiprows
        
        
    def get_dbConn_str(self, schemaname, uid, psw):
        connParams = [schemaname, uid, psw]
        conStr = "DRIVER={{Oracle in OraClient12Home1}};SERVER={0}.HAP.ORG;DBQ={0}.HAP.ORG;UID={1};PWD={2}".format(*connParams)
        dbConn = pyodbc.connect(conStr, autocommit=True)
        dbCursor = dbConn.cursor()
        db_sqlplus = '{}/{}@{}.HAP.ORG'.format(uid, psw, schemaname)
        return dbConn, dbCursor, db_sqlplus
    
    def create_table(self, folder, file, skiprows_hdr, sep, tbl_nm, seq_no=None):
        # if the table doesn't exist yet, create it, using the header row in the file
        header = pd.read_csv(folder+file, sep=sep, header=None, skiprows=skiprows_hdr, nrows=1)
        for row_index, row in header.iterrows():
            #print (row)
            pStr = ' VARCHAR2 (1000), '.join(re.sub('[^A-Za-z0-9]+', '', i) for i in row)
            headerList = [re.sub('[^A-Za-z0-9]+', '', i) for i in row]
            headerList.append('source_file')
            headerList.append('load_dt')
            headerStr= ',\n'.join(headerList)
            cmdStr = 'CREATE TABLE {} ({} VARCHAR2(1000), source_file VARCHAR2(1000), load_dt number);'.format(tbl_nm, pStr)
        return cmdStr, headerStr
    
    def get_field_names(self, dbExecution, tblnm):
        sqlstr = "SELECT count(*) FROM all_tab_columns WHERE table_name = '{}'".format(tblnm.upper())
        checkResult = dbExecution.execute(sqlstr).fetchone()
        try:
            if checkResult[0] == 0:
                ind = False
            else:
                ind = True
        except:
            ind = False
        return ind
    
    def get_control_file(self, uid, foldername, inputfilename, schemaname, tblname, mode, sep, headerStr, skiprows):
        """manipulate control string and then generate a control file
        """
        fullname = foldername + inputfilename
        inptflPrefix = fullname[:-5]
        dict_ctrl = {'str_skiprows': skiprows,
                     'str_infile': fullname,
                     'str_badfile': inptflPrefix+'.bad',
                     'str_discardfile': inptflPrefix+'.dsc',
                     'str_schema': uid.upper(),
                     'str_table': tblname.upper(),
                     'str_mode': mode,
                     'str_sep': sep,
                     'str_headers': headerStr
                    }
        str_control = self.ctr_str.format(**dict_ctrl)
        #print (str_control)
        os.makedirs(foldername + '\CTL_dest\\', exist_ok = True)
        ctrlFile = r'{}\CTL_dest\{}.ctl'.format(foldername, inputfilename[:-5])
        logFile = r'{}\CTL_dest\{}.log'.format(foldername, inputfilename[:-5])
        with open (ctrlFile, 'w') as foo:
            foo.write(str_control)
        with open (logFile, 'w') as foo:
            foo.write(str_control)
        return ctrlFile, logFile
    
    def insert_dt_n_source(self, dbExecution, tblname, source_file):
        updtStr = "UPDATE {} SET load_dt = '{}', source_file = '{}' WHERE load_dt IS NULL".format(tblname, self.v_time, source_file)
        dbExecution.execute(updtStr)
        
    def sql_loader_text_files(self, schemaname, uid, psw, foldername, tblName, mode, sep, skiprows=1, skiprows_hdr=0):
        dbConn, dbExecution, db_sqlplus = self.get_dbConn_str(schemaname, uid, psw)
        # get the field names first
        for i, f in enumerate([x for x in os.listdir(foldername) if os.path.isfile(foldername + '\\' + x) and 'TXT' in x.upper()]):
            print ('Working On ' + f)
            # get header names and create table string (if it'll create one in INSERT mode)
            cmdStr, headerStr = self.create_table(foldername, '\\'+f, skiprows_hdr, sep, tblName)
            # create control file
            destCTL, destLOG = self.get_control_file(uid, foldername, '\\'+f, schemaname, tblName, mode, sep, headerStr, skiprows)
            # need to drop table if the mode is "INSERT"
            if mode == 'INSERT': #i == 0
                if self.get_field_names(dbExecution, tblName):
                # truncate if it exists
                    TruncStm = "TRUNCATE TABLE {}".format(tblName)
                    dbExecution.execute(TruncStm)
                # create a new table then
                else:
                    print ('The DDL is \n{}'.format(cmdStr))
                    dbExecution.execute(cmdStr)
            # Test if data exists
            checkSQL = "SELECT COUNT(*) FROM {} WHERE source_file='{}'".format(tblName, f)
            try:
                checkResult = dbExecution.execute(checkSQL).fetchone()
            except:
                checkResult=[0]
                print ("Table Doesn't Exist")
            # If not, insert data
            if checkResult[0] == 0:
                subprocess.call("sqlldr userid='{}' control='{}' LOG='{}' SILENT=FEEDBACK ROWS=1000".format(db_sqlplus, destCTL, destLOG), 
                                shell=True)
                print ('Loading Done!')
            # insert load time and source file name
            self.insert_dt_n_source(dbExecution, tblName, f)
