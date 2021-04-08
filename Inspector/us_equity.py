from database import manager
from datetime import date
import datetime
import csv
import os
import time

import eikon as ek
import pandas as pd
import numpy as np
import pandas as pd

import math

_API_KEY = '4d6f9e96c25f49c9a8c4a5a464b19cca87ef749c'

sess = manager.mysql_con


"""EIKON 미국 종목 정보 """
class equity_hist():
  
    def __init__(self, *args, **kwargs):
        print("start process")

    def LoadM194HREFE(self):
        
        cursor = sess.cursor()

        # 테이블 삭제
        sql = "DELETE FROM m194hrefe;"
        cursor.execute(sql)
        sess.commit()

        sql = """insert into m194hrefe(F16013,F16012,F16288,F16002) values ( %s, %s, %s, %s)"""

        file = os.getcwd() + "\\data\\M194HREFE.csv"
        df = pd.read_csv(file,header = None)
        df = df.loc[ : , 0:3 ]

        records = []
        for i, row in df.iterrows():
            record = row.tolist()
            records.append(record)
    
        cursor.executemany(sql, records)
        sess.commit()
        
        print("M194HREFE Load")        

    def LoadM194HBASED(self):
    
        cursor = sess.cursor()
        # 테이블 삭제
        sql = "DELETE FROM m194hbased;"
        cursor.execute(sql)
        sess.commit()

# 파일 분할
        file = os.getcwd() + "\\data\\M194HBASED.csv"
        rows = pd.read_csv(file, chunksize=10000) 
        for i, chuck in enumerate(rows): 
            new_file = os.getcwd() + "\\data\\M194HBASED_SEP.csv"
            chuck.to_csv(new_file,header = None, index = False) 
            self.LoadEXM194HBASED()


    def LoadEXM194HBASED(self):

        cursor = sess.cursor()
        sql = """insert into m194hbased(F16013,F12506,F16288,F15009,F15010,F15011,F15001,F15015,F30510,F30511,F15472,F15004,F15006,F03003,F15028,F16143,F15007) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        file = os.getcwd() + "\\data\\M194HBASED_SEP.csv"
        df = pd.read_csv(file,header = None)
        
        df = df.loc[ : , 0:16 ]
        df.to_csv(file, header=False, index=False )

# 개별 Record 로 처리 
        records = []
        for i, row in df.iterrows():
            record = row.tolist()
            record = [0 if x != x else x for x in record]
            records.append(record)
   
        cursor.executemany(sql, records)
        sess.commit()


#        load_sql = "LOAD DATA LOCAL INFILE" + "'" + file + "'"  + "INTO TABLE usermanaged.city" + "FIELDS TERMINATED BY ',' ;"
#        cursor.execute(load_sql)
        
        stime = time.strftime('%c', time.localtime(time.time()))
        slog = stime + " => LoadEXM194HBASED Load"
        print(slog)        


    def Run(self):
        self.DropTables()

    def Request(self):
        ek.set_app_key(_API_KEY)
        df = ek.get_timeseries(["MSFT.O"],['TIMESTAMP','OPEN','HIGH','LOW','CLOSE','VOLUME'], start_date='2000-01-01',end_date='2021-03-30', interval="daily")        

        hists = []

'''
        for index, row in df.iterrows():
            print(row[1])
            print(row[2])

        for i in df.index:
            val = df.get_value(i, 0)
            val1 = df.get_value(i, 1)
            val2 = df.get_value(i, 2)
        print(df)
'''
#        master_data = sess.query(models.Master).filter_by(id=id).first()

