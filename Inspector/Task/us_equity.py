from database import manager,models
from datetime import date

import eikon as ek
import pandas as pd
import numpy as np

_API_KEY = '4d6f9e96c25f49c9a8c4a5a464b19cca87ef749c'

sess = manager.Session()

"""EIKON 미국 종목 정보 """
class EikonUsEqityHist():
  
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requests(self):
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

