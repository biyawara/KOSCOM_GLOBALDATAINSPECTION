from database import models, manager
from psycopg2.extras import execute_values, execute_batch, DictCursor
import logging

class MasterRepository:
    _master = {}

    def get_master(self):
        if len(self._master) == 0:
            query = manager.Session().query(models.Master)
            for row in query:
                self._master[row.id] = row
        return self._master

class DataRepository:
    """
    Assuming data is dict of row, support batch method to obtain performance. 
    """

    def batch_insert(self, data):
        conn = manager.engine.raw_connection()
        with conn.cursor() as cur:
            query = f"INSERT INTO gksc_s_macro_data_00 (id, date, iso_date, value, time_updated) VALUES %s"
            values_template = '(%(id)s, %(date)s, %(iso_date)s, %(value)s, %(time_updated)s)'
            execute_values(cur, query , data, values_template)    
        conn.commit()

    def batch_update(self, data):
        conn  = manager.engine.raw_connection()
        with conn.cursor() as cur:
            query = """
                UPDATE gksc_s_macro_data_00 a 
                SET a.id=b.id, a.date=b.date, a.iso_date=b.iso_date, a.value=b.value, a.time_updated=b.time_updated
                FROM (VALUES %s) AS b (id, date, iso_date, value, time_updated) WHERE a.id=b.id and a.date=b.date
            """
            values_template = '(%(id)s, %(date)s, %(iso_date)s, %(value)s, %(time_updated)s)'
            execute_values(cur, query, data, values_template)

    def batch_upsert(self, data):
        key_set = set()
        for row in data:
            key_set.add((row['id'], row['date']))
        pass