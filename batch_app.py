# This moudule contains batch programs which do
#  - execute spider
#  - collect rpa data
#  - calculate rates
#  - send db tables to targets
#  - write logs including stats(collected, updated, added, errors)
# Each program can be executed solely by command
#  "PYTHONPATH='.' luigi --module batch_app --local-scheduler <JOB_NAME, eg. CalculateRates>"
# Executing all programs is also supported.
#  "PYTHONPATH='.' luigi --module batch_app --local-scheduler RunAll" 

import luigi
from database import manager, models, repo
from psycopg2.extras import execute_values, execute_batch, DictCursor
from helper.dateutil import get_adj_date, get_iso_date, calculate_iso_date
import logging
from scrapy.utils import log as scrapy_log
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from multiprocessing import Process
from datetime import datetime, timedelta
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import math
import traceback


today_str = datetime.strftime(datetime.now(),'%Y%m%d')
sess = manager.Session()
master_repo = repo.MasterRepository()

def get_output_file(name):
    output_path = ""
    for i in range(1,999):
        output_path = f"./logs/{name}-{today_str}-{str(i).zfill(3)}.log"
        if not os.path.isfile(output_path):
            break
    logging.warn("Returning outputpath!!")
    return output_path

def save_all(data):
    frequency = 'D'
    try:
        id = str(data[0][0])
        meta = master_repo.get_master().get(id)    
        if meta is not None:
            frequency = meta.frequency
    except:
        pass
    for row in data:
        try:
            date = str(row[1])[:8]
            iso_date = get_iso_date(frequency, date)
            date = get_adj_date(iso_date)
            obj = models.MacroData(id=str(row[0]), date=date, iso_date=iso_date, value=float(str(row[2]).replace(',','')), time_updated=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            sess.merge(obj)
            sess.commit()
        except:
            logging.error(f'Row Dropped : '+str(row))

def get_s3_client(access_key='OWl4khyO7evCLF81vN4B', secret_key = 'HvUlTGwKMhVMhKwL6hWYoqHACQmytvBsO7NCgBD5'):
    service_name = 's3'
    endpoint_url = 'https://kr.object.fin-ncloudstorage.com'
    client = boto3.client(service_name, endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return client

class SingleTaskRunner(luigi.Task):
    def complete(self):
        return True

class CommonTask(luigi.Task):
    finish = False
    prev = luigi.TaskParameter(default=SingleTaskRunner())

    def requires(self):
        logging.info(self.prev)
        return self.prev
    
    """IMPLEMENTATION REQUIRED"""
    def process(self):
        pass
    
    def run(self):
        self.process()
        self.finish=True

    def complete(self):
        return self.finish

class LoadExternalData(CommonTask):
    """
    Import data in object storage(e.g RPA)
    """

    date = luigi.Parameter(default='')
    history = luigi.BoolParameter(parsing=luigi.BoolParameter.IMPLICIT_PARSING)
    
    client = get_s3_client()
    bucket_name = 'ossy-economy-data'

    def read_csv_infer_encodings(self, data, header=None, expected_encodings=["utf-8","euc-kr","cp949","ansi"]):
        for encoding in expected_encodings:
            try:
                return pd.read_csv(io.BytesIO(data), header=header, encoding=encoding)
            except Exception as e:
                logging.warn(f"Reading csv encoded by {encoding} is failed\n {str(e)}")
                continue
        raise RuntimeError("Can't read csv.")

    def import_data(self, operation_parameters):
        paginator = self.client.get_paginator('list_objects')
        page_iterator = paginator.paginate(**operation_parameters)
        key_list = []
        for page in page_iterator:
            if 'Contents' in page:
                for meta in page['Contents']:
                    if meta['Size'] != 0:
                        key_list.append(meta['Key'])        
                        # filename = key.split('/')[-1]
                        # id = filename.split('.')[0]
        for k in sorted(key_list, reverse=False):
            try:
                logging.info(k)
                obj = self.client.get_object(Bucket=self.bucket_name, Key=k)
                data = self.read_csv_infer_encodings(obj['Body'].read()).values.tolist()
                save_all(data)
            except Exception as e:
                logging.error("Errors on saving data into S3 " + str(e))
                continue;

    def process(self):
        if self.history:
            self.import_data({'Bucket': 'ossy-economy-data', 'Prefix': '_scrapped/소급데이터/'})
        if self.date == '':
            today = datetime.now()
            for i in range(0,8):
                date_prefix = datetime.strftime((today - timedelta(days=i)), '%Y%m%d')
                self.import_data({'Bucket': 'ossy-economy-data', 'Prefix': f'_scrapped/{date_prefix}/'})


class ExecuteSpiders(CommonTask):
    """
    Execute All spiders defined in Master
    """

    date = luigi.Parameter(default=today_str)

    def crawl(self, source, target_spider_list):
        try:
            log_dir = f'logs/{self.date}'
            os.makedirs(log_dir, exist_ok=True)
            settings = get_project_settings()
            settings['LOG_FILE'] = f'logs/{self.date}/{source}.log'
            runner = CrawlerProcess(settings, install_root_handler=True)
            logging.info(f'[{source}] {len(target_spider_list)} spider master data loaded')
            normal_spider_count = 0
            abnormal_spider_count = 0
            for m in target_spider_list:
                try:
                    runner.crawl(m.id)
                    normal_spider_count += 1
                except:
                    logging.info(f'Spider [{m.id}] does not exists')
                    abnormal_spider_count += 1
            logging.info(f'[{source}] Run {normal_spider_count} normal spiders of total {len(target_spider_list)} spiders in the list')
            runner.start()
        except Exception as ex:
            traceback.print_exception(type(ex), ex, ex.__traceback__)
            logging.error("Something wrong. CrawlerProcess exits unexpectedly.")
        

    def process(self):
        scrapy_log.DEFAULT_LOGGING['loggers']['scrapy']['level'] = 'WARNING'
        sess = manager.Session()
        query = sess.query(models.Master)
        master = query.filter_by(batch_yn=False).all()

        source_grouped_master = {}
        for m in master:
            source_grouped_master.setdefault(m.source, []).append(m)
        
        for source in source_grouped_master:
            target_spider_list = source_grouped_master[source]
            logging.info(f'[{source}] Fork child process')
            # FIXME: redirect stdout, stderr
            process = Process(target=self.crawl, args=(source,target_spider_list,))
            process.start()
            process.join()
            logging.info(f'[{source}] Process finished ')
           

class CalculateRates(CommonTask):

    id_set = set()

    def calculate_common(self, job, raw_data):
        try:
            batch_type = job.batch_params['type']
            job_id = job.id
            ref_id = job.batch_params.get('ref')
            if ref_id is None:
                ref_id = job_id[:-(len('_'+batch_type))]
            if ref_id not in self.id_set:
                logging.warn(f"{job_id} refers to {ref_id} not in the list.")
                return
            ref_data = raw_data.loc[raw_data.id == ref_id].sort_values(by='iso_date', ascending=False)
            ref_dict = (ref_data.set_index('iso_date')['value']).to_dict()
            if batch_type == 'mom':
                datedelta = 'month'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)/prev_val*100
            elif batch_type == 'wchg':
                datedelta = 'week'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)
            elif batch_type == 'mchg':
                datedelta = 'month'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)
            elif batch_type == 'wow':
                datedelta = 'week'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)/prev_val*100
            elif batch_type == 'qoq':
                datedelta = 'quarter'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)/prev_val*100
            elif batch_type == 'ychg':
                datedelta = 'year'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)
            elif batch_type == 'yoy':
                datedelta = 'year'
                formula = lambda cur_val, prev_val: (cur_val - prev_val)/prev_val*100
            else:
                logging.error(f'Not supported batch mode. {job.id} failed.')
                return
            result = []
            for row in ref_data.itertuples():
                try:
                    curr_iso_date = getattr(row, 'iso_date')
                    prev_iso_date = calculate_iso_date(curr_iso_date, mode=datedelta)
                    if prev_iso_date in ref_dict:
                        calculated_value = formula(getattr(row, 'value'),ref_dict[prev_iso_date])
                        if calculated_value is not None and not math.isnan(calculated_value):
                            result.append({
                                'id': job_id,
                                'date': getattr(row, 'date'),
                                'iso_date': getattr(row, 'iso_date'),
                                'value': calculated_value,
                                'time_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })
                except ZeroDivisionError as e:
                    pass
            return result
        except Exception as e:
            logging.error(f'Batch calculation for {job.id} failed. reason[{str(e)}]')
            
    def evaluate_formula(self, job, raw_data):
        try:
            formula = job.batch_params['formula']
            refs = job.batch_params['refs']
            ref_ids = set(refs)
            job_id = job.id
            ref_data = raw_data.loc[raw_data.id.isin(ref_ids)].sort_values(by='iso_date', ascending=False).set_index('iso_date')
            ref_dict = ref_data[['id','value']].groupby(level=0).agg(list).apply(lambda row: dict(zip(row['id'], row['value'])), axis=1).to_dict()
            # samples above : {'2020-12': {'kr_core_cpi': 105.89, 'kr_cpi': 105.67}, '2021-01': {'kr_cpi': 106.47, 'kr_core_cpi': 106.27}}
            result = []
            for curr_iso_date in ref_dict.keys():
                data = ref_dict[curr_iso_date]
                # disallow sparse data
                if not ref_ids == set(data):
                    continue
                aliases = {}
                for k in data.keys():
                    aliases[refs[k]] = data[k]
                calculated_value = eval(formula, aliases)
                if calculated_value is not None and not math.isnan(calculated_value):
                    result.append({
                        'id': job_id,
                        'date': get_adj_date(curr_iso_date),
                        'iso_date': curr_iso_date,
                        'value': calculated_value,
                        'time_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
            return result
        except Exception as e:
            logging.error(f'Batch calculation for {job.id} failed. reason[{str(e)}]')

    def reindex(self):
        # try reindexing
        try:
            with manager.engine.connect() as con:
                con.execute('REINDEX TABLE gksc_s_macro_data_00')
        except:
            pass
    
    def process(self):
        # try reindexing
        self.reindex()
        query = sess.query(models.Master)
        batch_master = query.filter_by(batch_yn=True).order_by(models.Master.id).all() # Let the low level batch job(shorter id) calculated first
        raw_data = pd.read_sql_table('gksc_s_macro_data_00',con=manager.engine, columns=['id','date','iso_date','value'])
        self.id_set = set(raw_data['id'].unique())
        # change output_tname to test batch
        output_tname = 'gksc_s_macro_data_00'
        for job in batch_master:
            batch_params = job.batch_params
            if batch_params is None:
                # Special batch?
                logging.warn(f"{job.id} has no batch params")
            else:
                result = []
                if batch_params['type'] in ['mom','wchg','mchg','wow','qoq','ychg','yoy']:
                    result = self.calculate_common(job, raw_data)
                elif batch_params['type'] in ['eval'] and 'formula' in batch_params and 'refs' in batch_params:
                    result = self.evaluate_formula(job, raw_data)                
                if result is not None and len(result) > 0:
                    to_del_ids = list(set([x['id'] for x in result]))
                    conn = manager.engine.raw_connection()
                    with conn.cursor() as cur:
                        for id in to_del_ids:
                            cur.execute(f"DELETE FROM {output_tname} WHERE id='{id}'")
                        query = f"INSERT INTO {output_tname} (id, date, iso_date, value, time_updated) VALUES %s"
                        values_template = '(%(id)s, %(date)s, %(iso_date)s, %(value)s, %(time_updated)s)'
                        execute_values(cur, query , result, values_template)    
                    conn.commit()
                    raw_data = raw_data.drop(raw_data.loc[raw_data.id.isin(to_del_ids)].index).append(result)
                    self.id_set = set(raw_data['id'].unique())
        # try reindexing
        self.reindex()


class WriteParquet(CommonTask):

    client = get_s3_client()
    bucket_name = 'ossy-economy-data'
    data_file_name = 'macro_data.parquet'
    master_file_name = 'macro_data_master.parquet'

    def process(self):
        df_data = pd.read_sql_table('gksc_s_macro_data_00',con=manager.engine, columns=['id','date','value']).rename(columns={"id": "symbol"})
        table_data = pa.Table.from_pandas(df_data)
        pq.write_table(table_data, self.data_file_name, row_group_size=20000)
        self.client.upload_file(self.data_file_name, self.bucket_name, f'economy_history_new/{self.data_file_name}')

        df_master = pd.read_sql_table(
            'gksc_s_macro_master_00',
            con=manager.engine,
            columns=['id','country','main_category','sub_category','name','unit','source','first_date','frequency','precision']).rename(
                columns={"id": "symbol","main_category":"category1","sub_category":"category2","first_date":"start_date","frequency":"period"}
                )
        table_master = pa.Table.from_pandas(df_master)
        pq.write_table(table_master, self.master_file_name, row_group_size=20000)
        self.client.upload_file(self.master_file_name, self.bucket_name, f'economy_symbol_new/{self.master_file_name}')


class RunAll(luigi.WrapperTask):

    finish = False

    def requires(self):
        return WriteParquet(
            prev=CalculateRates(
                prev=ExecuteSpiders(
                    prev=LoadExternalData())))

    def run(self):
        logging.info("Do All jobs at once.")
        self.finish = True

    def complete(self):
        return self.finish
