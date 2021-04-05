from datetime import datetime
from database import repo, manager, models
from sqlalchemy import text, Table, Column, Float, String, MetaData, DateTime, insert
import pandas as pd
import calendar
import logging

def get_iso_date(frequency, date):
    date_len = len(date)
    if date_len == 8:
        d = datetime.strptime(date,'%Y%m%d')
    elif date_len == 6:
        d = datetime.strptime(date,'%Y%m')
    else:
        d = datetime.strptime(date,'%Y')
    if frequency == 'W':
        # 해당 날짜가 속한 iso week 기록
        #datetime.strptime(s+'-5','%G-W%V-%u')
        iso_cal = d.isocalendar()
        return f'{iso_cal[0]}-W{str(iso_cal[1]).zfill(2)}'
    elif frequency == 'M':
        return d.strftime('%Y-%m')
    elif frequency == 'Q':
        q = pd.to_datetime(d).to_period('Q')
        return f'{q.year}-Q{q.quarter}'
    elif frequency == 'Y':
        return d.strftime('%Y')
    else:
        # including frequency null and D
        return d.strftime('%Y-%m-%d')

def get_adj_date(iso_date):
    date_len = len(iso_date)
    if 'Q' in iso_date:
        return pd.Period(iso_date).strftime('%Y%m%d')
    elif 'W' in iso_date:
        return datetime.strptime(iso_date+'-5','%G-W%V-%u').strftime('%Y%m%d')
    elif date_len == 10:
        return datetime.strptime(iso_date,'%Y-%m-%d').strftime('%Y%m%d')
    elif date_len == 7:
        d = datetime.strptime(iso_date,'%Y-%m')
        return f"{d.strftime('%Y%m')}{calendar.monthrange(d.year, d.month)[1]}"
    elif date_len == 4:
        return iso_date+'1231'
    else:
        raise ValueError('Not isodate')

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def create_merged_table():
    metadata = MetaData()
    merged_table = Table('gksc_s_merged_macro_data_02', metadata,
        Column("id", String),
        Column("date", String(8)),
        Column("iso_date", String(10)),
        Column("value", Float),
        Column("time_updated", String(19))
    )
    metadata.create_all(manager.engine)

    with manager.engine.connect() as con:
        backup_data = con.execute("select a.*, b.frequency from gksc_s_macro_data_00 a left outer join gksc_s_macro_master_00 b on a.id=b.id order by a.date desc")
        # backup_data = con.execute("select a.*, b.frequency from gksc_s_macro_data_backup_210118 a left join gksc_s_macro_master_00 b on a.id=b.id")
        data = []
        cnt = 0
        for row in backup_data:
            row_frequency = row['frequency']
            iso_date = get_iso_date(row_frequency,row['date'])
            real_date = get_adj_date(iso_date)
            data.append({
                    'id':row['id'],
                    'date':real_date,
                    'iso_date':iso_date,
                    'value':row['value'],
                    'time_updated':now
                })
            try:
                # con.execute(merged_table.insert(), {
                #     'id':row['id'],
                #     'date':real_date,
                #     'iso_date':iso_date,
                #     'value':row['value'],
                #     'time_updated':now
                # })
                cnt += 1
                if cnt == 1000:
                    logging.info("insert 1000 rows")
                    con.execute(merged_table.insert(), data)
                    data = []
                    cnt = 0
            except Exception as e:
                logging.error(f'Insert failure {str(row)} for reason {str(e)}')
        con.execute(merged_table.insert(), data)


def clean_date():
    metadata = MetaData()
    table = Table('gksc_s_macro_data_00', metadata,
        Column("id", String),
        Column("date", String(8)),
        Column("iso_date", String(10)),
        Column("value", Float),
        Column("time_updated", String)
    )

    with manager.engine.connect() as con:
        # target_data = con.execute(table.select().where(text("iso_date is null")))
        backup_data = con.execute("select a.*, b.frequency from gksc_s_macro_data_00 a left join gksc_s_macro_master_00 b on a.id=b.id where iso_date is null")
        data = []
        cnt = 0
        for row in backup_data:
            try:
                row_frequency = row['frequency']
                iso_date = get_iso_date(row_frequency,row['date'])
                real_date = get_adj_date(iso_date)
                update_data = {
                        'id':row['id'],
                        'date':real_date,
                        'iso_date':iso_date,
                        'value':row['value'],
                        'time_updated':now
                    }
                con.execute(table.update().where(table.c.id == row['id']).where(table.c.date == row['date']), update_data)
                cnt += 1
                logging.info(f"Update row count : {cnt}")
            except Exception as e:
                logging.error(f'failure {str(row)} for reason {str(e)}')

        

