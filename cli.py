import csv
import os
import sys
from datetime import date

import click
import yaml

from database import manager, repo
from database.manager import Session
from database.models import MacroData
from helper import auth, dateutil
from helper.dateutil import DateParser, get_iso_date, get_adj_date

@click.group()
def cli():
    pass

@click.command()
def initdb():
    if not os.path.isfile('credentials.cfg'):
        click.echo('DB 설정파일이 없습니다. credentials.cfg.template으로부터 credentials.cfg파일을 생성해 주세요.')
        sys.exit(1)
    manager.init_db()
    manager.init_master()
    click.echo('Database initialization finished')

@click.command()
@click.argument('filename')
def export_master(filename):
    master_data = repo.MasterRepository().get_master()
    master_dict = {}
    for k in master_data.keys():
        # Model To dict
        model = master_data[k].__dict__
        del(model['_sa_instance_state'])
        del(model['id'])
        # if model.get('batch_params') is not None:
        #     model['batch_params'] = {'type':model['batch_params']}
        master_dict[k] = model
    with open(filename, 'w', encoding='utf-8') as f:
        yaml.dump(master_dict, f, allow_unicode=True)


@click.command()
@click.argument('filename')
@click.option('--encoding', default='euc-kr', help='Specify file encoding')
def import_csv(filename, encoding):
    rf = open(filename, 'r', encoding=encoding)
    rdr = csv.reader(rf)
    date_parser = dateutil.DateParser("%Y%m", "m")
    for line in rdr:
        date = line[0]
        id = line[1]
        value = line[2]
        date_parser.update(date)
        rows = MacroData(id=id, date=date_parser.get(), value=value)
        sess = Session()
        sess.merge(rows)
        sess.commit()

@click.command()
@click.argument('id')
def clean_date(id):
    print("empty")
    
'''
    meta = repo.MasterRepository().get_master().get(id)
    frequency = 'D'
    if meta is not None:
        frequency = meta.frequency
    sess = Session()
    all_data = sess.query(MacroData).filter_by(id=id).all()
    for data in all_data:
        data.iso_date = get_iso_date(frequency, data.date)
        data.date = get_adj_date(data.iso_date)
        sess.commit()
'''

cli.add_command(initdb)
cli.add_command(export_master)
cli.add_command(import_csv)
cli.add_command(clean_date)

if __name__ == '__main__':
    cli()
