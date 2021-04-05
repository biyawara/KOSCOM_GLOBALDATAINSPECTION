from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, inspect
from sqlalchemy import create_engine, inspect
import yaml
import configparser
#import pymysql

__all__ = [
    "engine",
    "Base",
    "Session"
]

config = configparser.ConfigParser()
config.read("credentials.cfg")

connect_string = config['database']['address']

engine = create_engine("mysql+pymysql://{config['database']['username']}:{config['database']['password']}@{config['database']['address']}/{config['database']['dbname']}", pool_pre_ping=True, pool_size=20, max_overflow=30)


Base = declarative_base()
Session = scoped_session(sessionmaker(bind=engine))

def init_db():
    """models 모듈에 정의된 스키마를 생성"""
    import database.models
    Base.metadata.create_all(bind=engine)

'''
def init_master():
    """master.yml에 정의된 마스터 데이터를 머지"""
    from database.models import Master
    with open("master.yml", encoding="utf-8") as f:
        doc = yaml.load(f, Loader=yaml.FullLoader)
    db_session = Session()
    for key in doc:
        values = doc[key]
        m = Master(
            id=key,
            **values
        )
        db_session.merge(m)
    db_session.commit()
'''

