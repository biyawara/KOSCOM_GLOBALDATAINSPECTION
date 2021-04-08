#import mysql.connector
import pymysql
import configparser


config = configparser.ConfigParser()
config.read("credentials.cfg")

mysql_con = None
mysql_con = pymysql.connect(host=config['database']['address'], user=config['database']['username'], password=config['database']['password'], db=config['database']['dbname'],charset='utf8')


print("start db-engine")