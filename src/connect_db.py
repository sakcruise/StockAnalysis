# import psycopg2 as pg
import sqlalchemy
import sys
sys.path.append("..")

from src import config as conf

def connect_db():
    # conn_str = "host={} dbname={} user={} password={}".format(conf.db['host'], conf.db['database'], conf.db['username'], conf.db['password'])
    # conn = pg.connect(conn_str)
    # print(conn)
    # return conn

    engine = sqlalchemy.create_engine("postgresql://postgres:sakcruise@localhost/sakcruise")
    conn = engine.connect()
    return conn

if __name__ == "__main__":
    connect_db()