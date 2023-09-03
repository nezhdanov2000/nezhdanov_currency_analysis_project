from datetime import datetime, date
import os
from alpha_vantage.timeseries import TimeSeries
from time import sleep
from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2
import psycopg2.sql
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
apikey = Variable.get("apikey")
apiint = Variable.get("apiint")
tickers = Variable.get("tickers")

pg_conn = BaseHook.get_connection(Variable.get("conn_id"))

engine = create_engine('postgresql://' + pg_conn.login + ':' + \
  pg_conn.password + '@' + pg_conn.host + ':' + str(pg_conn.port) + \
  '/' + pg_conn.schema)

def get_daily_ticker_data(stock_id, interval, dummy=False, output=None):
  if (dummy):
    print("get_daily_data", stock_id, interval)
    return 0

  ts = TimeSeries(key=apikey, output_format='pandas')
  df, meta_data = ts.get_intraday(stock_id, interval=interval, outputsize='full')

  if (output):
    if (not os.path.exists(output)):
      os.makedirs(output)
    fname = output + "/" + stock_id + '_' + str(df.index[0].date()) + '.csv'
    print(fname)
    df.to_csv(fname)

  df.head()
  df.columns = ['open','high','low', 'close', 'volume']
  df['open'] = pd.to_numeric(df['open'])
  df['high'] = pd.to_numeric(df['high'])
  df['low'] = pd.to_numeric(df['low'])
  df['close'] = pd.to_numeric(df['close'])
  df['volume'] = pd.to_numeric(df['volume'])

  df.index.names = ['dt']
  df.to_sql(stock_id.lower(), engine, if_exists='append')

def get_daily_data():
  for stock_id in tickers.split():
    print(stock_id)
    get_daily_ticker_data(stock_id, apiint, dummy=False, output="data")
    sleep(20)

def refresh_views():
  conn = psycopg2.connect(database=pg_conn.schema, user=pg_conn.login,
    password=pg_conn.password, host=pg_conn.host, port=pg_conn.port)

  conn.autocommit = True
  cursor = conn.cursor()

  for stock_id in tickers.split():
    viewname=stock_id.lower()+"view"
    sql = "REFRESH MATERIALIZED VIEW " + viewname
    print(sql)
    cursor.execute(sql)
    conn.commit()
  conn.close()

def refresh_mart():
  conn = psycopg2.connect(database=pg_conn.schema, user=pg_conn.login,
    password=pg_conn.password, host=pg_conn.host, port=pg_conn.port)

  conn.autocommit = True
  cursor = conn.cursor()

  sql = "REFRESH MATERIALIZED VIEW stocks_mart"
  print(sql)
  cursor.execute(sql)
  conn.commit()

  conn.close()

with DAG(dag_id="a_incremental_load", start_date=datetime(2023, 9, 1), schedule="0 0 * * *", max_active_runs=1) as dag:
  get_daily_data_task = PythonOperator(task_id="get_daily_data", python_callable = get_daily_data)
  refresh_views_task = PythonOperator(task_id="refresh_views", python_callable = refresh_views)
  refresh_mart_task = PythonOperator(task_id="refresh_mart", python_callable = refresh_mart)
  get_daily_data_task >> refresh_views_task >> refresh_mart_task
