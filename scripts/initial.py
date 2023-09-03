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
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

apikey = Variable.get("apikey")
apiint = Variable.get("apiint")
tickers = Variable.get("tickers")
pg_conn = BaseHook.get_connection(Variable.get("conn_id"))

engine = create_engine('postgresql://' + pg_conn.login + ':' + \
  pg_conn.password + '@' + pg_conn.host + ':' + str(pg_conn.port) + \
  '/' + pg_conn.schema)

def get_month_slice(stock_id, interval, slice, dummy=False, output=None):
  if (dummy):
    print("get_month_slice", stock_id, interval, slice)
    return 0

  ts = TimeSeries(key=apikey, output_format='csv')
  data = ts.get_intraday_extended(symbol=stock_id, interval=interval, slice = slice)
  df = pd.DataFrame(list(data[0]))
  header_row=0
  df.columns = df.iloc[header_row]
  df = df.drop(header_row)
  df['time'] = pd.to_datetime(df['time'])
  df['open'] = pd.to_numeric(df['open'])
  df['high'] = pd.to_numeric(df['high'])
  df['low'] = pd.to_numeric(df['low'])
  df['close'] = pd.to_numeric(df['close'])
  df['volume'] = pd.to_numeric(df['volume'])
  df.set_index('time', inplace=True)

  if (output):
    if (not os.path.exists(output)):
      os.makedirs(output)
    # имя файла по шаблону, например GOOGL_year2_month12.csv
    fname = output + "/" + stock_id + '_' + slice + '.csv'
    print(fname)
    df.to_csv(fname)

  df.index.names = ['dt']
  df.to_sql(stock_id.lower(), engine, if_exists='append')

def load_ticker_data(ticker):
  # Загружает полный дамп данных за два года для данного тикера (24 куска помесячно)
  for current_year in [1, 2]:
    for current_month in range(1, 13):
      month_slice = 'year' + str(current_year) + 'month' + str(current_month)
      print(ticker, month_slice)
      get_month_slice(ticker, apiint, month_slice, output="data")
      # на стандартном (бесплатном) плане  ограничение 5 API запросов в минуту, поэтому спим между запросами
      sleep(20)

def load_data():
  for stock_id in tickers.split():
    print(stock_id)
    load_ticker_data(stock_id)

def create_view(ticker):
  conn = psycopg2.connect(database=pg_conn.schema, user=pg_conn.login,
    password=pg_conn.password, host=pg_conn.host, port=pg_conn.port)

  conn.autocommit = True
  cursor = conn.cursor()
  viewname=ticker.lower()+"view"
  temp_table=ticker.lower()+"temp_table"
  temp_table_wtime=ticker.lower()+"temp_table_wtime"
  sql = """
  CREATE MATERIALIZED VIEW {viewname} AS
  WITH
  {temp_table} AS (
  SELECT *, date(dt) AS day, dt::time AS time FROM {ticker} ORDER BY dt
  ),
  {temp_table_wtime} AS (
  SELECT *, min(dt::time) OVER w AS open_time, max(dt::time) OVER w AS close_time,
      max(volume) OVER w AS maxvolume, max(high) OVER w AS maxrate, min(low) OVER w AS minrate 
  FROM {temp_table}
  WINDOW w AS (PARTITION BY day)
  )
  SELECT * FROM {temp_table_wtime};
  """

  cursor.execute(psycopg2.sql.SQL(sql).format(
    viewname=psycopg2.sql.Identifier(viewname),
    ticker=psycopg2.sql.Identifier(ticker.lower()),
    temp_table=psycopg2.sql.Identifier(temp_table),
    temp_table_wtime=psycopg2.sql.Identifier(temp_table_wtime)))
  conn.commit()
  conn.close()

def create_views():
  for stock_id in tickers.split():
    print(stock_id)
    create_view(stock_id)

def create_mart(**kwargs):
  dummy=kwargs['dummy']
  conn = psycopg2.connect(database=pg_conn.schema, user=pg_conn.login,
    password=pg_conn.password, host=pg_conn.host, port=pg_conn.port)

  conn.autocommit = True
  cursor = conn.cursor()
  mart_query = psycopg2.sql.SQL("CREATE MATERIALIZED VIEW stocks_mart AS (with")
  sql_template = """
  {tckr}_sum_volume AS (
  select day, sum(volume) as sum_volume from {tckr}view group by day order by day
  ),
  {tckr}_open AS (
  select day, open from {tckr}view where time=open_time
  ),
  {tckr}_close AS (
  select day, close from {tckr}view where time=close_time
  ),
  {tckr}_volume AS (
    select day, min(time) as maxvolume_time from {tckr}view where volume=maxvolume group by day order by day
  ),
  {tckr}_maxrate AS (
    select day, min(time) as maxrate_time from {tckr}view where high=maxrate group by day order by day
  ),
  {tckr}_minrate AS (
    select day, min(time) as minrate_time from {tckr}view where low=minrate group by day order by day
  ),
  {tckr}_mart AS (
  select '{tckr}' as ticker, {tckr}sv.*, {tckr}o.open, {tckr}c.close, (({tckr}c.close-{tckr}o.open)*100)/{tckr}o.open as percent_diff, {tckr}v.maxvolume_time, {tckr}max.maxrate_time, {tckr}min.minrate_time
  from {tckr}_sum_volume {tckr}sv
  join {tckr}_open {tckr}o on {tckr}sv.day={tckr}o.day
  join {tckr}_close {tckr}c on {tckr}sv.day={tckr}c.day
  join {tckr}_volume {tckr}v on {tckr}sv.day={tckr}v.day
  join {tckr}_maxrate {tckr}max on {tckr}sv.day={tckr}max.day
  join {tckr}_minrate {tckr}min on {tckr}sv.day={tckr}min.day
  order by {tckr}sv.day
  ),"""

  sql_union = """
  select * from {tckr}_mart
  UNION"""

  sql_footer = psycopg2.sql.SQL("""
  )
  select row_number() over () as id, * from all_mart
  )
  """)

  for stock_id in tickers.split():
    sql_cte = psycopg2.sql.SQL(sql_template).format(
    tckr=psycopg2.sql.Identifier(stock_id.lower()))
    mart_query = mart_query + sql_cte

  mart_query = mart_query + psycopg2.sql.SQL(" all_mart AS (")

  for stock_id in tickers.split()[:-1]:
    sql_cte = psycopg2.sql.SQL(sql_union).format(
    tckr=psycopg2.sql.Identifier(stock_id.lower()))
    mart_query = mart_query + sql_cte

  sql_cte = psycopg2.sql.SQL(" select * from {tckr}_mart order by day").format(
    tckr=psycopg2.sql.Identifier(tickers.split()[-1].lower()))
  mart_query = mart_query + sql_cte + sql_footer
  mart_query = mart_query.as_string(conn).replace('"', '')

  if (dummy):
    print(mart_query)
  else:
    cursor.execute(mart_query)

  conn.commit()
  conn.close()

def create_database():
  conn = psycopg2.connect(user=pg_conn.login,
    password=pg_conn.password, host=pg_conn.host, port=pg_conn.port)

  conn.autocommit = True
  cursor = conn.cursor()
  sql = "CREATE DATABASE " + pg_conn.schema
  print(sql)
  cursor.execute(sql)
  conn.commit()
  conn.close()


with DAG(dag_id="a_initial_load", start_date=datetime(2022, 12, 28), schedule="@once", max_active_runs=1) as dag:
  create_database_task = PythonOperator(task_id="create_database", python_callable = create_database)
  load_data_task = PythonOperator(task_id="load_data", python_callable = load_data)
  create_views_task = PythonOperator(task_id="create_views", python_callable = create_views)
  create_mart_task = PythonOperator(task_id="create_mart", op_kwargs={'dummy': False}, python_callable = create_mart)
  create_database_task >> load_data_task >> create_views_task >> create_mart_task
