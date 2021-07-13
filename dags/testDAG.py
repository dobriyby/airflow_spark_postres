import datetime
import time
import requests
import pandas as pd
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from pandas import DataFrame
from datetime import date
import os

args = {
    'owner': 'dimon',
    'start_date':datetime.datetime(2018, 11, 1),
    'provide_context':True
}


def extract_data(**kwargs):
    print('extract_data')
    cur_date = date.today()
    # database hook
    db_hook = PostgresHook(postgres_conn_id='postgres_default')
    db_conn = db_hook.get_conn()
    db_cursor = db_conn.cursor()
    
    sql = "SELECT * FROM fct_events"
    df = pd.read_sql(sql, db_conn)
    df = df.drop_duplicates()
    df['event_dt'] = pd.to_datetime(df['event_dt'], format='%Y-%m-%d')
    print(df.head())

    df_out = pd.DataFrame(df.groupby(['event_dt']).size().reset_index(name = "count").sort_values('event_dt'))
    df_out = pd.DataFrame(df['event_dt']).drop_duplicates()
    df_out.sort_values(by=['event_dt'], inplace=True)

    print(df_out)
    list_countSUM = []
    list_count = []
    countClient = 0
    for index, rec in df_out.iterrows():
        countUP = len(df[(df['event_dt'] == rec['event_dt']) & (df['event_type_id'] == 1)])
        countDOWN = len(df[(df['event_dt'] == rec['event_dt']) & (df['event_type_id'] == 2)])
        countSUM = countUP - countDOWN
        list_countSUM.append(countSUM)
        list_count.append(countClient)
        countClient = countClient + countSUM
        
    df_out['countClient'] = list_count
    df_out['countINC'] = list_countSUM
    df_out['date_report'] = cur_date
    print(df_out)
    
    #save report to db
    db_cursor.execute(f"delete from fct_events_report where date_report = '{cur_date}' ")
    for i,row in df_out.iterrows():
       sql = "INSERT INTO fct_events_report (event_dt, countClient, countINC, date_report) VALUES ('"+row.event_dt.strftime("%Y-%m-%d")+"', "+str(row['countClient'])+",  "+str(row['countINC'])+", '"+row['date_report'].strftime("%Y-%m-%d")+"' )"
       db_cursor.execute(sql)
    db_conn.commit()
        
def transform_data(**kwargs):
    print('transform_data')
    

def load_data(**kwargs):
    print('load_data')
    
    
with DAG('testDAG', description='testDAG', schedule_interval='*/1 * * * *',  catchup=False,default_args=args) as dag: #0 * * * *   */1 * * * *
       extract_data    = PythonOperator(task_id='extract_data', python_callable=extract_data)
       transform_data  = PythonOperator(task_id='transform_data', python_callable=transform_data)
       create_table       = PostgresOperator(
                                task_id="create_table",
                                postgres_conn_id="postgres_default",
                                sql="""
                                    CREATE TABLE IF NOT EXISTS fct_events_report (
                                    event_dt date  NOT NULL,
                                    countClient int NOT NULL,
                                    countINC int NOT NULL,
                                    date_report date  NOT NULL);
                                """,
                                )
       load_data       = PythonOperator(task_id='load_data', python_callable=load_data)     
       
create_table >> extract_data >> transform_data  >> load_data