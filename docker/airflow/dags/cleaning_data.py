import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import logging
import pandas as pd
import psycopg2 as db
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_data():
    df = pd.read_csv('/opt/airflow/data/raw/scooter.csv')
    df.drop(columns=['region_id'], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df['started_at'] = pd.to_datetime(df['started_at'],
                                      format='%m/%d/%Y %H:%M')
    df.to_csv('/opt/airflow/data/processed/cleanscooter.csv')


def filter_data():
    df = pd.read_csv('/opt/airflow/data/processed/cleanscooter.csv')
    fromd = '2019-05-23'
    tod = '2019-06-03'
    tofrom = df[(df['started_at'] > fromd) & (df['started_at'] < tod)]
    tofrom.to_csv('/opt/airflow/data/processed/may23-june3.csv')


def insert_data():
    user = 'postgres'
    password = 'postgres'
    host = 'pgdatabase'
    port = '5432'
    database = 'cycling_data'
    connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    # SQLAlchemy engine
    engine = create_engine(connection_str)
    try:
        with engine.connect() as connection:
            print('Successfully connected to the PostgreSQL database')
            df = pd.read_csv('/opt/airflow/data/processed/may23-june3.csv')
            df.to_sql('scooter', engine, if_exists='replace', index=False)
            logger.info("Data inserted into PostgreSQL")
    except Exception as ex:
        print(f'Sorry failed to connect: {ex}')


default_args = {
    'owner': 'viannTH',
    'start_date': dt.datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('cleaning_data', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    cleanData = PythonOperator(task_id='clean', python_callable=clean_data)
    selectData = PythonOperator(task_id='filter', python_callable=filter_data)
    insertData = PythonOperator(task_id='insert', python_callable=insert_data)

    cleanData >> selectData >> insertData
