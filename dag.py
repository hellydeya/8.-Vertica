from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime
import pendulum
import boto3
import pandas as pd
import json 
import vertica_python
import logging

logger = logging.getLogger(__name__)

def load_from_s3():
    file = 'group_log.csv'
    session = boto3.session.Session()
    s3_client = session.client(**json.loads(Variable.get("s3")))
    s3_client.download_file(
            Bucket='--- hello ---',
            Key=--- hello ---,
            Filename=--- hello ---)


    logging.info(s3_client.head_object(Bucket='sprint6', Key=file),)
    logging.info(f'\nHeader in {file}:\n{pd.read_csv(file, nrows=10)}')


def load_stg(conn_info=json.loads(Variable.get("conn_info"))):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    truncate table --- hello ---;""")
        cur.execute("""
                    copy --- hello --- (group_id , user_id, user_id_from, event, datetime)
                    from local '--- hello ---' delimiter ',';                     
                    """) 
        logging.info(f'{cur.fetchall()} records have been copied')


with DAG(
        'vertica',
        schedule_interval=None,
        start_date=datetime.now(),
        catchup=False,
        tags=['vertica']
        ) as dag:

    load_from_s3 = PythonOperator(
        task_id='load_from_s3',
        python_callable=load_from_s3)

    load_stg = PythonOperator(
        task_id='load_stg',
        python_callable=load_stg)



load_from_s3 >> load_stg 
