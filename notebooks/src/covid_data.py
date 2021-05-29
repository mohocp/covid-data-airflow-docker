import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import subprocess
import sys
import json
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import psycopg2 
except:
    install('psycopg2-binary')
    import psycopg2
    
try:
    from sqlalchemy import create_engine
except:
    install('sqlalchemy')
    from sqlalchemy import create_engine
    
    
try:
    import pandas as pd 
except:
    install('pandas')
    import pandas as pd 

try:
    from pymongo import MongoClient 
except:
    install('pymongo')
    from pymongo import MongoClient

engine = create_engine('postgresql://airflow:airflow@postgres/Faker_DB')
client = MongoClient('mongo', 27017)

def extract_load():
    DF_Data = pd.read_sql_query('SELECT * FROM customers', engine)
    records = json.loads(DF_Data.T.to_json()).values()
    db = client.faker_db
    db.user_data.insert_many(records)

default_args = {
    'owner': 'mhd',
    'start_date': dt.datetime(2021, 5, 29),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
    
}

with DAG('move_data_from_postgrss_to_mongo',
        default_args=default_args,
        catchup=False,
        schedule_interval=timedelta(hours=24)) as dag:
    
        extract_load = PythonOperator(
            task_id='extract_load',
            python_callable=extract_load,
            provide_context=False
        )

extract_load
