from airflow import DAG
from airflow.operators.python import PythonOperator

from movies_src.extract import Extract
from movies_src.transform import Transform
from movies_src.load import Load
from movies_src.connect import Connect
from datetime import datetime  
import warnings
warnings.filterwarnings('ignore')

def _extract():
    Extract() 

def _transform():
    Transform()

def _load():
    table_name = 'movies_airflow'
    c = Connect()
    connection_string = c.processConnection()
    Load(connection_string, table_name).load_to_sql()

with DAG(
    dag_id = 'movies',
    start_date = datetime(2023,1,1),
    schedule = '@daily',
    catchup = False
) as dag:
    
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = _extract
    )

    transform = PythonOperator(
        task_id = 'transform',
        python_callable = _transform
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = _load
    )

    extract >> transform >> load