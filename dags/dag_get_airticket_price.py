import GetTicketPriceData as TicketService
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import requests
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta


# Snowflake에서 필요한 데이터를 가져와서, Top 100(?)
def get_airplane_direction(**kwargs):
    # Connect to MySQL and fetch data
    
    records = pd.DataFrame()
    kwargs['ti'].xcom_push(key='extracted_data', value=records)


def get_and_fetch_airticket_price(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='get_airplane_direction')
    
    
    # SearchList와 DepartureDate 변수를 적절하게 조정 / 적재 기간에 따라 설정정
    airticket_data = TicketService.GetBulkAirticketPrice(SearchList = [("GMP","CJU"), ('ICN','JFK'), ('ICN','HAN'), ('ICN','HNL'), ('ICN','PEK'), ('ICN','NRT')], DepartureDate = "20250211")

    # 적재 코드
    pass
    


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'upload_ticket_price',
    default_args=default_args,
    description='get airplane direction, get airplane ticket price, and upload to snowflake',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    fetch_and_process_data_task = PythonOperator(
        task_id = 'get_airplane_direction',
        python_callable = get_airplane_direction,
    )

    upload_partitioned_data_task = PythonOperator(
        task_id = 'get_and_fetch_airticket_price',
        python_callable = get_and_fetch_airticket_price,
    )

    fetch_and_process_data_task >> upload_partitioned_data_task
    
    
    
    