from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


import requests
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta


# 인증 정보
OpenskyID = Variable.get("OpenskyID")
OpenskyPW = Variable.get("OpenskyPW")
SnowflakeID = Variable.get("SnowflakeID")
SnowflakePW = Variable.get("SnowflakePW")
SnowflakeAccount = Variable.get("SnowflakeAccount")
SnowflakeDatabase = Variable.get("SnowflakeDatabase")
SnowflakeSchema = Variable.get("SnowflakeSchema")
SnowflakeTableName = 'AIRPLANE_LOCATION'



#%%
# 모든 위치 상태값 검색 -> 호출 제한량 있음, 
# 일일 4000 크레딧 보유
# all 요청 시 4 크레딧 필요 ()
# 제한에 도달한 후에는 상태 코드 429 - Too Many Requests가 반환
# 헤더 X-Rate-Limit-Retry-After-Seconds는 크레딧/요청이 다시 사용 가능해질 때까지 몇 초가 걸리는지 정보 제공


def get_airplane_location(**kwargs):
    url = 'https://opensky-network.org/api/states/all'
    response = requests.get(url, auth=(OpenskyID, OpenskyPW))

    # 응답 확인
    data = response.json()

    df_all_state_vector = pd.DataFrame(data['states'])

    df_all_state_vector.columns =  ['icao24','callsign','origin_country',
                                    'time_position','last_contact','longitude',
                                    'latitude','baro_altitude','on_ground',
                                    'velocity','true_track','vertical_rate',
                                    'sensors','geo_altitude','squawk','spi',
                                    'position_source'] # ,'category']
    
    df_all_state_vector['createAt'] = data['time']
    

    return df_all_state_vector


# upload 함수
def upload_to_snowflake(conn_info, dataframe, table_name):  
    with snowflake.connector.connect(user=conn_info['user'], 
                                     password=conn_info['password'], 
                                     account=conn_info['account'],
                                     database=conn_info['database'],
                                     schema=conn_info['schema']) as conn:
        

        cursor = conn.cursor()
        
        # insert data
        insert_query = f"""
                        INSERT INTO AIRPLANE_LOCATION ({','.join(dataframe.columns.tolist())})
                        VALUES ({','.join(['TO_TIMESTAMP(%s)' if col in ('CREATEAT', 'CreateAt') else '%s' for col in dataframe.columns])})
                        """
        values = [(row.map(lambda x: None if pd.isna(x) else x).tolist()) for _, row in dataframe.iterrows()]
        
        # batch insert
        cursor.executemany(insert_query, values)
        
        cursor.close()
        


def put_airplane_data(**kwargs):
    # get dags data
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='get_airplane_data')
    
    conn_info = {'user' : SnowflakeID,
                 'password' : SnowflakePW,
                 'account' : SnowflakeAccount,
                 'database' : SnowflakeDatabase,
                 'schema' : SnowflakeSchema}
    
    # snowflake only used capital letter colname
    df.columns = [col.upper() for col in df.columns]
    df['CREATEAT'] = pd.to_datetime(df['CREATEAT'], unit='s')
    df['CREATEAT'] = df['CREATEAT'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
    
    upload_to_snowflake(conn_info, df, SnowflakeTableName)
    
    

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
    'upload_location_data_sql_insert',
    default_args=default_args,
    description='Fetch API data, process into DataFrame, and upload to snowflake',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    fetch_and_process_data_task = PythonOperator(
        task_id = 'get_airplane_data',
        python_callable = get_airplane_location,
    )

    upload_partitioned_data_task = PythonOperator(
        task_id = 'upload_airplane_data',
        python_callable = put_airplane_data,
    )

    fetch_and_process_data_task >> upload_partitioned_data_task
    