import GetTicketPriceData as TicketService

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import os
import time
import requests
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta


# 인증 정보
SnowflakeID = Variable.get("SnowflakeID")
SnowflakePW = Variable.get("SnowflakePW")
SnowflakeAccount = Variable.get("SnowflakeAccount")
SnowflakeDatabase = Variable.get("SnowflakeDatabase")
SnowflakeSchema = Variable.get("SnowflakeSchema")

# Snowflake에서 필요한 데이터를 가져와서, Top 100(?)
def get_airplane_direction(**kwargs):
    # Connect to MySQL and fetch data
    
    sql = '''
        select ESTDEPARTUREAIRPORT, ESTARRIVALAIRPORT, COUNT(*) AS CNT
        FROM (select ESTDEPARTUREAIRPORT, ESTARRIVALAIRPORT, CALLSIGN
              from BIBIBIG.BRONZE_STATE_DATA.AIRCRAFT_DEPARTURE_ARRIVAL
              where CREATEAT BETWEEN DATEADD(DAY, -10, GETDATE()) AND DATEADD(DAY, 1, GETDATE())
                  AND ESTDEPARTUREAIRPORT IS NOT NULL
                  AND ESTARRIVALAIRPORT IS NOT NULL
                  AND CALLSIGN IS NOT NULL
              GROUP BY ESTDEPARTUREAIRPORT, ESTARRIVALAIRPORT, CALLSIGN)
        GROUP BY ESTDEPARTUREAIRPORT, ESTARRIVALAIRPORT
        ORDER BY CNT DESC
        '''
    
    with snowflake.connector.connect(user=SnowflakeID, 
                                     password=SnowflakePW, 
                                     account=SnowflakeAccount,
                                     database=SnowflakeDatabase,
                                     schema=SnowflakeSchema) as conn:
        with conn.cursor() as cursor:
            # SQL 실행
            cursor.execute(sql)
        
            # 결과를 Pandas DataFrame으로 가져오기
            df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description]).sort_values('CNT', ascending = False)

    kwargs['ti'].xcom_push(key='extracted_data', value=df)


def get_and_fetch_airticket_price(**kwargs):
    df = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='get_airplane_direction')
    
    # read meta data (1안 / 전체 리스트 사용하기)
    # meta_data = pd.read_csv('airport_code.csv')
    # airport_code_mapping_dict = {row[1]['ICAO'] : row[1]['IATA'] for row in meta_data.iterrows()}
    
    # read meta data (2안 / 국내에서 제공한 리스트만 사용하기)
    meta_data = pd.read_csv("https://raw.githubusercontent.com/bibibig-org/airbig/refs/heads/master/dags/airport_info_20231231_utf8.csv")
    airport_code_mapping_dict = {row[1]['공항코드2(ICAO)'] : row[1]['공항코드1(IATA)'] for row in meta_data.iterrows()}

    # code mapping
    df['ESTDEPARTUREAIRPORT'] = df['ESTDEPARTUREAIRPORT'].apply(lambda x : airport_code_mapping_dict[x] if x in airport_code_mapping_dict.keys() else None)
    df['ESTARRIVALAIRPORT'] = df['ESTARRIVALAIRPORT'].apply(lambda x : airport_code_mapping_dict[x] if x in airport_code_mapping_dict.keys() else None)
    
    # drop na
    df = df.dropna()
    
    # Top N (적절히 분배)
    df = df.iloc[:50]
    
    # 수집 date range
    departure_date_range = [(datetime.now() + timedelta(days=i)).strftime('%Y%m%d') for i in range(1,14)]
    
    for target_date in departure_date_range:

        # SearchList와 DepartureDate 변수를 적절하게 조정 / 적재 기간에 따라 설정정
        airticket_data = TicketService.GetBulkAirticketPrice(SearchList = [(row[1]['ESTDEPARTUREAIRPORT'], row[1]['ESTARRIVALAIRPORT']) for row in df.iterrows()],
                                                             DepartureDate = target_date)

        
        
        with snowflake.connector.connect(user=SnowflakeID, 
                                         password=SnowflakePW, 
                                         account=SnowflakeAccount,
                                         database=SnowflakeDatabase,
                                         schema=SnowflakeSchema) as conn:
            
            # make cursor
            cursor = conn.cursor()
            
            ################
            # Company Name #
            ################
            cursor.execute('select distinct COMPANY_CODE from AIRLINE_COMPANY')
            company_list = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            
            # drop duplicates
            dataframe = airticket_data['company_name']
            dataframe.columns = [i.upper() for i in dataframe.columns]
            dataframe = dataframe[~dataframe['COMPANY_CODE'].isin(company_list['COMPANY_CODE'].tolist())]
            
            
            if len(dataframe) > 0:
                insert_query = f"""
                                INSERT INTO AIRLINE_COMPANY ({','.join(dataframe.columns.tolist())})
                                VALUES ({','.join(['%s' for _ in dataframe.columns])})
                                """
                values = [tuple(row.map(lambda x: None if pd.isna(x) else x).tolist()) for _, row in dataframe.iterrows()]
                cursor.executemany(insert_query, values)

            
            ################
            #  Fare Type   #
            ################
            cursor.execute('select distinct FareType from AIRLINE_FARETYPE')
            faretype_list = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            
            # drop duplicates
            dataframe = airticket_data['fareType']
            dataframe.columns = [i.upper() for i in dataframe.columns]
            dataframe = dataframe[~dataframe['FARETYPE'].isin(faretype_list['FARETYPE'].tolist())]
            
            if len(dataframe) > 0:
                insert_query = f"""
                                INSERT INTO AIRLINE_FARETYPE ({','.join(dataframe.columns.tolist())})
                                VALUES ({','.join(['%s' for _ in dataframe.columns])})
                                """
                values = [tuple(row.map(lambda x: None if pd.isna(x) else x).tolist()) for _, row in dataframe.iterrows()]
                cursor.executemany(insert_query, values)


            ################
            #   Schedule   #
            ################
            dataframe = airticket_data['schedule']
            dataframe.columns = [i.upper() for i in dataframe.columns]
            dataframe['CREATEAT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_query = f"""
                            INSERT INTO AIRLINE_SCHEDULE ({','.join(dataframe.columns.tolist())})
                            VALUES ({','.join(['TO_TIMESTAMP(%s)' if col in ('CREATEAT', 'CreateAt') else '%s' for col in dataframe.columns])})
                            """
            values = [row.map(lambda x: None if pd.isna(x) else x).tolist() for _, row in dataframe.iterrows()]
            
            # batch insert
            cursor.executemany(insert_query, values)
            
            
            ################
            #     Fare     #
            ################
            dataframe = airticket_data['fare']
            if ('AgtCode' in dataframe.columns) and ('agtCode' in dataframe.columns):
                dataframe['AgtCode'] = dataframe['AgtCode'].combine_first(dataframe['agtCode'])
                dataframe = dataframe.drop('agtCode', axis = 1)
            dataframe.columns = [i.upper() for i in dataframe.columns]
            dataframe['CREATEAT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            insert_query = f"""
                            INSERT INTO AIRLINE_FARE ({','.join(dataframe.columns.tolist())})
                            VALUES ({','.join(['TO_TIMESTAMP(%s)' if col in ('CREATEAT', 'CreateAt') else '%s' for col in dataframe.columns])})
                            """
            values = [(row.map(lambda x: None if pd.isna(x) else x).tolist()) for _, row in dataframe.iterrows()]
            
            # batch insert
            cursor.executemany(insert_query, values)

            # cursor close
            cursor.close()
        
        # wait init
        time.sleep(20)
               
    


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
    'upload_ticket_price_naver',
    default_args=default_args,
    description='get airplane direction, get airplane ticket price, and upload to snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
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
    
    
