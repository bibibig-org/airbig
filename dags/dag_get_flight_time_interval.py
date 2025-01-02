from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta


# 인증 정보
OpenskyID = Variable.get("OpenskyID")
OpenskyPW = Variable.get("OpenskyPW")
SnowflakeID = Variable.get("SnowflakeID")
SnowflakePW = Variable.get("SnowflakePW")
SnowflakeAccount = Variable.get("SnowflakeAccount")
SnowflakeDatabase = Variable.get("SnowflakeDatabase")
SnowflakeSchema = Variable.get("SnowflakeSchema")
SnowflakeTableName = 'AIRPLANE_TIMELINE'


@task
def get_airplane_location():
    # 현재 시간 (UTC 기준)
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    # begin과 end를 타임스탬프(초 단위)로 계산
    end = int(one_hour_ago.timestamp())  # 하루 전 시간
    begin = int(one_hour_ago - timedelta(days=1))  # 현재 시간

    url = f'https://opensky-network.org/api/states/all?begin={begin}&end={end}'
    response = requests.get(url, auth=(OpenskyID, OpenskyPW))

    # 응답 확인
    data = response.json()

    df_all_state_vector = pd.DataFrame(data['states'])

    df_all_state_vector.columns = ['icao24', 'callsign', 'origin_country',
                                   'time_position', 'last_contact', 'longitude',
                                   'latitude', 'baro_altitude', 'on_ground',
                                   'velocity', 'true_track', 'vertical_rate',
                                   'sensors', 'geo_altitude', 'squawk', 'spi',
                                   'position_source']

    df_all_state_vector['createAt'] = data['time']

    return df_all_state_vector


@task
def upload_to_snowflake(dataframe):
    conn_info = {
        'user': SnowflakeID,
        'password': SnowflakePW,
        'account': SnowflakeAccount,
        'database': SnowflakeDatabase,
        'schema': SnowflakeSchema
    }

    # snowflake only used capital letter colname
    dataframe.columns = [col.upper() for col in dataframe.columns]
    dataframe['CREATEAT'] = pd.to_datetime(dataframe['CREATEAT'], unit='s')
    dataframe['CREATEAT'] = dataframe['CREATEAT'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)

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
    'upload_flihgt_timeInterval_data_to_snowflake',
    default_args=default_args,
    description='Fetch OpenSky API data for Flights within a specified time interval (from 1 hour ago to one day ago) and upload to snowflake',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # 순차적으로 작업 정의
    df = get_airplane_location()  # 데이터 가져오기
    upload_to_snowflake(df)  # 데이터 업로드
