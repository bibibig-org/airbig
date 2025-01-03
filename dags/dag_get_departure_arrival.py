from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta, timezone
import requests
import json
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import snowflake.connector

'''
문서 : https://openskynetwork.github.io/opensky-api/rest.html

•	icao24: `str`
	•	항공기의 고유 ICAO 24비트 주소를 16진수 문자열 형식으로 나타낸 값입니다. 모든 문자는 소문자로 표시됩니다.
•	firstSeen: `int`
	•	항공편의 출발 시간이 Unix 시간(1970년 1월 1일부터 초 단위로 경과한 시간)으로 추정된 값입니다.
•	estDepartureAirport: `str`
	•	추정된 출발 공항의 ICAO 코드입니다. 공항을 식별할 수 없는 경우 `null`일 수 있습니다.
•	lastSeen: `int`
	•	항공편의 도착 시간이 Unix 시간으로 추정된 값입니다.
•	estArrivalAirport: `str`
	•	추정된 도착 공항의 ICAO 코드입니다. 공항을 식별할 수 없는 경우 `null`일 수 있습니다.
•	callsign: `str`
	•	항공기의 호출부호(최대 8자)입니다. 호출부호를 수신하지 못한 경우 `null`일 수 있습니다. 항공기가 비행 중 여러 호출부호를 전송하는 경우, 가장 자주 관측된 호출부호가 사용됩니다.
•	estDepartureAirportHorizDistance: `int`
	•	마지막으로 수신된 비행 위치와 추정된 출발 공항 간의 수평 거리(미터 단위)입니다.
•	estDepartureAirportVertDistance: `int`
	•	마지막으로 수신된 비행 위치와 추정된 출발 공항 간의 수직 거리(미터 단위)입니다.
•	estArrivalAirportHorizDistance: `int`
	•	마지막으로 수신된 비행 위치와 추정된 도착 공항 간의 수평 거리(미터 단위)입니다.
•	estArrivalAirportVertDistance: `int`
	•	마지막으로 수신된 비행 위치와 추정된 도착 공항 간의 수직 거리(미터 단위)입니다.
•	departureAirportCandidatesCount: `int`
	•	다른 가능한 출발 공항의 개수입니다. 이는 추정된 출발 공항 근처에 있는 다른 공항들을 나타냅니다.
•	arrivalAirportCandidatesCount: `int`
	•	다른 가능한 도착 공항의 개수입니다. 이는 추정된 도착 공항 근처에 있는 다른 공항들을 나타냅니다.

'''

# 인증 정보
OpenskyID = Variable.get("OpenskyID")
OpenskyPW = Variable.get("OpenskyPW")
SnowflakeID = Variable.get("SnowflakeID")
SnowflakePW = Variable.get("SnowflakePW")
SnowflakeAccount = Variable.get("SnowflakeAccount")
SnowflakeDatabase = Variable.get("SnowflakeDatabase")
SnowflakeSchema = Variable.get("SnowflakeSchema")


'''
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()
'''


def return_snowflake_conn():

    conn_info = {
        'user': SnowflakeID,
        'password': SnowflakePW,
        'account': SnowflakeAccount,
        'database': SnowflakeDatabase,
        'schema': SnowflakeSchema
    }
    
    conn = snowflake.connector.connect(user=conn_info['user'],
                                password=conn_info['password'],
                                account=conn_info['account'],
                                database=conn_info['database'],
                                schema=conn_info['schema'])

    return conn

@task
def extract_opensky():
    # 현재 시간과 하루 전 시간 계산
    now = datetime.utcnow()
    one_day_ago = now - timedelta(days=1)

    # 시작 시간과 종료 시간 설정 (하루 전 1시간 범위)
    begin = int((one_day_ago - timedelta(hours=1)).timestamp())
    end = int(one_day_ago.timestamp())

    url = f'https://opensky-network.org/api/flights/all?begin={begin}&end={end}'

    try:
        # API 요청 (인증 포함)
        response = requests.get(url, auth=(OpenskyID, OpenskyPW))

        # 응답 상태 확인
        if response.status_code == 200:
            data = response.json()  # JSON 데이터를 파싱

            # 데이터가 비어 있는 경우 처리
            if not data:
                print("No flight data available for the requested time range.")
                return []

            df_raw = pd.DataFrame(data)
            df_raw.columns = [
                "icao24",
                "firstSeen",
                "estDepartureAirport",
                "lastSeen",
                "estArrivalAirport",
                "callsign",
                "estDepartureAirportHorizDistance",
                "estDepartureAirportVertDistance",
                "estArrivalAirportHorizDistance",
                "estArrivalAirportVertDistance",
                "departureAirportCandidatesCount",
                "arrivalAirportCandidatesCount",
            ]    
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"An error occurred during request API: {e}")

    return df_raw
@task
def transform_opensky(df_transformed):
    df_transformed['createAt'] = datetime.now(timezone.utc)
    df_transformed['createAt'] = df_transformed['createAt'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)

    return df_transformed

@task
def load_opensky_to_snowflake(results, target_table):
    try:
        conn = return_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("BEGIN")

        # insert data
        insert_query = f"""
                        INSERT INTO {target_table} ({','.join(results.columns.tolist())})
                        VALUES ({','.join(['TO_TIMESTAMP(%s)' if col in ('CREATEAT', 'CreateAt') else '%s' for col in results.columns])})
                        """
        values = [(row.map(lambda x: None if pd.isna(x) else x).tolist()) for _, row in results.iterrows()]

        # batch insert
        cursor.executemany(insert_query, values)
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(e)
        raise RuntimeError(f"Error loading data to Snowflake: {e}")
    finally:
        conn.close()
        cursor.close()


with DAG(
    dag_id='opensky_flights_etl_dept_arrival_to_snowflake',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'OpenSkyAPI', 'Flights in Time Interval', 'aircraft departure and arrival data'],
    schedule='*/2 * * * *'
) as dag:
    target_table = "bibibig.bronze_state_data.aircraft_departure_arrival"
    
    raw_data = extract_opensky()
    transformed_data = transform_opensky(raw_data)
    load_opensky_to_snowflake(transformed_data, target_table)
