# ✈️ 전 세계 비행 모니터링 및 공항 대시보드 ✈️ 

# 목차
1. [프로젝트 개요](#1-프로젝트-개요)
   - [주제](#1-1-주제)
   - [주제 선정 이유](#1-2-주제-선정-이유)
2. [프로젝트 최종 목표](#2-프로젝트-최종-목표)
3. [활용 기술 및 프레임워크](#3-활용-기술-및-프레임워크)
4. [프로젝트 내용](#4-프로젝트-내용)
   - [Infra](#infra)
   - [Data Pipeline](#data-pipeline)
   - [Data](#data)
     - [ERD & 데이터 리니지](#erd--데이터-리니지)
     - [데이터 수집](#데이터-수집)
   - [Dashboard](#dashboard)
   - [Cowork Tools](#cowork-tools)
5. [프로젝트 결과](#5-프로젝트-결과)
   - [주요 시각화 결과](#주요-시각화-결과)
6. [결론 및 향후 개선사항](#6-결론-및-향후-개선사항)
   - [결론](#결론)
   - [향후 개선사항](#향후-개선사항)

## 1. 프로젝트 개요
### 📖 1-1. 주제
OpenSky API를 활용하여 실시간 비행기의 위치를 추적하고, 이를 대시보드에서 시각화하여 사용자에게 제공하는 시스템을 구축.

### ✔️ 1-2. 주제 선정 이유
항공 산업은 글로벌화된 세상에서 중요한 역할을 담당하며, 전 세계적으로 수많은 비행기가 동시에 운항 중입니다. 이러한 환경에서 실시간 비행 추적은 항공 안전, 공항 관리, 그리고 항공기 경로 최적화를 위한 중요한 데이터를 제공합니다. 본 프로젝트는 OpenSky API를 활용하여 비행기의 실시간 위치 데이터를 시각화함으로써, 항공 산업의 데이터 활용 가능성을 탐구하고 공항 운영 및 사용자 경험 개선에 기여하고자 합니다.

1. **실시간 데이터 활용 경험**: 센서 기반 데이터를 수집, 처리, 시각화하는 과정을 경험하며, 데이터 엔지니어링 및 대시보드 설계 역량을 강화하고자 합니다.
2. **항공 산업의 중요성**: 항공기의 위치 정보를 추적하는 기술은 항공 운항의 안전성과 효율성을 높이는 데 기여할 수 있습니다. 이를 통해 항공 산업에서 데이터 활용의 가능성을 검토합니다.
3. **사용자 중심의 대시보드 개발**: 공항 관리자, 항공 관제사, 일반 사용자 등 다양한 이해관계자에게 필요한 직관적이고 유용한 대시보드를 설계하여 데이터 활용의 실질적인 사례를 제시합니다.

## 2. 프로젝트 최종 목표
1. **실시간 비행 데이터 수집 및 시각화**
    
    OpenSky API를 활용하여 전 세계 비행기의 실시간 위치 데이터를 효과적으로 수집하고 이를 직관적인 지도 기반 인터페이스로 시각화하여 대시보드에 나타냅니다.
    
2. **다양한 사용자 요구를 반영한 대시보드 설계**
    
    공항 관리자, 항공 관제사 등 다양한 이해관계자를 대상으로 유용한 정보와 분석 도구를 제공하는 대시보드를 개발합니다.
    
3. **확장 가능한 데이터 파이프라인 구축**
    
    데이터를 배치형태로 안정적으로 처리하고 대용량 데이터를 안정적으로 저장하는 데이터 웨어하우스를 사용하여 대용량 데이터 저장에 맞춤인 데이터 파이프라인을 구현하여 대시보드의 신뢰성을 보장합니다.
    
4. **항공 데이터 활용 가능성 제시**
    
    프로젝트를 통해 항공 데이터의 활용 가능성과 잠재적 가치를 제시하여 데이터 기반 의사결정 및 서비스 개선에 기여합니다.

## 3. 활용 기술 및 프레임워크
| 카테고리               | 기술/도구                                |
|--------------------|----------------------------------------|
| **개발 언어**          | Python3                                 |
| **협업 툴**            | Slack, Zep                              |
| **데이터 수집**         | Request, RestAPI (OpenSky API)          |
| **데이터 레이크/데이터웨어하우스** | PostgreSQL, Snowflake             |
| **데이터 ETL**         | Airflow                                 |
| **인프라**             | AWS (EC2)                               |
| **대시보드**            | Tableau                                 |


## 4. 프로젝트 내용
### 🏗️ Infra
![비비빅 drawio](https://github.com/user-attachments/assets/0165c510-b416-44db-bebf-4bc2cf50a81c)


### Data Pipeline
**전체적인 데이터 흐름은 다음과 같습니다.**

1. OpenSky REST API에서 전세계 항공 정보 데이터를 크롤링.
2. 데이터를 Snowflake로 읽어와 데이터를 목적에 맞는 테이블 생성 및 업데이트.
3. Airflow를 활용해 데이터 파이프라인을 구성하며, EC2 위에 Docker를 생성
4. Airflow DAG를 5분, 1시간 간격 등으로 설정하여 데이터 적재
5. Snowflake와 Tableau를 연동하여 대시보드 시각화 및 분석


### 💽 Data
#### 1. ERD & 데이터 리니지
![비비빅4](https://github.com/user-attachments/assets/62cc8bf4-cae7-43d5-b1a5-78345f4d0aa0)


#### 2. 데이터 수집
- 요약

| Table Type | Table Name                  | Comment                                    | Source                                |
|------------|-----------------------------|--------------------------------------------|---------------------------------------|
| FACT       | AIRPLANE_LOCATION            | 항공기의 실시간 위치 값                    | OpenSky REST API                      |
| FACT       | AIRPLANE_NOW_LOCATION        | 항공기의 실시간 위치 값                    | OpenSky REST API                      |
| FACT       | AIRPLANE_DEPARTURE_ARRIVAL   | 조회 시간에 해당하는 항공기의 출도착 정보 | OpenSky REST API                      |
| DIM        | AIRPORT_LOCATION             | IATA, ICAO 코드와 위/경도 값을 매핑하기 위한 테이블 | [github/ip2location/ip2location-iata-icao](https://github.com/ip2location/ip2location-iata-icao) |
| DIM        | AIRPORT_INFO                 | IATA, ICAO 코드에 해당하는 공항 메타 데이터 테이블 | 국토교통부_세계공항_정보(공공데이터) https://www.data.go.kr/data/3051587/fileData.do?recommendDataYn=Y |

- **(FACT) AIRPLANE_LOCATION, AIRPLANE_NOW_LOCATION**
    - Airflow Dag (5분 간격)을 통해 실시간 데이터를 적재
    - 코드 
        - https://github.com/bibibig-org/airbig/blob/master/dags/dag_get_current_location_type2.py
    - Dag 구성
        - OpenSkyAPI에 `/states/all`을 요청
        - Snowflake Conn을 이용해 INSERT 문을 작성, excutemany로 한번에 적재
        - AIRPLANE_NOW_LOCATION 테이블은 TRUNCATE TABLE을 통해 테이블을 비운 뒤 현재 데이터만 적재
- **(FACT) AIRPLANE_DEPARTURE_ARRIVAL**
    - Airflow Dag (1시간 간격)을 통해 (현재로부터 25시간 ~ 24시간 전) 데이터를 적재
        - AIRPLANE_DEPARTURE_ARRIVAL 데이터는 Opensky측에서 일 배치로 적재되고 있어, 24시간 이전 데이터는 반환하지 않음.
    - 코드
        - https://github.com/bibibig-org/airbig/blob/master/dags/dag_get_departure_arrival.py
    - Dag 구성
        - OpenSkyAPI에 `/api/flights/all`을 요청
        - Snowflake Conn을 이용해 INSERT 문을 작성, excutemany로 한번에 적재
- **(DIM) AIRPORT_LOCATION, AIRPORT_INFO**
    - Snowflake에 CSV를 통한 1회성 적재 실시


### 📋 Dashboard
<img width="1042" alt="image" src="https://github.com/user-attachments/assets/09f8f459-4d15-4655-8dae-07b7a6b32968" />

![image (1)](https://github.com/user-attachments/assets/ea5b7f7b-1ecd-424a-83f7-ec56f618d0f2)

- **실시간 비행기 위치 정보 Chart**
    - 현 시각 비행 중인 비행기의 위치 정보를 지도에 표시
 
| Dimension | 항공기 |
|----------------|-----|
| **X axis**     | 경도 |
| **Y axis**     | 위도 |

- **실시간 비행 국가 순위**
    - 현 시각 비행 중인 비행기의 국적 정보 순위를 표현

|                |           |
|----------------|-----------|
| **X axis**     |Count|
| **Y axis**     | 비행기의 국적 |

- *공항 출도착 비행 정보**

![image (2)](https://github.com/user-attachments/assets/d8e70dd8-af6c-4510-ae27-996e74cab67e)

- **공항 별 / 국적 별 출발 비행기 수**
    - 해당 시각대에 출발한 비행기의 출발 공항/ 출발 국가를 표현

| Dimension      | 공항/국가 |
|----------------|-----|
| **X axis**     | 시간 (년-월-일-시) |
| **Y axis**     | Count |


- **공항 별 / 국가 별 출발 - 도착 정보**
    - 비행기가 가장 많이 비행한 경로(국가 - 국가 / 공항 - 공항) 정보를 표현

| Dimension      | 공항/국가 |
|----------------|-----|
| **X axis**     | Count |
| **Y axis**     | 출발 공항 - 도착 공항 |


- **항공 포화도**
    - 최근 1개월 간 비행기가 취항하고 있는 항공 노선(경로) 수

|                |           |
|----------------|-----------|
| **X axis**     | CallSign(비행기 호칭)     |
| **Y axis**     | Count |

- **비행기 항공 경로**

<img width="1044" alt="비행기_항공_경로" src="https://github.com/user-attachments/assets/178f9380-c91f-4ce9-ae48-7e0162fbe881" />

![비행기_항공_경로_2](https://github.com/user-attachments/assets/0eea9949-8d89-44d8-a985-2c81d30af7ab)

- **비행기 항공 경로 Chart**
    - 실시간 비행 경로는 비행 중인 항공기의 출발 - 도착 공항 위/경도 기준으로 작성
    - 현 시각 비행 중인 비행기의 항공 경로를 Line Chart로 시각화

| Line      | 비행기의 경로 (출발 - 도착) |
|----------------|-----|
| **X axis**     | 경도 |
| **Y axis**     | 위도 |


### 🗣️Cowork Tools
- **Notion**: 코드 버전 관리 및 협업
  - 1. 매일 오전 10:00 스크럼 회의 진행하여 Daily to do 작성 및 진행 상황 보고
  - 2. 프로젝트 일정 <img width="843" alt="노션_진행상황" src="https://github.com/user-attachments/assets/f02d3e38-aa85-4cde-a01a-cb3070945e2b" />
  -  <img width="752" alt="노션_달력" src="https://github.com/user-attachments/assets/12a7419a-71e2-46dc-801f-5f6e33f9d38e" />
  - 3. 주제 선정 회의 <img width="543" alt="주제선정회의_비비빅" src="https://github.com/user-attachments/assets/c1ec8ab1-cd38-4310-9064-89aa4aa4895c" />

- **Slack**: 팀 내 커뮤니케이션
   - 1. 팀채널에서 비동기적 커뮤니케이션
   - 2. 팀채널 캔버스에서 필요한 공유 사항 작성 <img width="824" alt="슬랙_팀채널" src="https://github.com/user-attachments/assets/de4f3abd-2f72-42e6-880d-c37699b8a37b" />

- **Slack**: 회의 채널
 - <img width="310" alt="비비빅_슬랙_간이회의" src="https://github.com/user-attachments/assets/06ab6384-f33e-4c54-9589-54f90911a5af" />

## 5. 프로젝트 결과
1. **실시간 비행 데이터 수집 및 시각화**
    - OpenSky API를 사용하여 전 세계 비행기의 실시간 위치 데이터를 성공적으로 수집하였습니다.
    - 수집된 데이터를 처리하여 비행기의 현재 위치(위도, 경도), 경로 등의 정보를 직관적으로 시각화하였습니다.
    - 태블로(Tableau)를 활용한 지도 기반 대시보드를 구현하여 비행 경로를 직관적으로 표현하였습니다.
2. **확장 가능한 데이터 파이프라인 구축**
    - Apache Airflow를 활용하여 데이터 수집, 변환(ETL), 저장 작업을 자동화하고 관리 가능한 데이터 파이프라인을 설계하였습니다.
    - 데이터를 배치(batch) 처리 방식으로 저장하고, 대용량 데이터를 처리하기 위해 ‘Snowflake’를 활용하여 안정적인 파이프라인을 환경 구축하였습니다.
    - 데이터 처리 및 저장 과정에서의 트랜잭션 안정성을 보장하여 실시간 대시보드의 신뢰성을 확보하였습니다.
3. **항공 데이터 활용 가능성 검증**
    - 본 프로젝트는 실시간 항공 데이터의 활용 가능성을 입증하며, 공항 운영 최적화 및 항공 안전성 향상을 위한 데이터 기반 의사결정에 기여할 수 있는 가능성을 제시하였습니다.

### 주요 시각화 결과
1. **실시간 비행기 위치 정보 시각화**
    - 전 세계 항공기의 실시간 위경도 데이터를 지도에 시각화하여, 현재 운항 중인 항공기들의 위치를 한눈에 확인 가능.
2. **비행기 항공 경로 시각화**
    - 특정 항공편의 출발지부터 도착지까지의 경로를 시각적으로 표시하여 경로를 추적할 수 있도록 구현.
3. **공항 출도착 비행 정보 시각화**
    - 주요 공항의 출발 및 도착 항공편 현황을 제공하며, 공항별 혼잡도 및 운영 상황을 생각해볼 수 있도록 설계.
  

  
## 6. 결론 및 향후 개선사항

### **결론**

본 프로젝트는 OpenSky API를 통해 실시간 항공 데이터를 수집하고 이를 대시보드로 시각화하여 사용자와 공항 운영자에게 인사이트를 제공하는 시스템을 구축하였습니다.

실시간 비행기 위치 정보, 항공 경로, 공항 출도착 정보 등을 직관적으로 제공함으로써 항공 산업에서 데이터 활용을 통한 인사이트를 도출했습니다.

또한, 본 프로젝트를 통해 실시간 데이터 수집, 처리, 저장, 시각화 전반에 걸친 기술을 통합적으로 활용함으로써 데이터 엔지니어링의 실무적 역량을 강화하였습니다.

개발된 대시보드는 실시간 항공 데이터를 효과적으로 제공하여 항공 산업의 데이터 활용 가능성을 제시하며, 공항 운영과 사용자 경험을 개선할 수 있는 기반을 마련하였습니다.


### 향후 개선사항

1. **항공권 데이터 수집 코드 고도화**
    - a. 현재 Github 내 GetTicketPriceData.py와, dag_get_airticket_price.py가 구현되어 있음
        - i. 코드 - https://github.com/bibibig-org/airbig/blob/master/dags/GetTicketPriceData.py
        - ii. 설명 : 네이버 항공권 검색에 POST 요청을 통해 국내/외의 항공권을 조회하는 서비스
    - b. 해당 서비스를 통해 가장 많이 이용하는 항공 노선 100개의 평균 구매 가격을 구하고자 하였으나, 네이버 항공권 크롤링 시 150건 이상의 POST 요청을 보내면 약 1시간의 Time Out이 내부적으로 존재하는 점을 확인하였음 (503 에러 반환, VPN 등으로 IP 전환 시 정상 값 반환)
    - c. 고도화 단계에서 이용 가능한 IP 대역대를 다수 확보하여, 503 에러 반환시 새로운 IP를 통해 요청하는 방식으로 대응 코드 작성 필요
2. **대용량 데이터 처리를 위한 Spark 클러스터 도입**
    - 현재 데이터 처리 방식은 pandas로 이루어졌으며, 대규모 데이터를 처리하는 데 한계가 존재할 수 있습니다.
    - Apache Spark 클러스터를 도입하여 데이터 처리 작업을 분산하여 병렬 처리 성능을 극대화하고,  대규모 데이터 분석 작업을 최적화할 수 있습니다.
3. **데이터 레이크 도입으로 비용 효율화**
    - Amazon S3를 데이터 레이크로 활용하여 수집된 데이터를 효율적이고 비용 효과적으로 저장 및 관리할 수 있습니다.
    - S3의 확장성을 활용하면 데이터 증가에 따른 저장소 문제를 효과적으로 해결할 수 있습니다.
4. **SQL 기반 데이터 조회를 위한 Athena 활용**
    - S3에 저장된 데이터를 Amazon Athena를 통해 SQL 기반 쿼리로 분석하여 데이터 조회 및 탐색을 간소화할 수 있습니다.
    - 이를 통해 기존 대시보드와 연동하거나 빠른 데이터 분석 작업을 지원할 수 있습니다.
5. **데이터 처리 및 최적화 방향**
    - Spark 클러스터와 S3 데이터 레이크를 연계하여 데이터 수집, 처리, 저장 및 조회 과정을 더욱 최적화할 수 있습니다.
    - Spark의 고급 연산 최적화 및 데이터 파티셔닝 기능을 활용하여 처리 성능을 높이고, 분석 시간 단축 및 리소스 사용을 최소화하는 방향으로 개선할 수 있습니다.
6. **데이터 정확도와 다양성 개선**
    - OpenSky API 외에도 항공사 API와 같은 외부 데이터 소스를 통합하여 데이터 정확성과 신뢰성을 강화합니다.
    - 추가적인 데이터 정제를 통해 보다 깨끗한 데이터를 제공할 수 있습니다.
7. **사용자 경험 및 기능 강화**
    - 대시보드 인터페이스를 더욱 직관적이고 사용자 친화적으로 개선하여 데이터 활용의 접근성을 높입니다.
    - 모바일 및 태블릿 환경에서도 사용 가능한 반응형 UI를 설계합니다.
    - 항공 지연 예측, 혼잡 공항 경고, 효율적 경로 추천 등의 고급 분석 기능을 추가하여 서비스의 가치를 확장합니다.


## 7. 프로젝트 실행 방법

### 적재 준비

OpenSky 웹 사이트 회원 가입 - https://opensky-network.org/

Snowflake 계정 준비

### 레포지터리 복제
```
git clone https://github.com/bibibig-org/airbig.git
```

### airflow_variable.env 파일 수정
```
AIRFLOW_VAR_OPENSKY_ID="YourOpenSkyID"
AIRFLOW_VAR_OPENSKY_PW="YourOpenSkyPW"
AIRFLOW_VAR_SNOWFLAKE_ACCOUNT="YourSnowflakeAccount"
AIRFLOW_VAR_SNOWFLAKE_ID="YourSnowflakeID"
AIRFLOW_VAR_SNOWFLAKE_PW="YourSnowflakePW"
AIRFLOW_VAR_SNOWFLAKE_DATABASE="BIBIBIG"
AIRFLOW_VAR_SNOWFLAKE_SCHEMA="BRONZE_STATE_DATA"
```

### snowflake_table_schema 폴더 아래 SQL 실행 (Snowflake Table 생성, DDL)
- 실행 순서
   1. INIT_DATABASE_SCHEMA.sql 구문 실행
   2. 나머지 .sql 파일 실행
   3. AIRPLANE_LOCATION_DATA.csv, AIRPORT_INFO_DATA.csv는 snowflake에 동일한 명칭을 가진 테이블에 데이터 적재


### 프로젝트 실행
```
docker compose up
```

### 활성화 할 Dag 선택
- upload_location_data_sql_insert : 실시간 항공기 위치를 업로드 (Snowflake에 직접 Insert) 
- upload_location_data : 실시간 항공기 위치를 업로드 (csv 업로드)
- opensky_flights_etl_dept_arrival_to_snowflake : 항공기의 최근 1시간 내 출/도착 정보 업로드 (Snowflake에 직접 Insert)
- upload_ticket_price_naver : 최다 이용 경로의 네이버 항공기 가격 조회 (소량 조회 가능)

### Tableau나 외부 BI로 Snowflake를 연결


