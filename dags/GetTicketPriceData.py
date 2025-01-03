import os
import time
import json
import requests
import urllib.parse
import pandas as pd
import concurrent.futures


def GetDomesticAirticketPrice(DepartureAirport : str, ArrivalAirport : str, DepartureDate : str):
    '''
    Parameters
    ----------
    DepartureAirport : String
        IATA CODE : 3 char departureAirport Name (required)
    ArrivalAirport : String
        IATA CODE : 3 char ArrivalAirport Name (required)
    DepartureDate : String
        date : %Y%m%d (required)
        can only view future points in time rather than today

    Returns
    -------
    total_data : json(dict)
        Current moment price data (naver search data)
        all_fareType_seat return

    '''
    
    # init values
    mxm_requests_try = 5
    current_page = 0
    mxm_page = 10   # 50 * mxm_page = mxm_return value
    true, false, null = True, False, None
    total_data = {
        'flights' : [],
        'status' : {}
        }
    template = {
        'url' : 'https://flight-api.naver.com/flight/domestic/searchFlights',
        'headers' : {
            "Content-Type": "application/json",
            "accept-language" : 'ko-KR,ko',
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            },
        'payload' :  {
          "type": "domestic",
          "device": "PC",
          "fareType": "YC",
          "itineraries": [
            {
              "departureAirport": DepartureAirport,
              "arrivalAirport": ArrivalAirport,
              "departureDate": DepartureDate
            }
          ],
          "person": {
            "adult": 1,
            "child": 0,
            "infant": 0
          },
          "tripType": "OW",
          "version": 0,
          "flightFilter": {
            "filter": {
              "type": "departure"
            },
            "limit": 100,
            "skip": 0,
            "sort": {
              "segment.departure.time": 1,
              "minFare": 1
            }
          }
        }
    }

    while current_page < mxm_page:
        # sometime occur response data error 
        for current_try_num in range(mxm_requests_try):
            try:
                response = requests.post(template['url'], json=template['payload'], headers=template['headers'])
                data = eval(response.text.replace('data: ', ''))
            except Exception as e:
                if current_try_num == mxm_requests_try - 1:
                    print(f"page requests {mxm_requests_try} times failed", e)
                    raise "Can't Get AirTicket Data"
        
        if len(data['flights']) < 1:
            break
        
        # concat data
        total_data['flights'].extend(data['flights'])
        total_data['status'] = {**total_data['status'], **data['status']} # dict
        
        # page count, next_page setting
        current_page += 1
        template['payload']['flightFilter']['skip'] += 50
        
        # waiting data load
        time.sleep(5)

    # parse data
    if 'departure' in total_data['status'].keys():
        total_data['status']['departure']['depAirportName'] = bytes(total_data['status']['departure']['depAirportName'], 'latin1').decode('utf-8')
        total_data['status']['departure']['arrAirportName'] = bytes(total_data['status']['departure']['arrAirportName'], 'latin1').decode('utf-8')
    
    if 'airlinesCodeMap' in total_data['status'].keys():
        total_data['status']['airlinesCodeMap'] = {k : bytes(v, 'latin1').decode('utf-8') for k, v in total_data['status']['airlinesCodeMap'].items()}
        
    if 'opCodeMap' in total_data['status'].keys():
        total_data['status']['opCodeMap'] = {k : bytes(v, 'latin1').decode('utf-8') for k, v in total_data['status']['opCodeMap'].items()}


    return total_data



def GetInternationalAirticketPrice(DepartureAirport : str, ArrivalAirport : str, DepartureDate : str, isDirect = True, fareType = "Y"):
    '''
    Parameters
    ----------
    DepartureAirport : String
        IATA CODE : 3 char departureAirport Name (required)
    ArrivalAirport : String
        IATA CODE : 3 char ArrivalAirport Name (required)
    DepartureDate : String
        date : %Y%m%d (required)
        can only view future points in time rather than today.
    isDirect : Boolean
        Decide whether to fly direct or via transit (optional)
    fareType : String
        type of seat (optional)
            Y: economy class (Default)
            P: Premium Economy Class
            C: Business class
            F: First class

    Returns
    -------
    total_data : json(dict)
        Current moment price data (naver search data)
        selected_fareType_seat return
    '''
    # init values
    if fareType not in ('Y', 'P', 'C', 'F'):
        raise "fareType must set (Y: economy class (Default), P: Premium Economy Class, C: Business class, F: First class)"
        
    faretype_mapping_dict = {'Y' : 'Economy', 'P' : 'Premium Economy', 'C' : 'Business', 'F' : 'First'}
    current_page = 0
    mxm_page = 25       # 20 * mxm_page = mxm_return value
    total_data = {
        'seatClass' : faretype_mapping_dict[fareType],
        'airlines' : {},
        'airports' : {},
        'carbonEmissionAverage' : {},
        'fares' : {},
        'fareTypes' : {},
        'schedules' : [],
        }
    
    template = {
        'url' : "https://airline-api.naver.com/graphql",
        'headers' : {
            "Content-Type": "application/json",
            "Referer": f"https://flight.naver.com/flights/international/{DepartureAirport}-{ArrivalAirport}-{DepartureDate}?adult=1&fareType=Y",
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        },
        'payload' : {
             "operationName": "getInternationalList",
             "variables": {
               "adult": 1,
               "child": 0,
               "infant": 0,
               "where": "pc",
               "isDirect": isDirect,
               "galileoFlag": True,
               "travelBizFlag": False,
               "fareType": fareType,
               "itinerary": [
                 {
                   "departureAirport": DepartureAirport, # airport Name (ENG, IATA CODE)
                   "arrivalAirport": ArrivalAirport, # airport Name (ENG, IATA CODE)
                   "departureDate": DepartureDate # %Y%m%d
                 }
               ],
               "stayLength": "",
               "trip": "OW",
               "galileoKey": "", # pagination value
               "travelBizKey": "", # pagination value
             },
             "query" : """
                 query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {
                   internationalList(
                     input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}
                   ) {
                     galileoKey
                     galileoFlag
                     travelBizKey
                     travelBizFlag
                     totalResCnt
                     resCnt
                     results {
                       airlines
                       airports
                       fareTypes
                       schedules
                       fares
                       errors
                       carbonEmissionAverage {
                         directFlightCarbonEmissionItineraryAverage
                         directFlightCarbonEmissionAverage
                       }
                     }
                   }
                 } """
           }
        }

    response = requests.post(template['url'], json=template['payload'], headers=template['headers'])
    data = response.json()
    
    # pagination setting
    try:
        template['payload']['variables']['galileoKey'] = str(data['data']['internationalList']['galileoKey'])
        template['payload']['variables']['travelBizKey'] = str(data['data']['internationalList']['travelBizKey'])
        template['payload']['variables']['galileoFlag'] = template['payload']['variables']['galileoKey'] != ""
        template['payload']['variables']['travelBizFlag'] =  template['payload']['variables']['travelBizKey'] != ""   
    except Exception as e:
        print('cannot found galileoKey, galileoKey', e)
        
    # waiting data load
    time.sleep(5) 
    
    # get air ticket data
    while current_page < mxm_page:
        # get data 
        response = requests.post(template['url'], json=template['payload'], headers=template['headers'])
        data = response.json()
        
        # break condition
        try:
            if data['data']['internationalList']['results']['airlines'] == {}: # empty
                break
        except: # Nonetype이 들어온 경우 (*요청할 수 없는 IATA 코드로 요청한 경우)
            break
        
        # concat data        
        total_data['airlines'] = {**total_data['airlines'], **data['data']['internationalList']['results']['airlines']} # dict
        total_data['airports'] = {**total_data['airports'], **data['data']['internationalList']['results']['airports']} # dict
        total_data['carbonEmissionAverage'] = {**total_data['carbonEmissionAverage'], **data['data']['internationalList']['results']['carbonEmissionAverage']} # dict
        total_data['fares'] = {**total_data['fares'], **data['data']['internationalList']['results']['fares']} # dict
        total_data['fareTypes'] = {**total_data['fareTypes'], **data['data']['internationalList']['results']['fareTypes']} # dict
        total_data['schedules'].extend(data['data']['internationalList']['results']['schedules']) # list

        # waiting data load
        time.sleep(3) 
        
    # url encoding data transform
    total_data['airlines'] = {k : urllib.parse.unquote(v) for k, v in total_data['airlines'].items()}
    total_data['airports'] = {k : urllib.parse.unquote(v) for k, v in total_data['airports'].items()}
    total_data['fareTypes'] = {k : urllib.parse.unquote(v) for k, v in total_data['fareTypes'].items()}
    
    # count page
    current_page += 1

    return total_data


def FetchDomesticAirticketPrice(json_data):
    if 'airlinesCodeMap' not in json_data['status'].keys():
        return None, None, None, None
    
    # meta data
    df_company_name = (pd.Series(json_data['status']['airlinesCodeMap'])
                       .reset_index()
                       .rename({'index' : 'company_code', 0 : 'company_kor'}, axis = 1))
    
    
    # schedule data & fare data
    fare_idx = 0
    df_schedule = pd.DataFrame()
    df_fare = pd.DataFrame()
    for schedule_idx, schedule in enumerate(json_data['flights']):
        # schedule data
        df_schedule.loc[schedule_idx, 'schedule'] = schedule['itineraryId']
        df_schedule.loc[schedule_idx, 'seatClass'] = schedule['seatClass']
        df_schedule.loc[schedule_idx, 'minFare'] = schedule['minFare']
        df_schedule.loc[schedule_idx, 'duration'] = schedule['duration']
        df_schedule.loc[schedule_idx, 'airlineCode'] = schedule['segment']['airlineCode']
        df_schedule.loc[schedule_idx, 'flightNumber'] = schedule['segment']['flightNumber']
        df_schedule.loc[schedule_idx, 'departure_airportCode'] = schedule['segment']['departure']['airportCode']
        df_schedule.loc[schedule_idx, 'arrival_airportCode'] = schedule['segment']['arrival']['airportCode']
        df_schedule.loc[schedule_idx, 'departure_date'] = schedule['segment']['departure']['date']
        df_schedule.loc[schedule_idx, 'departure_time'] = schedule['segment']['departure']['time']
        
        
        # fare data
        for fare in schedule['fares']: 
            # fare meta data
            df_fare.loc[fare_idx, 'schedule'] = schedule['itineraryId']
            df_fare.loc[fare_idx, 'seatClass'] = schedule['seatClass']
            
            # fare data (price by company)
            for k, v in fare.items():
                if k not in ('etc', 'childFare', 'cFuel', 'cTax', 'supportNPay', 'discountFare'):
                    df_fare.loc[fare_idx, k] = v
            
            # add index
            fare_idx += 1

    return df_company_name, None, df_schedule, df_fare



def FetchInternationalAirticketPrice(json_data):
    # meta data
    df_company_name = (pd.Series(json_data['airlines'])
                       .reset_index()
                       .rename({'index' : 'company_code', 0 : 'company_kor'}, axis = 1))
    df_faretype = (pd.Series(json_data['fareTypes'])
                   .reset_index()
                   .rename({'index' : 'FareType', 0 : 'FareType_kor'}, axis = 1))
    
    # schedule data
    df_schedule = pd.DataFrame()
    for schedule_dict in json_data['schedules']: # json_data['schedules'] is list
        for schedule in schedule_dict.values():
            df_schedule.loc[schedule['id'], 'journeyTime_minute'] = int(schedule['journeyTime'][0]) * 60 + int(schedule['journeyTime'][1]) * 1 
            for k, v in schedule['detail'][0].items():
                df_schedule.loc[schedule['id'], k] = v
    df_schedule.insert(0, 'seat', json_data['seatClass'])
    df_schedule = (df_schedule
                   .reset_index()
                   .rename({'index' : 'schedule'}, axis = 1))

    # fare data
    idx_counter = 0
    df_fare = pd.DataFrame()
    for schedule, fare_dict in json_data['fares'].items(): # schedule -> 항공 편 명칭 / fare_dict -> 결제 구분 별 가격 정보
        current_schedule = fare_dict['sch'][0]
        
        for fare_type, fare_info in fare_dict['fare'].items(): # fare_info -> 결제 구분 별 가격 정보 상세
            # primary key data
            df_fare.loc[idx_counter, 'schedule'] = current_schedule
            
            # meta data (string only)
            for k, v in fare_info[0].items():
                if type(v) == str:
                    df_fare.loc[idx_counter, k] = v
        
            # adult price data (string only)
            for k, v in fare_info[0]['Adult'].items():
                if type(v) == str:
                    df_fare.loc[idx_counter, k] = v
            
            idx_counter += 1
            
    if len(df_fare) > 0:
        df_fare.insert(1, 'seatClass', json_data['seatClass'])
        df_fare = df_fare.drop_duplicates()
    
    return df_company_name, df_faretype, df_schedule, df_fare


def GetAirticketPrice(DepartureAirport : str, ArrivalAirport : str, DepartureDate : str, isDirect = True, fareType = "Y"):
    '''
    Parameters
    ----------
    DepartureAirport : String
        IATA CODE : 3 char departureAirport Name (required)
    ArrivalAirport : String
        IATA CODE : 3 char ArrivalAirport Name (required)
    DepartureDate : String
        date : %Y%m%d (required)
        can only view future points in time rather than today.
    isDirect : Boolean
        KOREA_DOMESTIC_DATA IS NOT USED VALUE
        Decide whether to fly direct or via transit (optional)
    fareType : String
        KOREA_DOMESTIC_DATA IS NOT USED VALUE
        type of seat (optional)
            Y: economy class (Default)
            P: Premium Economy Class
            C: Business class
            F: First class

    Returns
    -------
    bulk_result : list[DataFrame]
        [company_name : DataFrame, # meta dataframe
         faretype : DataFrame,     # meta dataframe
         schedule : DataFrame,     # schedule dataframe
         fare : DataFrame]         # fare dataframe
        
    '''
    KOR_DOMESTIC_AIRPORT = ('ICN', 'GMP', 'YNY', 'MWX', 'CJU', 'TAE', 'CJJ', 'PUS', 'USN', 'RSU', 'KWJ', 'KUV', 'WJU', 'KPO', 'HIN')
    
    # DomesticAirline
    if (DepartureAirport in KOR_DOMESTIC_AIRPORT) and (ArrivalAirport in KOR_DOMESTIC_AIRPORT):
        data = GetDomesticAirticketPrice(DepartureAirport, ArrivalAirport, DepartureDate)
        data = FetchDomesticAirticketPrice(data)
        # print('domestic')
    # InternationalAirline
    else: 
        data = GetInternationalAirticketPrice(DepartureAirport, ArrivalAirport, DepartureDate, isDirect = isDirect, fareType = fareType)
        data = FetchInternationalAirticketPrice(data)  
    return data



def GetBulkAirticketPrice(SearchList : list[tuple], DepartureDate : str, isDirect=True, fareType = "Y"):
    '''

    Parameters
    ----------
    SearchList : list[tuple]
        airline info list : [(<departure>, <arrival>), ...]
        ex. [("GMP","CJU"), ('ICN','JFK'), ('ICN','HAN'), ...]
    DepartureDate : str
        (%Y%m%d) date data

    Returns
    -------
    bulk_result : dict[DataFrame]
        {company_name : DataFrame, # meta dataframe
         faretype : DataFrame,     # meta dataframe
         schedule : DataFrame,     # schedule dataframe
         fare : DataFrame}         # fare dataframe

    '''
    # init
    bulk_result = {
        'company_name' : [],
        'fareType' : [],
        'schedule' : [],
        'fare' : []
        }
    
    with concurrent.futures.ThreadPoolExecutor(max_workers = os.cpu_count()*2) as executor:
        futures = []
        # submit data
        for DepartureAirport, ArrivalAirport in SearchList:
            futures.append(executor.submit(GetAirticketPrice, DepartureAirport, ArrivalAirport, DepartureDate, isDirect, fareType))

        for future in concurrent.futures.as_completed(futures):
            # fetch data
            data = future.result()
            
            # append data
            bulk_result['company_name'].append(data[0])
            bulk_result['fareType'].append(data[1])
            bulk_result['schedule'].append(data[2])
            bulk_result['fare'].append(data[3])
        
        bulk_result['company_name'] = pd.concat(bulk_result['company_name'], axis = 0).drop_duplicates()
        bulk_result['fareType'] = pd.concat(bulk_result['fareType'], axis = 0).drop_duplicates()
        bulk_result['schedule'] = pd.concat(bulk_result['schedule'], axis = 0).drop_duplicates()
        bulk_result['fare'] = pd.concat(bulk_result['fare'], axis = 0).drop_duplicates()
        
    return bulk_result



# Test Code
if __name__ == "__main__":
    # 국내선 데이터 (싱글 스레드)
    domestic_ticket_price = GetDomesticAirticketPrice("GMP", "CJU", "20250217")
    chunk_domestic_price = FetchDomesticAirticketPrice(domestic_ticket_price)
    
    # 국제선 데이터 (싱글 스레드)
    international_ticket_price = GetInternationalAirticketPrice("ICN", "JFK", "20250211", isDirect=True, fareType = "P")
    chunk_international_price = FetchInternationalAirticketPrice(international_ticket_price)
    
    # 통합 데이터 요청
    chunk_ticket_price = GetAirticketPrice("ICN", "JFK", "20250211")
    chunk_ticket_price = GetAirticketPrice("GMP", "CJU", "20250211")

    # 멀티 스레드 데이터 요청
    price_dict = GetBulkAirticketPrice(SearchList = [("GMP","CJU"), ('ICN','JFK'), ('ICN','HAN'), ('ICN','HNL'), ('ICN','PEK'), ('ICN','NRT')], DepartureDate = "20250211", isDirect=True, fareType = "Y")


