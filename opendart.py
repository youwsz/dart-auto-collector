# IMPORT
import pandas as pd
import requests
import json
from io import BytesIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
from pykrx import stock

#########################################################################################
#       OpenDartManager
#
#########################################################################################
class OpenDartManager:
    __crtfcKey = "82784d59168f5d7c14e99c36e596e80888d6eccb"  # crtfc_key
    __operating_profit_filter = False # 영업이익 적자 필터
    __net_income_filter = False # 당기순이익 적자 필터
    __base_time = datetime.today()

    def __init__(self):
        print("start to get ticker list : ", datetime.now())
        cur_folder_path = os.path.dirname(os.path.realpath(__file__))
        #cache_file_path = cur_folder_path + "/ticker_cache.txt"

        ## 전날짜기준, 주말은 금요일 기준
        weekday = datetime.today().weekday()
        today = datetime.today()
        if weekday == 5:  # 토요일
            today = today - timedelta(days=1)
        elif weekday == 6:  # 일요일
            today = today - timedelta(days=2)
        elif weekday == 0:  # 월요일
            today = today - timedelta(days=3)
        else:
            today = today - timedelta(days=1)

        self.__base_time = today.strftime("%Y%m%d")

        # if os.path.exists(cache_file_path):
        #     cache_file = open(cache_file_path, "rt")
        #     lines = cache_file.readlines()
        #     cache_file.close()
        #
        #     for line in lines:
        #         line = line.replace('\n','')
        #         name_and_code = line.split(':');
        #         self.__dict_tickers[name_and_code[0]] = name_and_code[1]
        # else:
        #     cache_file = open(cache_file_path, "wt")
        #
        #     tickers = stock.get_market_ticker_list(self.__base_time, market="KOSDAQ")
        #     for ticker in tickers:
        #         name = stock.get_market_ticker_name(ticker)
        #         self.__dict_tickers[name] = ticker
        #         cache_file.write(name + ":" + ticker + "\n")
        #
        #     tickers = stock.get_market_ticker_list(self.__base_time, market="KOSPI")
        #     for ticker in tickers:
        #         name = stock.get_market_ticker_name(ticker)
        #         self.__dict_tickers[name] = ticker
        #         cache_file.write(name + ":" + ticker + "\n")
        #     cache_file.close()

        #print("end to get ticker list : ", datetime.now())

    # 실적 필터링 설정
    def set_filter(self, operating_profit_filter, net_income_filter):
        self.__operating_profit_filter = operating_profit_filter
        self.__net_income_filter = net_income_filter

    # 실적 필터링
    # ret : TRUE (필터링 대상)/FALSE(필터링 대상 아님)
    def __check_filter(self, account_name, money):
        if money > 0:
            return False

        if account_name  in '영업수익' or account_name in '영업이익(손실)':
            if self.__operating_profit_filter is True:
                return True
        elif account_name in '당기순이익(손실)':
            if self.__net_income_filter is True:
                return True
        return False

    # 공시대상회사의 고유번호 xml 파일로 얻기 (파일명 : CORPCODE.xml)
    def __update_corp_code_File(self):
        print("__updateCorpCodeFile")
        params = {'crtfc_key': self.__crtfcKey}
        url = "	https://opendart.fss.or.kr/api/corpCode.xml"
        response = requests.get(url, params=params)

        with ZipFile(BytesIO(response.content)) as zipfile:
            file_list = zipfile.namelist()
            zipfile.extractall()

    # 상장 회사인지 조회 후 반환 (유가증권, 코스닥만 상장회사로 판단)
    # ret : TRUE/FALSE
    def __is_stock_listing_corp(self, corp_code):
        params = {'crtfc_key': self.__crtfcKey, 'corp_code': corp_code}
        url = "https://opendart.fss.or.kr/api/company.json"
        res = requests.get(url, params)
        json_dict = json.loads(res.text)
        if json_dict['status'] == "000":
            if json_dict['corp_cls'] == "Y" or json_dict['corp_cls'] == "K":
                return True
        return False

    # 모든 회사의 고유번호 딕셔너리로 가져오기
    # ret : dictionary { corpName : corpCode }
    def __get_corp_code_list(self):
        cur_folder_path = os.path.dirname(os.path.realpath(__file__))
        cache_file_path = cur_folder_path + "/corp_list_cache.txt"

        if os.path.exists(cache_file_path):
            cache_file = open(cache_file_path, "rt")
            lines = cache_file.readlines()
            cache_file.close()
            ##TODO cache 파일에 읽기 -> #todo::추후에 DB로 변경
            print("corp_code_list cache start : ", datetime.now())
            corp_code_list = {}
            for line in lines:
                line = line.replace('\n','')
                name_and_code = line.split(',');
                corp_code_list[name_and_code[0]] = {name_and_code[1],name_and_code[2]}
            print("corp_code_list cache end : ", datetime.now())
            return corp_code_list

        else:
            cache_file = open(cache_file_path, "wt")

            # CORPCODE.xml 업데이트
            self.__update_corp_code_File()

            # CORPCODE.xml 파싱
            doc = ET.parse("CORPCODE.xml")

            # root 태그 가져오기
            root = doc.getroot()

            print("corp_code_list start : ", datetime.now())
            corp_code_list = {}

            for list_tag in root.iter("list"):
                if list_tag.find("stock_code").text != ' ':  # 상장기업만 가져온다.
                    corp_code = list_tag.find("corp_code").text
                    if self.__is_stock_listing_corp(corp_code):
                        corp_name = list_tag.find("corp_name").text
                        stock_code = list_tag.find("stock_code").text
                        corp_code_list[corp_name] = corp_code
                        ##TODO 파일에 쓰기 -> 추후에 DB로 변경
                        cache_file.write(corp_name + "," + corp_code + "," + stock_code + "\n")

            cache_file.close()
            print("corp_code_list end : ", datetime.now())
            return corp_code_list

    # 분기별 보고서 코드 가져오기
    # ret : string
    def __get_report_code(self, quarter):
        print("__getReportCode")
        if quarter == "1":
            return "11013"  # 1분기 보고서
        elif quarter == "2":
            return "11012"  # 2분기 보고서
        elif quarter == "3":
            return "11014"  # 3분기 보고서
        elif quarter == "4":
            return "11011"  # 4분기 보고서
        else:
            return

    # 기업 시총, 상장주식수 가져오기
    # ret : list [시가총액, 상장주식수]
    def __get_stock_info(self, stock_code):
        try:
            ## 기업명으로 오늘 날짜 기준 시가총액, 상장주식수  가져오기
            # 1. 해당 기업명의 네이버 주식코드(티커) 알아오기
            if company_name not in self.__dict_tickers:
                return []

            # 2. df = stock.get_market_cap_by_date("20190101", "20190131", "005930") 호출
            #   df = 날짜, 시가총액, 거래량, 거래대금, 상장주식수
            stock_df = stock.get_market_cap_by_date(self.__base_time, self.__base_time, stock_code)

            # 3. df에서 시가총액 컬럼 series
            series_cap = stock_df.get("시가총액")
            if not series_cap.array:
                print("series_cap erroor")

            market_capitalization = round(int(series_cap.array[0]) / 100, 2)  # 억단위로 변경

            # 4. df에서 발행주식 컬럼 series
            series_shares = stock_df.get("상장주식수")
            if not series_shares.array:
                print("series_shares erroor")
            listed_shares = series_shares.array[0]

            data = []
            data.append(market_capitalization)
            data.append(listed_shares)
            return data

        except:
            print("[__get_stock_info] ", company_name," error")
            return []

    # 연결 재무제표 요청
    # ret : list [회사명, 매출액, 영업이익, 당기순이익]
    def __get_performance(self, corp_code, bsns_year, reprt_code):
        try:
            url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?"
            params = {'crtfc_key': self.__crtfcKey,
                      'corp_code': corp_code,
                      'bsns_year': bsns_year,
                      'reprt_code': reprt_code,
                      'fs_div': "OFS"}  # OFS : 재무제표, CFS : 연결재무제표
            response = requests.get(url, params)
            json_dict = json.loads(response.text)
            data = []
            num_of_data_to_find = 3  # 매출액, 영업이익, 당기순이익만 추출한다.

            if json_dict['status'] == "000":
                # 재무제표가 없을 수 있음
                if 'list' in json_dict:
                    # data list convert
                    for line in json_dict['list']:
                        if line['sj_div'] == 'CIS':  # 포괄손익계산서만 필요함
                            if line['account_nm'] in '수익(매출액)' or line['account_nm'] in '영업수익' or \
                                    line['account_nm'] in '영업이익(손실)' or line['account_nm'] in '당기순이익(손실)':

                                amount_data = 0
                                if line['thstrm_amount'] != '':
                                    amount_of_money = int(line['thstrm_amount'])
                                    amount_of_money = round(amount_of_money / 100000000, 2)  # 억단위로 변환

                                    if self.__check_filter(line['account_nm'], amount_of_money) is True:
                                        return []
                                    amount_data = amount_of_money

                                data.append(amount_data)

                                if data.__len__() == num_of_data_to_find:  # 이미 다 찾았으면 그만 찾아
                                    return data

            if data.__len__() < num_of_data_to_find:
                return []

            return data

        except:
            print("[__get_performance] ", corp_code, " except")
            return []

    # 연도 - 분기에 해당하는 실적 보고서 데이터 가져오기
    # ret : DataFrame
    def get_performance_table(self, bsns_year, quarter):

        #validation check
        if self.__dict_tickers.__len__() == 0:
            print("stock api is disconnected")
            return pd.DataFrame()


        print("getPerformanceTable")
        reprt_code = self.__get_report_code(quarter)
        if reprt_code == 0:
            print("__getReportCode fail")
            return pd.DataFrame()

        corp_code_list = self.__get_corp_code_list()

        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?"
        performance_table = []

        for company_name, corp_code, stock_code in corp_code_list.items():

            company_data = []
            company_data.append(company_name)

            # 각 회사별 실적 데이터 가져오기
            performance_data = []   #'매출액(억원)', '영업이익(억원)', '당기순이익(억원)'
            performance_data = self.__get_performance(corp_code, bsns_year, reprt_code)
            if not performance_data:    #data empty
                continue

            ## 시가총액, 상장주식수
            stock_data = []     #'시가총액', '상장주식수'
            stock_data = self.__get_stock_info(stock_code)
            if not stock_data:    #data empty  #Todo:유가증권 이름과 네이버 증권 이름이 다른 경우가 있다.(ex 네이버증권 : 삼영전자 -> DART : 삼영전자공업)
                continue

            print(company_data, performance_data, "시총 및 주식수 : ", stock_data)


            # 개인적인 계산
            # 멀티플 = 시가총액 / 당기 순이익
            a = float(stock_data[0])
            b = float(performance_data[2])
            # 사업보고서가 아닌 경우, 분기실적 * 4를 함
            if quarter != "4":
                b = b * 4
            multiple = round(a / b, 1)
            #multiple = int(stock_data[0]) / (int(float(performance_data[2])) * 4)
            multiple_data = []
            multiple_data.append(multiple)

            performance_table.append(company_data + performance_data + stock_data + multiple_data)

            # testcode
            #if performance_table.__len__() > 10:
            #    break

        column_names = ['기업명', '매출액(억원)', '영업이익(억원)', '당기순이익(억원)', '시가총액', '상장주식수', '멀티플']
        df = pd.DataFrame(performance_table, columns=column_names)
        return df

    # date:YYYYMMDD
    def get_report(self, date):
        print('get_report_list')
        rcept_no_list = {}

        if date == '':
            now = datetime.datetime.now()  # 2015-04-19 12:11:32.669083
            date = now.strftime('%Y%m%d')

        corp_code_list = self.__get_corp_code_list()

        for company_name, corp_code in corp_code_list.items():

            params = {
                        'crtfc_key': "82784d59168f5d7c14e99c36e596e80888d6eccb",
                        'last_reprt_at': 'Y',
                        'corp_code': corp_code,
                        'bgn_de': date,
                        'pblntf_ty': 'I',
                        'pblntf_detail_ty': 'I002',
                        'sort': 'rpt',
                        'corp_cls': 'Y'}
            url = "https://opendart.fss.or.kr/api/list.json"

            resonse = requests.get(url, params=params)
            json_dict = json.loads(resonse.text)
            if json_dict['status'] == "000":
                if 'list' in json_dict:
                    # data list convert
                    find_rcept = False
                    for data in json_dict['list']:
                        if data['report_nm'] in '연결재무제표기준영업(잠정)실적':
                            rcept_no_list[corp_code] = data['rcept_no']
                            break


        print(rcept_no_list)

        for rcept_no in rcept_no_list:
            params = {
                'crtfc_key': "82784d59168f5d7c14e99c36e596e80888d6eccb",
                'rcept_no': rcept_no
            }
            url = "https://opendart.fss.or.kr/api/document.xml"

            resonse = requests.get(url, params=params)
            json_dict = json.loads(resonse.text)
            if json_dict['status'] == "000":
                with ZipFile(BytesIO(response.content)) as zipfile:
                    #file_list = zipfile.namelist()
                    zipfile.extractall()




#########################################################################################


#########################################################################################
#               test api
#########################################################################################
# 공시 리포트 리스트 검색
# https: // opendart.fss. or.kr / api / list.json
# date:YYYYMMDD
# pblntf_detail_ty : I002
##todo:: 테스트 진행중인 api..
