# IMPORT
import pandas as pd
import requests
import json
from io import BytesIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import datetime
import os

#########################################################################################
#       OpenDartManager
#
#########################################################################################
class OpenDartManager:
    __crtfcKey = "82784d59168f5d7c14e99c36e596e80888d6eccb"  # crtfc_key

    # def __init__(self):

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
        print("__getCorpCodeList")
        cur_folder_path = os.path.dirname(os.path.realpath(__file__))
        cache_file_path = cur_folder_path + "/corp_list_cache.txt"

        if os.path.exists(cache_file_path):
            cache_file = open(cache_file_path, "rt")
            lines = cache_file.readlines()
            cache_file.close()

            print("corp_code_list cache start : ", datetime.datetime.now())
            corp_code_list = {}
            for line in lines:
                name_and_code = line.split(':');
                corp_code_list[name_and_code[0]] = name_and_code[1]
            print("corp_code_list cache end : ", datetime.datetime.now())
            return corp_code_list

        else:
            cache_file = open(cache_file_path, "wt")

            # CORPCODE.xml 업데이트
            self.__update_corp_code_File()

            # CORPCODE.xml 파싱
            doc = ET.parse("CORPCODE.xml")

            # root 태그 가져오기
            root = doc.getroot()

            print("corp_code_list start : ", datetime.datetime.now())
            corp_code_list = {}

            for list_tag in root.iter("list"):
                if list_tag.find("stock_code").text != ' ':  # 상장기업만 가져온다.
                    corp_code = list_tag.find("corp_code").text
                    if self.__is_stock_listing_corp(corp_code):
                        corp_name = list_tag.find("corp_name").text
                        corp_code_list[corp_name] = corp_code
                        cache_file.write(corp_name + ":" + corp_code + "\n")

            cache_file.close()
            print("corp_code_list end : ", datetime.datetime.now())
            return corp_code_list

    # 분기별 보고서 코드 가져오기
    # ret : string
    def __get_report_code(self, quarter):
        print("__getReportCode")
        if quarter == 1:
            return "11013"  # 1분기 보고서
        elif quarter == 2:
            return "11012"  # 2분기 보고서
        elif quarter == 3:
            return "11014"  # 3분기 보고서
        elif quarter == 4:
            return "11011"  # 4분기 보고서
        else:
            return "0"

    # 연도 - 분기에 해당하는 실적 보고서 데이터 가져오기
    # ret : DataFrame
    def get_performance_table(self, bsns_year, quarter):
        print("getPerformanceTable")
        reprt_code = self.__get_report_code(quarter)
        if reprt_code == 0:
            print("__getReportCode fail")
            return

        corp_code_list = self.__get_corp_code_list()

        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?"
        performance_table = []
        column_names = ['기업명', '매출액(억원)', '영업이익(억원)', '당기순이익(억원)']
        target_account_names = ['수익(매출액)', '영업수익', '영업이익(손실)', '당기순이익(손실)']
        for company_name, corp_code in corp_code_list.items():
            # 각 회사별 연결 재무제표 요청
            params = {'crtfc_key': self.__crtfcKey,
                      'corp_code': corp_code,
                      'bsns_year': str(bsns_year),
                      'reprt_code': reprt_code,
                      'fs_div': "CFS"}  # OFS : 재무제표, CFS : 연결재무제표
            response = requests.get(url, params)
            json_dict = json.loads(response.text)
            data = []
            if json_dict['status'] == "000":
                data.append(company_name)
                if 'list' in json_dict:
                    # data list convert
                    for line in json_dict['list']:
                        if line['sj_div'] == 'CIS':  # 포괄손익계산서만 필요함
                            if line['account_nm'] in target_account_names:
                                int_data = int(line['thstrm_amount'])
                                if int_data == 0:
                                    print(company_name + " : not found thstrm_amount, " + line['account_nm'])
                                    break
                                int_data = int_data / 100000000  # 억단위로 변환
                                data.append(int_data)
                                if data.__len__() == column_names.__len__():  # 이미 다 찾았으면 그만 찾아
                                    break
            else:
                continue

            if data.__len__() < column_names.__len__():  # 풀 데이터가 아니면 꼬임. 버린다
                continue
            print(company_name," : ", data)
            performance_table.append(data)

        df = pd.DataFrame(performance_table, columns=column_names)


#########################################################################################


#########################################################################################
#               test api
#########################################################################################
# 공시 리포트 리스트 검색
# https: // opendart.fss. or.kr / api / list.json
# date:YYYYMMDD
# pblntf_detail_ty : H002 사업/반기/분기보고서
##todo:: 테스트 진행중인 api..
def get_report_num_list_by_date(date):
    print('get_report_list')
    rcept_no_list = []
    if date == '':
        now = datetime.datetime.now()  # 2015-04-19 12:11:32.669083
        date = now.strftime('%Y%m%d')
    params = {'crtfc_key': "82784d59168f5d7c14e99c36e596e80888d6eccb",
              'last_reprt_at': 'Y',
              'bgn_de': date,
              'pblntf_detail_ty': 'H002',
              'corp_cls': 'Y'}
    url = "https://opendart.fss.or.kr/api/list.json"

    resonse = requests.get(url, params=params)
    json_dict = json.loads(resonse.text)
    if json_dict['status'] == "000":
        if 'list' in json_dict:
            # data list convert
            for line in json_dict['list']:
                rcept_no_list.append(line['rcept_no'])
    print(rcept_no_list)
    return rcept_no_list
