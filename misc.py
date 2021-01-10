
import pandas as pd

from pandas.tests.io.excel.test_xlsxwriter import xlsxwriter

#########################################################################################

#EXCEL write
def toExcel(excel_name, write_sheet_name, df):
        writer = pd.ExcelWriter(excel_name + '.xlsx', engine='xlsxwriter')
        df.to_excel(writer, sheet_name=write_sheet_name,  index = False)
        writer.close()

#엑셀 key, value 쓰기
def write_key_value_to_excel(data):
        workbook = xlsxwriter.Workbook('corp_data.xlsx')
        worksheet = workbook.add_worksheet()

        row = 0
        col = 0

        for key in data.keys():
                row += 1
                worksheet.write(row, col, key)
                for item in data[key]:
                        worksheet.write(row, col + 1, item)
                        row += 1

        workbook.close()

#########################################################################################