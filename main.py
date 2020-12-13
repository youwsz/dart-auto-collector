import opendart
import misc

dartMgr = opendart.OpenDartManager()
misc.toExcel('2020_3Q', dartMgr.get_performance_table(2020, 3))


##
#네이밍 규칙
#
#변수
#:소문자로 시작, 중간에 _
#
#상수
#:모두 대문자, 중간에 _
#
#함수
#:소문자로 시작, 중간에 _
##