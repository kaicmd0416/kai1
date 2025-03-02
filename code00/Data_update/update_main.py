from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from TimeSeries_update.time_series_data_update import timeSeries_update_main_part1,timeSeries_update_main_part2
from Time_tools.time_tools import time_tools
from Data_checking.data_check import checking
import global_tools_func.global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
def daily_update_705():
    tt=time_tools()
    ck = checking('time_1')
    date=tt.target_date_decision_705()
    date = gt.strdate_transfer(date)
    score_type='fm'
    score_update_main(score_type,date, date)
    ck.checking_main()
def daily_update_1515():
    tt = time_tools()
    fm=File_moving('time_2')
    ck=checking('time_2')
    date = tt.target_date_decision_1515()
    date=gt.strdate_transfer(date)
    MktData_update_main(date,date)
    ck.checking_main()
    timeSeries_update_main_part1()
    fm.file_moving_update_main()
def daily_update_1800():
    tt = time_tools()
    ck = checking('time_3')
    fm = File_moving('time_3')
    date = tt.target_date_decision_1800()
    date = gt.strdate_transfer(date)
    fu = FactorData_update(date,date)
    fu.FactorData_update_main()
    ck.checking_main()
    timeSeries_update_main_part2()
    fm.file_moving_update_main()
def daily_update_auto(): #只触发这一个大的
    tt = time_tools()
    time_zoom=tt.time_zoom_decision()
    if time_zoom=='time_1':
        daily_update_705()
    elif time_zoom=='time_2':
        daily_update_1515()
    elif time_zoom=='time_3':
        daily_update_1800()
    else:
        print('当前时间段没有任务可以触发')
def daily_update_l4():
    L4_update_main()


if __name__ == '__main__':
    L4_update_main()
    # daily_update_auto()
    daily_update_705()
    daily_update_1515()
    daily_update_1800()
    pass
