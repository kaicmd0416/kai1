from FactorData_update.factor_update import FactorData_history_main
from MktData_update.MktData_update_main import MktData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from L4Data_update.L4_running_main import L4_history_main
from TimeSeries_update.time_series_data_update import timeSeries_update_main_part1,timeSeries_update_main_part2
def Data_history(start_date,end_date):#end_date是最新日期，start_date自由选择
    fm=File_moving('time_2')
    fm.file_moving_history_main()
    MktData_update_main(start_date, end_date)
    FactorData_history_main(start_date, end_date)
    score_update_main('fm', start_date, end_date)
    timeSeries_update_main_part1()
    timeSeries_update_main_part2()
    L4_history_main('all',None,start_date, end_date)

if __name__ == '__main__':
    Data_history(start_date='2025-02-10',end_date='2025-02-20')
    # fm = File_moving('time_2')
    # fm.file_moving_history_main()
    pass