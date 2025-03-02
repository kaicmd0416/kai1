from Portfolio_tracking.portfolio_performance.cross_section_portfolio_performance import cross_portfolio_update_main
from Portfolio_tracking.timeseries_pros.timeSeries_portTracking import PortTracking_timeSeries_main
import  datetime
import global_tools_func.global_tools as gt
def backtesting_update_main():#每天1515跑完之后跑，等历史数据补齐之后，将里面注释的释放
    today=datetime.date.today()
    date=gt.strdate_transfer(today)
    cross_portfolio_update_main(date)
    PortTracking_timeSeries_main()
def backtesting_history_main(start_date,end_date):
    working_day_list=gt.working_days_list(start_date,end_date)
    for target_date in working_day_list:
        print(target_date)
        cross_portfolio_update_main(target_date)
    PortTracking_timeSeries_main()
def backtesting_update_bu(start_date,end_date):
    working_days_list=gt.working_days_list(start_date,end_date)
    for date in working_days_list:
        cross_portfolio_update_main(date)
    PortTracking_timeSeries_main()

#backtesting_update_main()
#backtesting_history_main('2025-02-10', '2025-02-19')
# backtesting_history_main('2025-01-01','2025-02-21')
#time_series_portfolio_performance_update()
#backtesting_update_bu(start_date='2024-01-01',end_date='2024-11-19')
