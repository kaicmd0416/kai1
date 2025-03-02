import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import os
import pandas as pd
from Signal_tracking.main.running_main import analyse_main
import datetime
from datetime import date
from timeseries_pro.timeSeries_weightTracking import ScoreTracking_timeSeries_main_weight
from timeseries_pro.timeSeries_portSplitTracking import ScoreTracking_timeSeries_main_port
def timeSeries_main(portfolio_name_list):
    ScoreTracking_timeSeries_main_weight(portfolio_name_list)
    ScoreTracking_timeSeries_main_port(portfolio_name_list)
def tracking_score_withdraw():
    inputpath=glv.get('valid_score')
    df=pd.read_excel(inputpath)
    base_score = df['base_score'].unique().tolist()
    base_score=[i for i in base_score if str(i)[:2]=='rr' or str(i)[:4]=='vp02']
    score_name_list=df['score_name'].tolist()
    index_type_list=['沪深300','中证500','中证1000','中证A500']
    return score_name_list,base_score,index_type_list
def cross_section_update_main(target_date):
    score_name_list,base_score,index_type_list=tracking_score_withdraw()
    arm=analyse_main(target_date)
    arm.score_running_main(base_score,index_type_list)
    arm.portfolio_running_main(score_name_list)
    return score_name_list
def update_main(): #触发这个
    today=date.today()
    if gt.is_workday2()==True:
        target_date=today
    else:
        target_date=gt.last_workday_calculate(today)
    score_name_list=cross_section_update_main(target_date)
    timeSeries_main(score_name_list)
def history_main(start_date,end_date):
    working_days_list=gt.working_days_list(start_date,end_date)
    for target_date in working_days_list:
        print(target_date)
        score_name_list=cross_section_update_main(target_date)
    timeSeries_main(score_name_list)
#update_main()
# history_main(start_date='2025-01-04',end_date='2025-02-19')
