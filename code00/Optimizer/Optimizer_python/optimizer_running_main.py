import sys
import global_tools_func.global_tools as gt
import Optimizer_python.global_setting.global_dic as glv
import os
import pandas as pd
from datetime import date
from main.optimizer_main_python import Optimizer_main
from data_prepare.data_prepare import time_series_data_preparing,cross_sectional_data_preparing
def Score_name_withdraw():
    inputpath_mode_dic=glv.get('mode_dic')
    df_mode_dic=pd.read_excel(inputpath_mode_dic)
    score_name_list=df_mode_dic['score_name'].tolist()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath = os.path.join(inputpath, 'config.xlsx')
    df_config = pd.read_excel(inputpath)
    inputpath_trading=df_config['inputpath_trading_config'].tolist()[0]
    df_trading_hs300=pd.read_excel(inputpath_trading,sheet_name='沪深300')
    df_trading_zz500 = pd.read_excel(inputpath_trading, sheet_name='中证500')
    df_trading_zz1000 = pd.read_excel(inputpath_trading, sheet_name='中证1000')
    score_list1 = df_trading_hs300['score_name'].tolist()
    score_list2 = df_trading_zz500['score_name'].tolist()
    score_list3 = df_trading_zz1000['score_name'].tolist()
    score_list_priority = list(set(score_list1) | set(score_list2) | set(score_list3))
    score_list_rest=list(set(score_name_list)-set(score_list_priority))
    return score_list_priority,score_list_rest
def optimizer_history():#跑一个时间段的
    df_indexreturn, df_stock_return, df_st, df_stockuniverse = time_series_data_preparing()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath = os.path.join(inputpath, 'config_history.xlsx')
    df_config = pd.read_excel(inputpath)
    for i in range(len(df_config)):
        start_date = df_config['start_date'].tolist()[i]
        end_date = df_config['end_date'].tolist()[i]
        score_name = df_config['score_name'].tolist()[i]
        available_time_list = gt.working_days_list(start_date, end_date)
        for target_date in available_time_list:
            available_date=gt.last_workday_calculate(target_date)
            df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor = cross_sectional_data_preparing(
                available_date)
            Opm = Optimizer_main(df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure,
                                 df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor,
                                 df_indexreturn, df_stock_return, df_st)
            outputpath = Opm.main_updating(score_name, target_date)
def cross_section_optimizer_update_part1(target_date,score_name,df_indexreturn, df_stock_return, df_st, df_stockuniverse,df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor):
    Opm=Optimizer_main(df_stockpool,df_hs300, df_zz500, df_zz1000, df_zz2000,df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure,df_stock_factor,df_indexreturn,df_stock_return,df_st)
    outputpath=Opm.main_updating(score_name,target_date)
    return outputpath
def cross_section_optimizer_update_part2(target_date,score_name):
    target_date2=gt.intdate_transfer(target_date)
    inputpath=glv.get('output_optimizer')
    outputpath=glv.get('result')
    inputpath_daily = os.path.join(inputpath, score_name)
    inputpath_daily = os.path.join(inputpath_daily, target_date)
    inputpath_weight = os.path.join(inputpath_daily, 'weight.csv')
    inputpath_code = os.path.join(inputpath_daily, 'Stock_code.csv')
    df_weight = pd.read_csv(inputpath_weight,header=None)
    df_code = gt.readcsv(inputpath_code)
    df_final = pd.concat([df_code, df_weight],axis=1)
    df_final.columns = ['code', 'weight']
    outputpath_daily = os.path.join(outputpath, score_name)
    gt.folder_creator(outputpath_daily)
    outputpath_daily = os.path.join(outputpath_daily, str(score_name) + '_' + target_date2 + '.csv')
    df_final.to_csv(outputpath_daily, index=False)
def daily_update_part1(priority):#日更触发这个
    target_date=date.today()
    target_date=gt.strdate_transfer(target_date)
    available_date=gt.last_workday_calculate(target_date)
    df_indexreturn, df_stock_return, df_st, df_stockuniverse=time_series_data_preparing()
    df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor=cross_sectional_data_preparing(available_date)
    score_list_priority, score_list_rest=Score_name_withdraw()
    if priority==True:
        for score_name in score_list_priority:
            outputpath=cross_section_optimizer_update_part1(target_date,score_name,df_indexreturn, df_stock_return, df_st, df_stockuniverse,df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor)
    else:
        for score_name in score_list_rest:
            if score_name in ['fm50_vp50_hs300','fm50_vp50_zz500','fm50_vp50_zz1000']:
                pass
            else:
                outputpath=cross_section_optimizer_update_part1(target_date,score_name,df_indexreturn, df_stock_return, df_st, df_stockuniverse,df_stockpool, df_hs300, df_zz500, df_zz1000, df_zz2000, df_hs300_exposure, df_zz500_exposure, df_zz1000_exposure, df_zz2000_exposure, df_stock_factor)
def cross_section_optimizer_update_part2(priority):
    target_date=date.today()
    target_date=gt.strdate_transfer(target_date)
    score_list_priority, score_list_rest = Score_name_withdraw()
    if priority==True:
        for score_name in score_list_priority:
            cross_section_optimizer_update_part2(target_date, score_name)
    else:
        for score_name in score_list_rest:
            if score_name in ['fm50_vp50_hs300','fm50_vp50_zz500','fm50_vp50_zz1000']:
                pass
            else:
                cross_section_optimizer_update_part2(target_date, score_name)


