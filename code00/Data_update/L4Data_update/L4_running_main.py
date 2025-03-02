from L4Data_update.L4Data_preparing import L4Data_preparing
from L4Data_update.L4Holding_update import L4Holding_update
from L4Data_update.L4Info_update import L4Info_update
from L4Data_update.L4Prod_update import L4Prod_update
import global_tools_func.global_tools as gt
import global_setting.global_dic as glv
import pandas as pd
from L4Data_update.tools_func import tools_func
import datetime
from datetime import date
import os
def valid_productCode_withdraw():
    not_running_list=['STH580','SST132']
    inputpath = glv.get('L4_config')
    df = pd.read_excel(inputpath)
    time_now = datetime.datetime.now().strftime("%H:%M")
    if time_now>'15:00':
        product_code_list = ['SGS958']

    else:
        not_running_list.append('SGS958')
        product_code_list = df[~(df['product_code'].isin(not_running_list))]['product_code'].tolist()
    return product_code_list
def valid_productName_withdraw():
    not_running_list=['STH580','SST132']
    inputpath = glv.get('L4_config')
    df = pd.read_excel(inputpath)
    product_name_list = df[~(df['product_code'].isin(not_running_list))]['product_name'].tolist()
    return product_name_list
def target_date_decision_L4():
    today = date.today()
    today=gt.strdate_transfer(today)
    available_date = gt.last_workday_calculate(today)
    return available_date
def L4_running_main(product_code_list,start_date,end_date):
       tf=tools_func()
       working_days_list=gt.working_days_list(start_date,end_date)
       for available_date in working_days_list:
           for product_code in product_code_list:
               print(available_date)
               available_date2=gt.intdate_transfer(available_date)
               lp = L4Data_preparing(product_code, available_date2)
               try:
                   daily_df = lp.raw_L4_withdraw()
               except:
                   product_name=tf.product_NameCode_transfer(product_code)
                   print(product_name+'在'+str(available_date)+'日期没有数据')
                   daily_df=pd.DataFrame()
               if len(daily_df)!=0:
                   lh = L4Holding_update(product_code, available_date2, daily_df)
                   li = L4Info_update(product_code, available_date2, daily_df)
                   available_date_yes=gt.last_workday_calculate(available_date)
                   available_date_yes=gt.intdate_transfer(available_date_yes)
                   lpro = L4Prod_update(product_code,available_date_yes, available_date)
                   lh.L4Holding_processing()
                   li.L4Info_processing()
                   lpro.holding_diff()

def L4_update_main():
    product_code_list=valid_productCode_withdraw()
    target_date=target_date_decision_L4()
    target_date2 = target_date
    for i in range(3):
         target_date2=gt.last_workday_calculate(target_date2)
    L4_running_main(product_code_list, target_date2, target_date)
def L4_history_main(mode,product_name_list,start_date,end_date):
    outputpath=glv.get('output_l4')
    tf = tools_func()
    product_code_list=[]
    if mode=='all':
        product_name_list=valid_productName_withdraw()
    for product_name in product_name_list:
        product_code=tf.product_CodeName_transfer(product_name)
        product_code_list.append(product_code)
    try:
        os.listdir(outputpath)
    except:
        if start_date>'2024-06-01':
            start_date='2024-06-01'
    L4_running_main(product_code_list, start_date, end_date)