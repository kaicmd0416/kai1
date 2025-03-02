import sys
import os
import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
def file_name_withdraw(index_type):
    if index_type == '上证50':
        return 'sz50'
    elif index_type == '沪深300':
        return 'hs300'
    elif index_type == '中证500':
        return 'zz500'
    elif index_type == '中证1000':
        return 'zz1000'
    elif index_type == '中证2000':
        return 'zz2000'
    else:
        return 'zzA500'
def portfolio_config_withdraw():
    inputpath_config=glv.get('portfolio_dic')
    df_config=pd.read_excel(inputpath_config)
    df_config=portfolio_config_processing(df_config)
    return df_config
def portfolio_config_processing(df):

    top_list=['a1_top200','a3_top200','a3_top5']
    top_list2=top_list*4
    top_list2=['ubp500']+top_list2
    index_type_list=[]
    for index_type in ['沪深300','中证500','中证1000','中证A500']:
        index_type_list+=[index_type]*len(top_list)
    index_type_list=['中证500']+index_type_list
    df_add=pd.DataFrame(columns=df.columns.tolist())
    df_add['score_name']=top_list2
    df_add['index_type']=index_type_list
    df=pd.concat([df,df_add])
    return df
def portfolio_weight_withdraw(score_name,target_date,yesterday):
    inputpath_portfolio=glv.get('portfolio_weight')
    inputpath_portfolio=os.path.join(inputpath_portfolio,score_name)
    if yesterday==False:
        target_date2 = gt.intdate_transfer(target_date)
        inputpath_portfolio = gt.file_withdraw(inputpath_portfolio, target_date2)
        df_weight = gt.readcsv(inputpath_portfolio)
        df_weight['weight'] = df_weight['weight'] / df_weight['weight'].sum()
        df_weight.columns = ['code', 'weight']
    else:
        target_date=gt.last_workday_calculate(target_date)
        target_date2 = gt.intdate_transfer(target_date)
        inputpath_portfolio = gt.file_withdraw(inputpath_portfolio, target_date2)
        df_weight = gt.readcsv(inputpath_portfolio)
        if len(df_weight)!=0:
            df_weight['weight'] = df_weight['weight'] / df_weight['weight'].sum()
            df_weight.columns = ['code', 'weight']
    return df_weight