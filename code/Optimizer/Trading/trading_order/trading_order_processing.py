import os
import pandas as pd
# import chinese_calendar as cal
from datetime import datetime, timedelta, date
import global_tools_func.global_tools as gt
from Trading.global_setting import global_dic as glv
from Trading.trading_weight.trading_weight_selecting import product_target_weight_withdraw,target_date_decision
from Trading.trading_order.trading_order_xuanye import trading_order_xy_main,t0_trading_xy_main
from Trading.trading_order.trading_order_renrui import trading_order_renrui
def stock_close_withdraw(df_today):
    target_date=target_date_decision()
    available_date = gt.last_workday_calculate(target_date)
    available_date=gt.intdate_transfer(available_date)
    inputpath2 = glv.get('stock_close')
    inputpath2=gt.file_withdraw(inputpath2,available_date)
    df_close=gt.readcsv(inputpath2)
    code_list=df_today['code'].tolist()
    df_close=df_close[code_list]
    df_close = df_close.T
    df_close.reset_index(inplace=True)
    df_close.columns = ['code', 'close']
    if df_close.isna().sum().tolist() !=[0,0]:
        print('Stock_data_close.csv数据存在Nan值，请查看')
        raise ValueError
    return df_close
def parameter_getting(product_name):
    target_date=target_date_decision()
    inputpath1=glv.get('input_product')
    inputpath=os.path.join(inputpath1,product_name)
    target_date2=gt.intdate_transfer(target_date)
    inputpath=gt.file_withdraw(inputpath,target_date2)
    df=pd.read_excel(inputpath)
    product_name=df['产品名称'].tolist()[0]
    index_type=df['标的名称'].tolist()[0]
    account_money=df['account_money'].tolist()[0]
    t0_money=df['t0_money'].tolist()[0]
    trading_time=df['trading_time'].tolist()[0]
    end_time=df['end_time'].tolist()[0]
    return product_name,index_type,account_money,t0_money,trading_time,end_time
def holding_withdraw(product_name):
    target_date=target_date_decision()
    available_date=gt.last_workday_calculate(target_date)
    available_date2=gt.intdate_transfer(available_date)
    inputpath_holding=glv.get('input_holding')
    inputpath_yes=os.path.join(inputpath_holding,product_name)
    inputpath_yes=gt.file_withdraw(inputpath_yes,available_date2)
    df_yes =gt.readcsv(inputpath_yes)
    df_yes=df_yes[['StockCode','HoldingQty']]
    return df_yes
def holding_withdraw2(product_name):
    target_date=target_date_decision()
    available_date=gt.last_workday_calculate(target_date)
    available_date2=gt.intdate_transfer(available_date)
    inputpath_holding=glv.get('input_holding')
    inputpath_yes=os.path.join(inputpath_holding,product_name)
    inputpath_yes=gt.file_withdraw(inputpath_yes,available_date2)
    df_yes =gt.readcsv(inputpath_yes)
    return df_yes
def trading_xy_main(trading_mode,t0_mode):
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    target_time=target_date_decision()
    product_name, index_type, account_money, t0_money, trading_time, end_time = parameter_getting(product_name='宣夜惠盈1号')
    stock_money = account_money - t0_money
    df_today= product_target_weight_withdraw(product_name, yesterday=False)
    df_close = stock_close_withdraw(df_today)
    df_yes = holding_withdraw(product_name)
    df_today2=trading_order_xy_main(df_today, df_yes, df_close, target_time, stock_money, trading_time, product_name,
                          trading_mode)
    t0_trading_xy_main(df_yes, df_today2,target_time,t0_money,end_time,product_name,t0_mode)
def trading_rr_main():
    target_date = target_date_decision()
    product_name, index_type, account_money, t0_money, trading_time, end_time = parameter_getting(
        product_name='仁睿价值精选1号')
    df_today = product_target_weight_withdraw(product_name, yesterday=False)
    df_close = stock_close_withdraw(df_today)
    df_yes = holding_withdraw2(product_name)
    df_final= trading_order_renrui(df_today,df_yes,df_close,target_date,account_money,product_name)
