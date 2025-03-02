import os
import pandas as pd
import global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import numpy as np
def stock_pool_withdraw(available_date):
    inputpath_stock_pool = glv.get('data_other')
    inputpath_stockdata=glv.get('output_stockclose')
    available_date2=gt.intdate_transfer(available_date)
    inputpath_stockdata=gt.file_withdraw(inputpath_stockdata,available_date2)
    inputpath_stock_pool = os.path.join(inputpath_stock_pool, 'StockUniverse_new.csv')
    df_stock_pool = gt.readcsv(inputpath_stock_pool)
    code_list1 = df_stock_pool['S_INFO_WINDCODE'].tolist()
    df=gt.readcsv(inputpath_stockdata)
    code_list2 = df['code'].tolist()
    stock_pool = list(set(code_list1) | set(code_list2))
    stock_pool.sort()
    slice_df_stock_pool = pd.DataFrame()
    slice_df_stock_pool['code'] = stock_pool
    return slice_df_stock_pool
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
def index_return_update():
    outputpath_timeseries=glv.get('output_timeseries')
    gt.folder_creator2(outputpath_timeseries)
    outputpath=os.path.join(outputpath_timeseries,'index_return.csv')
    try:
        df_index=gt.readcsv(outputpath)
        df_index['valuation_date']=pd.to_datetime(df_index['valuation_date'])
        df_index['valuation_date']=df_index['valuation_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        end_date=df_index['valuation_date'].tolist()[-1]
    except:
        df_index=pd.DataFrame(columns=['valuation_date','上证50','沪深300','中证500','中证1000','中证2000'])
        end_date=None
    inputpath_index_return=glv.get('output_indexreturn')
    inputlist=os.listdir(inputpath_index_return)
    df_input=pd.DataFrame(inputlist,columns=['name'])
    df_input['type']=df_input['name'].apply(lambda x: str(x)[-3:])
    df_input=df_input[df_input['type']=='csv']
    df_input['time']=df_input['name'].apply(lambda x: str(x)[-12:-4])
    df_input['time']=pd.to_datetime(df_input['time'])
    df_input['time']=df_input['time'].apply(lambda x: x.strftime("%Y-%m-%d"))
    if end_date:
        df_input=df_input[df_input['time']>end_date]
    if len(df_input)>0:
        namelist=df_input['name'].tolist()
        for i in range(len(namelist)):
            name=namelist[i]
            inputpath_daily=os.path.join(inputpath_index_return,name)
            slice_df=gt.readcsv(inputpath_daily)
            try:
                  slice_df=slice_df[['valuation_date','上证50','沪深300','中证500','中证1000','中证2000','中证A500']]
            except:
                slice_df = slice_df[
                    ['valuation_date', '上证50', '沪深300', '中证500', '中证1000', '中证2000']]
                slice_df['中证A500']=0
            if slice_df.isnull().values.any()==True:
                print('当日文件存在nan值，请检验')
                print(slice_df)
                raise ValueError
            else:
                df_index=pd.concat([df_index,slice_df])
        df_index['valuation_date']=df_index['valuation_date'].apply(lambda x: str(x)[:10])
        df_index['valuation_date'] = pd.to_datetime(df_index['valuation_date'])
        df_index['valuation_date'] = df_index['valuation_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_index.to_csv(outputpath,index=False,encoding='gbk')
    else:
        print('index_return.csv已经更新到最新日期--'+str(end_date))
def stock_data_update(): #提取stockdata里面的股票日收益率并输出成df
    inputpath_stock_pool=glv.get('data_other')
    inputpath_stock_pool=os.path.join(inputpath_stock_pool,'StockUniverse_new.csv')
    df_stockpool=gt.readcsv(inputpath_stock_pool)
    stock_pool=df_stockpool['S_INFO_WINDCODE'].tolist()
    inputpath_stockreturn = glv.get('output_stockreturn')
    inputpath_stockclose=glv.get('output_stockclose')
    inputlist = os.listdir(inputpath_stockclose)
    outputpath_stock=glv.get('output_timeseries')
    outputpath_stockReturn = os.path.join(outputpath_stock, 'stock_return.csv')
    outputpath_stockClose = os.path.join(outputpath_stock, 'stock_close.csv')
    try:
        df_stockreturn=gt.readcsv(outputpath_stockReturn)
        end_date=df_stockreturn['valuation_date'].tolist()[-1]
    except:
        df_stockreturn=pd.DataFrame(columns=['valuation_date']+stock_pool)
        end_date=None
    try:
        df_stockclose= gt.readcsv(outputpath_stockClose)
        end_date2 = df_stockclose['valuation_date'].tolist()[-1]
    except:
        df_stockclose = pd.DataFrame(columns=['valuation_date']+stock_pool)
        end_date2 = None
    df_time=pd.DataFrame()
    df_time['name']=inputlist
    df_time['time']=df_time['name'].apply(lambda x: str(x)[-12:-4])
    df_time['time']=pd.to_datetime(df_time['time'])
    df_time['time']=df_time['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    if end_date:
        df_time=df_time[df_time['time']>end_date]
    if end_date2:
        df_time = df_time[df_time['time'] > end_date2]
    time_list=df_time['time'].tolist()
    if len(time_list)!=0:
        for time in time_list:
            print(time)
            available_date=gt.intdate_transfer(time)
            daily_inputpath_stockreturn=gt.file_withdraw(inputpath_stockreturn,available_date)
            daily_inputpath_stockclose = gt.file_withdraw(inputpath_stockclose, available_date)
            daily_df_stockreturn=gt.readcsv(daily_inputpath_stockreturn)
            daily_df_stockclose=gt.readcsv(daily_inputpath_stockclose)
            if daily_df_stockreturn.isnull().values.any()==True or daily_df_stockclose.isnull().values.any()==True:
                print('当日文件存在nan值，请检验')
                print(daily_df_stockreturn)
                print(daily_df_stockclose)
                raise ValueError
            else:
                df_stockreturn = pd.concat([df_stockreturn, daily_df_stockreturn])
                df_stockclose = pd.concat([df_stockclose, daily_df_stockclose])
        df_stockreturn['valuation_date'] = df_stockreturn['valuation_date'].apply(lambda x: str(x)[:10])
        df_stockclose['valuation_date'] = df_stockclose['valuation_date'].apply(lambda x: str(x)[:10])
        df_stockreturn['valuation_date'] = pd.to_datetime(df_stockreturn['valuation_date'])
        df_stockreturn['valuation_date'] = df_stockreturn['valuation_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_stockclose['valuation_date'] = pd.to_datetime(df_stockclose['valuation_date'])
        df_stockclose['valuation_date'] =df_stockclose['valuation_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_stockreturn.to_csv(outputpath_stockReturn,index=False)
        df_stockclose.to_csv(outputpath_stockClose, index=False)
    else:
        print("stock_return.csv和stock_close.csv已经更新到最新日期--"+str(end_date))
def factor_return_update():
    outputpath_stock = glv.get('output_timeseries')
    inputpath_factor=glv.get('output_factor_return')
    outputpath_index=os.path.join(outputpath_stock,'lnmodel.csv')
    inputlist = os.listdir(inputpath_factor)
    df_lnmodel=pd.DataFrame()
    df_lnmodel['name']=inputlist
    df_lnmodel['date']=df_lnmodel['name'].apply(lambda x: str(x)[-12:-4])
    df_lnmodel['date']=pd.to_datetime(df_lnmodel['date'])
    try:
        df_factor_exposure=gt.readcsv(outputpath_index)
        end_date=df_factor_exposure['valuation_date'].tolist()[-1]
    except:
        df_factor_exposure=pd.DataFrame()
        end_date=None
    if end_date:
        df_lnmodel=df_lnmodel[df_lnmodel['date']>end_date]
    date_list=df_lnmodel['date'].tolist()
    if len(df_lnmodel)!=0:
        for available_date in date_list:
              available_date=gt.intdate_transfer(available_date)
              daily_inputpath_factor_return=gt.file_withdraw(inputpath_factor,available_date)
              daily_df_factorreturn=gt.readcsv(daily_inputpath_factor_return)
              df_factor_exposure = pd.concat([df_factor_exposure, daily_df_factorreturn])
        df_factor_exposure.fillna(method='ffill', inplace=True)
        df_factor_exposure.fillna(method='bfill', inplace=True)
        df_factor_exposure.fillna(0.005,inplace=True)
        df_factor_exposure['valuation_date'] = df_factor_exposure['valuation_date'].apply(lambda x: str(x)[:10])
        df_factor_exposure['valuation_date'] = pd.to_datetime(df_factor_exposure['valuation_date'])
        df_factor_exposure['valuation_date'] = df_factor_exposure['valuation_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_factor_exposure.to_csv(outputpath_index, encoding='gbk',index=False)
    else:
        print("lnmodel.csv已经更新到最新日期--" + str(end_date))
def factor_stockpool_update():  # 计算每天因子有效的股票数据
    outputpath_stockpool = glv.get('output_timeseries')
    outputpath_stockpool = os.path.join(outputpath_stockpool, 'Stock_pool.csv')
    inputpath_factor = glv.get('output_factor_stockpool')
    list_factor = os.listdir(inputpath_factor)
    df_lnmodel = pd.DataFrame()
    df_lnmodel['name'] = list_factor
    df_lnmodel['date'] = df_lnmodel['name'].apply(lambda x: str(x)[-12:-4])
    df_lnmodel['date'] = pd.to_datetime(df_lnmodel['date'])
    try:
        df_stockpool = gt.readcsv(outputpath_stockpool)
        end_date = df_stockpool.columns.tolist()[-1]
    except:
        df_stockpool = pd.DataFrame()
        end_date = None
    if end_date:
        df_lnmodel = df_lnmodel[df_lnmodel['date'] > end_date]
    date_list = df_lnmodel['date'].tolist()
    if len(df_lnmodel) > 0:
        df_final = pd.DataFrame()
        for available_date in date_list:
            available_date=gt.intdate_transfer(available_date)
            daily_inputpath_stockpool=gt.file_withdraw(inputpath_factor,available_date)
            daily_df_stockpool = gt.readcsv(daily_inputpath_stockpool)
            stock_code=daily_df_stockpool[daily_df_stockpool.columns.tolist()[0]].tolist()
            missing_len = 5600 - len(stock_code)
            stock_code += [None] * missing_len
            df_final[daily_df_stockpool.columns.tolist()[0]] = stock_code
        df_stockpool = pd.concat([df_stockpool, df_final], axis=1)
        df_stockpool.to_csv(outputpath_stockpool, index=False)
    else:
        print('Stock_pool.csv已经更新到最新日期--' + str(end_date))

def factor_indexexposure_update(index_type):  # 计算每天因子有效的股票数据
    outputpath_stockpool = glv.get('output_timeseries')
    outputpath_stockpool = os.path.join(outputpath_stockpool, index_type+'因子风险暴露.csv')
    inputpath_factor = glv.get('output_indexexposure')
    inputpath_factor = os.path.join(inputpath_factor, index_type)
    list_factor = os.listdir(inputpath_factor)
    df_lnmodel = pd.DataFrame()
    df_lnmodel['name'] = list_factor
    df_lnmodel['date'] = df_lnmodel['name'].apply(lambda x: str(x)[-12:-4])
    df_lnmodel['date'] = pd.to_datetime(df_lnmodel['date'])
    try:
        df_factor_exposure = gt.readcsv(outputpath_stockpool)
        end_date = df_factor_exposure['valuation_date'].tolist()[-1]
    except:
        df_factor_exposure= pd.DataFrame()
        end_date = None
    if end_date:
        end_date=pd.to_datetime(end_date)
        end_date=gt.strdate_transfer(end_date)
        df_lnmodel = df_lnmodel[df_lnmodel['date'] > end_date]
    date_list = df_lnmodel['date'].tolist()
    if len(df_lnmodel) > 0:
        df_final = pd.DataFrame()
        for available_date in date_list:
            available_date=gt.intdate_transfer(available_date)
            if index_type == '中证2000' and int(available_date) < 20230901:
                available_date = '20230901'
            if index_type == '中证A500' and int(available_date) < 20241008:
                available_date = '20241008'
            daily_inputpath_stockpool=gt.file_withdraw(inputpath_factor,available_date)
            daily_df_stockpool = gt.readcsv(daily_inputpath_stockpool)
            df_final=pd.concat([df_final,daily_df_stockpool],axis=0,ignore_index=True)
        df_factor_exposure = pd.concat([df_factor_exposure, df_final],axis=0,ignore_index=True)
        df_factor_exposure.replace(0, np.nan, inplace=True)
        df_factor_exposure.fillna(method='ffill', inplace=True)
        df_factor_exposure.fillna(method='bfill', inplace=True)
        df_factor_exposure.fillna(0.0005, inplace=True)
        df_factor_exposure['valuation_date'] = df_factor_exposure['valuation_date'].apply(lambda x: str(x)[:10])
        df_factor_exposure['valuation_date'] = pd.to_datetime(df_factor_exposure['valuation_date'])
        df_factor_exposure['valuation_date'] = df_factor_exposure['valuation_date'].apply(
            lambda x: x.strftime("%Y-%m-%d"))
        df_factor_exposure.to_csv(outputpath_stockpool, index=False,encoding='gbk')
    else:
        print('index_exposure.csv已经更新到最新日期--' + str(end_date))
def timeSeries_update_main_part1():
    index_return_update()
    stock_data_update()
def timeSeries_update_main_part2():
    factor_return_update()
    factor_stockpool_update()
    for index_type in ['沪深300','中证500','中证1000','中证2000','中证A500']:
          index_type2= file_name_withdraw(index_type)
          factor_indexexposure_update(index_type=index_type2)

