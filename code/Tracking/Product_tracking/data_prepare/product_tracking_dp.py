import datetime
import os
import pandas as pd
import sys
import Product_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Product_tracking.tools_func.tools_func import product_name_transfer
class data_prepared:
    def __init__(self,available_date):
        self.available_date=available_date
    def realtime_data_withdraw(self,yes):
        inputpath_realtime=glv.get('realtime_data')
        if yes==False:
           available_date=gt.intdate_transfer(self.available_date)
        else:
            yesterday=gt.last_workday_calculate(self.available_date)
            available_date = gt.intdate_transfer(yesterday)
        inputpath_realtime=gt.file_withdraw(inputpath_realtime,available_date)
        realtime_data_df = pd.read_excel(inputpath_realtime, sheet_name='option_info')
        realtime_data_future = pd.read_excel(inputpath_realtime, sheet_name='future_info')
        realtime_data_index=gt.crossSection_index_return_withdraw2(available_date)
        realtime_data_future = realtime_data_future[['简称', '日期', '时间', '现价', '合约系数', '前结算价']]
        realtime_data_future.columns = ['future', 'date', 'time', 'future_price', 'ratio', '前结算价']
        realtime_data_future.dropna(axis=0, inplace=True)
        return realtime_data_df, realtime_data_future,realtime_data_index
    def stock_factor_exposure_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_factor = glv.get('input_factorexposure')
        inputpath_other = glv.get('input_other')
        inputpath_other = os.path.join(inputpath_other, 'StockUniverse_new.csv')
        inputpath_factor = gt.file_withdraw(inputpath_factor, available_date2)
        df_factor = gt.readcsv(inputpath_factor)
        df_stockuniverse = gt.readcsv(inputpath_other)
        stock_list = df_stockuniverse['S_INFO_WINDCODE'].tolist()
        df_factor['code'] = stock_list
        return df_factor

    def crossSection_stock_close_withdraw(self,yes):
        if yes==False:
            target_date2=gt.intdate_transfer(self.available_date)
        else:
            available_date=gt.last_workday_calculate(self.available_date)
            target_date2 = gt.intdate_transfer(available_date)
        inputpath_stockreturn = glv.get('input_stockclose')
        input_name = gt.file_withdraw(inputpath_stockreturn, target_date2)
        inputpath_stockreturn = os.path.join(inputpath_stockreturn, input_name)
        df_stock = gt.readcsv(inputpath_stockreturn)
        df_stock.set_index('valuation_date', inplace=True)
        df_stock = df_stock.T
        df_stock.reset_index(inplace=True)
        df_stock.columns = ['code', 'close']
        return df_stock

    def crossSection_index_return_withdraw(self,index_type,yes):
        if yes==False:
            target_date2=gt.intdate_transfer(self.available_date)
        else:
            available_date=gt.last_workday_calculate(self.available_date)
            target_date2 = gt.intdate_transfer(available_date)
        inputpath_indexreturn = glv.get('input_indexreturn')
        file_name = gt.file_withdraw(inputpath_indexreturn, target_date2)
        inputpath_index = os.path.join(inputpath_indexreturn, file_name)
        df = gt.readcsv(inputpath_index, dtype=str)
        index_return=df[index_type].tolist()[0]
        return index_return
    def index_exposure_withdraw(self):
        df_sz50 = gt.crossSection_index_factorexposure_withdraw_new(index_type='上证50',available_date=self.available_date)
        df_hs300 = gt.crossSection_index_factorexposure_withdraw_new(index_type='沪深300', available_date=self.available_date)
        df_zz500 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证500', available_date=self.available_date)
        df_zz1000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证1000', available_date=self.available_date)
        df_zz2000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证2000', available_date=self.available_date)
        df_zzA500=gt.crossSection_index_factorexposure_withdraw_new(index_type='中证A500', available_date=self.available_date)
        df_sz50.drop(columns='valuation_date', inplace=True)
        df_hs300.drop(columns='valuation_date',inplace=True)
        df_zz500.drop(columns='valuation_date',inplace=True)
        df_zz1000.drop(columns='valuation_date',inplace=True)
        df_zz2000.drop(columns='valuation_date', inplace=True)
        df_zzA500.drop(columns='valuation_date', inplace=True)
        return df_sz50,df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500
    def convertible_bond_withdraw(self,yes):
        if yes==False:
           available_date=gt.intdate_transfer(self.available_date)
        else:
            yesterday=gt.last_workday_calculate(self.available_date)
            available_date = gt.intdate_transfer(yesterday)
        try:
            inputpath = glv.get('convertible_bond')
            inputpath = gt.file_withdraw(inputpath, available_date)
            df = gt.readcsv(inputpath)
            df.columns = ['code', 'index_code', 'convert_price', 'price', 'duration']
        except:
            df = pd.DataFrame()
        return df
    def timeseries_stockreturn_withdraw(self):
        inputpath=glv.get('stock_return')
        df = gt.readcsv(inputpath)
        df = df[df['valuation_date'] <= self.available_date]
        if len(df)<240:
            print('timeseries_stock_return时间过短')
        return df
class product_data:
    def __init__(self,product_name,available_date):
        self.product_name=str(product_name)
        self.product_name2=str(product_name_transfer(product_name))
        self.available_date=available_date
    def paper_portfolio_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath=glv.get('paper_holding')
        inputpath=os.path.join(inputpath,str(self.product_name))
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        return df
    def product_index_withdraw(self):
        inputpath_product = glv.get('product_detail')
        df_proindex = pd.read_excel(inputpath_product)
        index_type = df_proindex[df_proindex['product_name'] == self.product_name]['index_type'].tolist()[0]
        return index_type
    def position_withdraw(self):
        inputpath=glv.get('holding_info')
        inputpath=os.path.join(inputpath,self.product_name2)
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df_stock=pd.read_excel(inputpath,sheet_name='stock_tracking')
        df_bond = pd.read_excel(inputpath, sheet_name='bond_tracking')
        df_cbond=pd.read_excel(inputpath, sheet_name='c_bond_tracking')
        df_future = pd.read_excel(inputpath, sheet_name='future_tracking')
        df_option= pd.read_excel(inputpath, sheet_name='option_tracking')
        df_etf=pd.read_excel(inputpath,sheet_name='etf_tracking')
        return df_stock,df_bond,df_cbond,df_future,df_option,df_etf
    def yes_position_withdraw(self):
        inputpath = glv.get('holding_info')
        inputpath = os.path.join(inputpath, self.product_name2)
        available_date2=gt.last_workday_calculate(self.available_date)
        available_date2 = gt.intdate_transfer(available_date2)
        inputpath = gt.file_withdraw(inputpath, available_date2)
        df_bond_yes = pd.read_excel(inputpath, sheet_name='bond_tracking')
        df_bond_yes = df_bond_yes[['代码', '市价']]
        df_bond_yes.rename(columns={'市价':'昨日市价'},inplace=True)
        df_etf_yes=pd.read_excel(inputpath,sheet_name='etf_tracking')
        df_etf_yes=df_etf_yes[['代码', '市价']]
        df_etf_yes.rename(columns={'市价':'yes_price','代码':'code'},inplace=True)
        return df_bond_yes,df_etf_yes
    def position_processing(self):
        df_stock, df_bond,df_cbond, df_future, df_option,df_etf=self.position_withdraw()
        df_bond=df_bond[['代码','数量','市价','市值','单位成本']]
        df_option=df_option[['科目名称','方向','数量','市价']]
        df_future = df_future[['科目名称', '方向', '数量','市价']]
        df_stock=df_stock[['代码','数量','市值']]
        df_stock.columns = ['code', 'amount', 'mkt_value']
        df_stock=df_stock.iloc[:-1]
        df_stock=gt.code_transfer(df_stock)
        df_stock['weight']=df_stock['mkt_value']/df_stock['mkt_value'].sum()
        df_cbond=df_cbond[['代码','数量','市值','市价']]
        df_cbond.columns=['code','amount','mkt_value','original_price']
        df_cbond=gt.code_transfer2(df_cbond)
        df_bond_yes,df_etf_yes=self.yes_position_withdraw()
        df_bond=df_bond.merge(df_bond_yes,on='代码',how='left')
        df_bond.loc[df_bond['昨日市价'].isna(),['昨日市价']]=df_bond[df_bond['昨日市价'].isna()]['单位成本']
        df_etf=df_etf[['代码','数量','市价','市值','单位成本']]
        df_etf.columns=['code','quantity','price','mktvalue','单位成本']
        if len(df_etf)>0:
              if len(df_etf_yes)>0:
                   df_etf=df_etf.merge(df_etf_yes,how='left',on='code')
                   df_etf.loc[df_etf['yes_price'].isna(),['yes_price']]=df_etf[df_etf['yes_price'].isna()]['单位成本']
              else:
                  df_etf['yes_price']=df_etf['单位成本'].tolist()
        else:
            df_etf=df_etf
        return df_stock,df_bond,df_cbond,df_future,df_option,df_etf
    def product_info_withdraw(self):
        inputpath = glv.get('product_info')
        inputpath = os.path.join(inputpath, self.product_name2)
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath = gt.file_withdraw(inputpath, available_date2)
        df_info=pd.read_excel(inputpath)
        return df_info
    def asset_yes_withdraw(self):
        yes=gt.last_workday_calculate(self.available_date)
        inputpath = glv.get('product_info')
        inputpath = os.path.join(inputpath, self.product_name2)
        available_date2 = gt.intdate_transfer(yes)
        inputpath = gt.file_withdraw(inputpath, available_date2)
        df_info=pd.read_excel(inputpath)
        asset_value=df_info['资产净值'].tolist()[0]
        net_value_yes=df_info['产品累计净值'].tolist()[0]
        return asset_value,net_value_yes
