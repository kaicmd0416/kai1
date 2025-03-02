import os
import pandas as pd
import global_setting.global_dic as glv
import global_tools_func.global_tools as gt

class indexReturn_prepare:
    def __init__(self,available_date):
        self.available_date=available_date
    def indexcode_correction(self,df):
        index_code_list = df.columns.tolist()
        if '932000.SH' in index_code_list:
            df.rename(columns={'932000.SH': '932000.CSI'}, inplace=True)
        if '000510.SH' in index_code_list:
            df.rename(columns={'000510.SH': '000510.CSI'}, inplace=True)
        return df

    def raw_choice_index_return_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_choice = glv.get('input_choice_indexreturn')
        inputpath_choice = gt.file_withdraw(inputpath_choice, self.available_date)
        df_choice = gt.readcsv(inputpath_choice)
        df_choice = self.indexcode_correction(df_choice)
        return df_choice

    def raw_ifind_index_return_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_ifind = glv.get('input_ifind_indexreturn')
        inputpath_ifind = gt.file_withdraw(inputpath_ifind, self.available_date)
        df_ifind = gt.readcsv(inputpath_ifind)
        df_ifind = self.indexcode_correction(df_ifind)
        return df_ifind

    def raw_wind_index_return_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_wind = glv.get('input_wind_indexreturn')
        inputpath_wind = gt.file_withdraw(inputpath_wind, self.available_date)
        df_wind = gt.readcsv(inputpath_wind)
        df_wind = self.indexcode_correction(df_wind)
        return df_wind

    def raw_jy_index_return_withdraw(self):  # available_date这里是YYYYMMDD格式
        df_index = pd.DataFrame(
            columns=['valuation_date', '000016.SH', '000300.SH', '000852.SH', '000905.SH', '932000.CSI', '999004.SSI',
                     '000510.CSI'])
        list_code = ['000016', '000300', '000852', '000905', '932000', '999004', '000510']
        inputpath_jy = glv.get('input_jy_indexreturn')
        inputpath_jy = gt.file_withdraw(inputpath_jy, self.available_date)
        df_jy = gt.readcsv(inputpath_jy)
        if len(df_jy) != 0:
            df_jy['new_code'] = df_jy['S_INFO_WINDCODE'].apply(lambda x: str(x)[:6])
            df_jy = df_jy[df_jy['new_code'].isin(list_code)]
            df_jy = df_jy[['S_INFO_WINDCODE', 'S_DQ_PCTCHANGE']]
            df_jy.set_index('S_INFO_WINDCODE', inplace=True)
            df_jy = df_jy.T
            df_jy.reset_index(inplace=True, drop=True)
            available_date = pd.to_datetime(self.available_date)
            available_date = available_date.strftime('%Y-%m-%d')
            df_jy['valuation_date'] = available_date
            df_jy = self.indexcode_correction(df_jy)
            df_index = pd.concat([df_index, df_jy])
        else:
            df_index = pd.DataFrame()
        return df_index
class indexComponent_prepare:
    def __init__(self,available_date):
        self.available_date=available_date
    def file_name_withdraw(self,index_type,source_name):
        if index_type == '上证50':
            if source_name=='jy':
                return 'sz50Monthly'
            else:
                return'sz50'
        elif index_type == '沪深300':
            if source_name == 'jy':
                 return 'csi300Monthly'
            else:
                return 'hs300'
        elif index_type == '中证500':
            if source_name == 'jy':
                 return 'zz500Monthly'
            else:
                return'zz500'
        elif index_type == '中证1000':
            if source_name=='jy':
                 return 'zz1000Monthly'
            else:
                return 'zz1000'
        elif index_type == '中证2000':
            if source_name == 'jy':
                return 'zz2000Monthly'
            else:
                return 'zz2000'
        else:
            if source_name == 'jy':
                return 'zzA500Monthly'
            else:
                return 'zzA500'

    def raw_jy_index_component_preparing(self,index_type):
        inputpath_component = glv.get('input_jy_indexcomponent')
        file_name = self.file_name_withdraw(index_type,'jy')
        inputpath_component_update = os.path.join(inputpath_component, file_name)
        inputpath_component_update = gt.file_withdraw(inputpath_component_update, self.available_date)
        df_daily = gt.readcsv(inputpath_component_update)
        if len(df_daily) != 0:
            df_daily.columns = ['code', 'weight', 'status']
            df_daily = df_daily[df_daily['status'] == 1]
            df_daily['weight'] = df_daily['weight'] / 100
        return df_daily
    def raw_wind_index_component_preparing(self,index_type):
        inputpath_component = glv.get('input_wind_indexcomponent')
        file_name = self.file_name_withdraw(index_type,'wind')
        inputpath_component_update = os.path.join(inputpath_component, file_name)
        inputpath_component_update = gt.file_withdraw(inputpath_component_update, self.available_date)
        df_daily = gt.readcsv(inputpath_component_update)
        if len(df_daily) != 0:
            df_daily.columns = ['date','code','weight']
            df_daily['status']=1
            df_daily=df_daily[['code', 'weight', 'status']]
            df_daily = df_daily[df_daily['status'] == 1]
            df_daily['weight'] = df_daily['weight'] / 100
        return df_daily
class stockData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date

    def raw_wind_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_wind_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_choice_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_choice_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_ifind_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_ifind_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_jy_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_jy_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        if len(df_stock) != 0:
            df_stock.columns = ['code', 'pre_close', 'open', 'high', 'low', 'close', 'volume', 'amount', 'return',
                                'log_return', 'vwap', 'adjfactor', 'trade_status']
            available_date2 = pd.to_datetime(self.available_date)
            available_date2 = available_date2.strftime('%Y-%m-%d')
            df_stock['valuation_date'] = available_date2
        return df_stock

    def stock_pool_withdraw(self,df):
        inputpath_stock_pool = glv.get('data_other')
        inputpath_stock_pool = os.path.join(inputpath_stock_pool, 'StockUniverse_new.csv')
        df_stock_pool = gt.readcsv(inputpath_stock_pool)
        code_list1 = df_stock_pool['S_INFO_WINDCODE'].tolist()
        code_list2 = df['code'].tolist()
        stock_pool = list(set(code_list1) | set(code_list2))
        stock_pool.sort()
        slice_df_stock_pool = pd.DataFrame()
        slice_df_stock_pool['code'] = stock_pool
        return slice_df_stock_pool

    # stock_close
    def stock_close_withdraw(self,df_stock, df_stockpool):
        available_date = gt.strdate_transfer(self.available_date)
        df_stock = df_stock[['code', 'close']]
        df_stock = df_stockpool.merge(df_stock, on='code', how='outer')
        df_stock.fillna(0, inplace=True)
        df_stock.set_index('code', inplace=True)
        df_stock = df_stock.T
        df_stock.reset_index(inplace=True, drop=True)
        df_stock['valuation_date'] = available_date
        df_stock = df_stock[['valuation_date'] + df_stock.columns.tolist()[:-1]]
        return df_stock

    # stock_return
    def stock_return_withdraw(self,df_stock, df_stockpool):
        available_date = gt.strdate_transfer(self.available_date)
        df_stock = df_stock[['code', 'return']]
        df_stock = df_stockpool.merge(df_stock, on='code', how='outer')
        df_stock.fillna(0, inplace=True)
        df_stock.set_index('code', inplace=True)
        df_stock = df_stock.T
        df_stock.reset_index(inplace=True, drop=True)
        df_stock['valuation_date'] = available_date
        df_stock = df_stock[['valuation_date'] + df_stock.columns.tolist()[:-1]]
        return df_stock
