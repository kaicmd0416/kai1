import os
import pandas as pd
import global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from MktData_update.Mktdata_preparing import indexReturn_prepare,stockData_preparing,indexComponent_prepare
class indexReturn_update:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_return')
        return df_config

    def index_return_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_index_return_base = glv.get('output_indexreturn')
        gt.folder_creator2(outputpath_index_return_base)
        input_list=os.listdir(outputpath_index_return_base)
        if len(input_list)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            available_date=gt.intdate_transfer(available_date)
            ir=indexReturn_prepare(available_date)
            outputpath_index_return = os.path.join(outputpath_index_return_base,
                                                   'indexReturn_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_index_return = pd.DataFrame()
            for source_name in source_name_list:
                if source_name == 'wind':
                    df_index_return = ir.raw_wind_index_return_withdraw()
                elif source_name == 'ifind':
                    df_index_return = ir.raw_ifind_index_return_withdraw()
                elif source_name == 'choice':
                    df_index_return = ir.raw_choice_index_return_withdraw()
                else:
                    df_index_return = ir.raw_jy_index_return_withdraw()
                try:
                    df_index_return.rename(columns={'000852.SH': '中证1000'}, inplace=True)
                    df_index_return.rename(columns={'932000.CSI': '中证2000'}, inplace=True)
                    df_index_return.rename(columns={'000905.SH': '中证500'}, inplace=True)
                    df_index_return.rename(columns={'000016.SH': '上证50'}, inplace=True)
                    df_index_return.rename(columns={'999004.SSI': '微盘股'}, inplace=True)
                    df_index_return.rename(columns={'000300.SH': '沪深300'}, inplace=True)
                    df_index_return.rename(columns={'000510.CSI': '中证A500'}, inplace=True)
                except:
                    pass
                if len(df_index_return) != 0:
                    print('index_return使用的数据源是:' + str(source_name))
                    break
            if len(df_index_return) != 0:
                df_index_return.to_csv(outputpath_index_return, index=False, encoding='gbk')
            else:
                print('index_return' + str(available_date) + '四个数据源都没有数据')

class indexComponent_update:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
    def file_name_withdraw(self,index_type):
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

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_component')
        return df_config

    def index_dic_processing(self):
        dic_index = {'上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
                     '中证2000': 'zz2000', '中证A500': 'zzA500'}
        return dic_index

    def index_component_update_main(self):
        df_config = self.source_priority_withdraw()
        df_config.sort_values(by='rank', inplace=True)
        source_name_list = df_config['source_name'].tolist()
        dic_index = self.index_dic_processing()
        outputpath_component = glv.get('output_indexcomponent')
        for index_type in ['上证50', '沪深300', '中证500', '中证1000', '中证2000', '中证A500']:
            file_name = self.file_name_withdraw(index_type)
            outputpath_component_update_base = os.path.join(outputpath_component, file_name)
            gt.folder_creator2(outputpath_component_update_base)
            input_list=os.listdir(outputpath_component_update_base)
            if len(input_list)==0:
                if self.start_date > '2023-06-01':
                    start_date = '2023-06-01'
                else:
                    start_date = self.start_date
            else:
                start_date=self.start_date
            working_days_list=gt.working_days_list(start_date,self.end_date)
            for available_date in working_days_list:
                available_date=gt.intdate_transfer(available_date)
                if index_type == '中证2000' and int(available_date) < 20230901:
                    available_date2 = '20230901'
                elif index_type == '中证A500' and int(available_date) < 20241008:
                    available_date2 = '20241008'
                else:
                    available_date2 = available_date
                df_daily = pd.DataFrame()
                index_code = dic_index[index_type]
                outputpath_component_update = os.path.join(outputpath_component_update_base,
                                                           index_code + 'ComponentWeight_' + available_date + '.csv')
                ic = indexComponent_prepare(available_date2)
                for source_name in source_name_list:
                    if source_name == 'jy':
                        df_daily =ic.raw_jy_index_component_preparing(index_type)
                    elif source_name == 'wind':
                        df_daily =ic.raw_wind_index_component_preparing(index_type)
                    elif source_name == 'ifind':
                        print('暂无数据')
                    else:
                        print('暂无数据')
                    if len(df_daily) != 0:
                        print('index_component使用的数据源是:' + str(source_name))
                        break
                if len(df_daily) != 0:
                    df_daily.to_csv(outputpath_component_update, index=False)
                else:
                    print('index_component在' + str(available_date) + '暂无数据')


class stockData_update:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='stock')
        return df_config

    def stock_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_stock_close_base = glv.get('output_stockclose')
        outputpath_stock_return_base = glv.get('output_stockreturn')
        gt.folder_creator2(outputpath_stock_close_base)
        gt.folder_creator2(outputpath_stock_return_base)
        input_list1=os.listdir(outputpath_stock_return_base)
        input_list2=os.listdir(outputpath_stock_close_base)
        if len(input_list1)==0 or len(input_list2)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            available_date=gt.intdate_transfer(available_date)
            st = stockData_preparing(available_date)
            outputpath_stock_close = os.path.join(outputpath_stock_close_base, 'stockClose_' + available_date + '.csv')
            outputpath_stock_return = os.path.join(outputpath_stock_return_base,
                                                   'stockReturn_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_stock_return = pd.DataFrame()
            df_stock_close = pd.DataFrame()
            for source_name in source_name_list:
                if source_name == 'wind':
                    df_stock = st.raw_wind_stockdata_withdraw()
                elif source_name == 'ifind':
                    df_stock = st.raw_ifind_stockdata_withdraw()
                elif source_name == 'choice':
                    df_stock = st.raw_choice_stockdata_withdraw()
                else:
                    df_stock = st.raw_jy_stockdata_withdraw()
                try:
                    df_stockpool = st.stock_pool_withdraw(df_stock)
                    df_stock_return = st.stock_return_withdraw(df_stock, df_stockpool)
                    df_stock_close = st.stock_close_withdraw(df_stock, df_stockpool)
                except:
                    pass
                if len(df_stock_return) != 0 and len(df_stock_close) != 0:
                    print('stock_return使用的数据源是:' + str(source_name))
                    print('stock_close使用的数据源是:' + str(source_name))
                    break
            if len(df_stock_return) != 0 and len(df_stock_close) != 0:
                df_stock_close.to_csv(outputpath_stock_close, index=False)
                df_stock_return.to_csv(outputpath_stock_return, index=False)
            else:
                print('stock_data' + str(available_date) + '四个数据源更新有问题')
def MktData_update_main(start_date,end_date):
    su=stockData_update(start_date,end_date)
    iu=indexReturn_update(start_date,end_date)
    icu=indexComponent_update(start_date,end_date)
    su.stock_data_update_main()
    iu.index_return_update_main()
    icu.index_component_update_main()