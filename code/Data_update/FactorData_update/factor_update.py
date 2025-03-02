import os
import pandas as pd
import global_setting.global_dic as glv
# import stock.stock_data_preparing as st
from FactorData_update.factor_preparing import FactorData_prepare
import global_tools_func.global_tools as gt
class FactorData_update:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='factor')
        return df_config

    def index_dic_processing(self):
        dic_index = {'上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
                     '中证2000': 'zz2000', '中证A500': 'zzA500'}
        return dic_index

    def factor_update_main(self):
        outputpath_factor_exposure_base = glv.get('output_factor_exposure')
        outputpath_factor_return_base = glv.get('output_factor_return')
        outputpath_factor_stockpool_base = glv.get('output_factor_stockpool')
        outputpath_factor_cov_base = glv.get('output_factor_cov')
        outputpath_factor_risk_base = glv.get('output_factor_specific_risk')
        gt.folder_creator2(outputpath_factor_exposure_base)
        gt.folder_creator2(outputpath_factor_return_base)
        gt.folder_creator2(outputpath_factor_stockpool_base)
        gt.folder_creator2(outputpath_factor_cov_base)
        gt.folder_creator2(outputpath_factor_risk_base)
        input_list1=os.listdir(outputpath_factor_exposure_base)
        input_list2 = os.listdir(outputpath_factor_return_base)
        input_list3 = os.listdir(outputpath_factor_stockpool_base)
        input_list4 = os.listdir(outputpath_factor_cov_base)
        input_list5 = os.listdir(outputpath_factor_risk_base)
        if len(input_list1)==0 or len(input_list2)==0 or len(input_list3)==0 or len(input_list4)==0 or len(input_list5)==0:
            if self.start_date>'2023-06-01':
                start_date='2023-06-01'
            else:
               start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            available_date=gt.intdate_transfer(available_date)
            outputpath_factor_exposure = os.path.join(outputpath_factor_exposure_base,
                                                      'factorExposure_' + available_date + '.csv')
            outputpath_factor_return = os.path.join(outputpath_factor_return_base, 'factorReturn_' + available_date + '.csv')
            outputpath_factor_stockpool = os.path.join(outputpath_factor_stockpool_base,
                                                       'factorStockPool_' + available_date + '.csv')
            outputpath_factor_cov = os.path.join(outputpath_factor_cov_base, 'factorCov_' + available_date + '.csv')
            outputpath_factor_risk = os.path.join(outputpath_factor_risk_base,
                                                  'factorSpecificRisk_' + available_date + '.csv')
            df_config = self.source_priority_withdraw()
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            fc = FactorData_prepare(available_date)
            for source_name in source_name_list:
                if source_name == 'jy':
                    df_factorexposure = fc.jy_factor_exposure_update()
                    df_factorreturn = fc.jy_factor_return_update()
                    df_stockpool = fc.jy_factor_stockpool_update()
                    df_factorcov = fc.factor_jy_covariance_update()
                    df_factorrisk = fc.factor_jy_SpecificRisk_update()
                elif source_name == 'wind':
                    df_factorexposure = fc.wind_factor_exposure_update()
                    df_factorreturn = fc.wind_factor_return_update()
                    df_stockpool = fc.wind_factor_stockpool_update()
                    df_factorcov = fc.factor_wind_covariance_update()
                    df_factorrisk = fc.factor_wind_SpecificRisk_update()
                else:
                    raise ValueError
                if len(df_factorexposure) != 0 and len(df_factorreturn) != 0 and len(df_stockpool) != 0 and len(
                        df_factorcov) != 0 and len(df_factorrisk) != 0:
                    print('factor使用的数据源是:' + str(source_name))
                    break
            if len(df_factorexposure) != 0 and len(df_factorreturn) != 0 and len(df_stockpool) != 0 and len(
                    df_factorcov) != 0 and len(df_factorrisk) != 0:
                df_factorexposure.to_csv(outputpath_factor_exposure, index=False, encoding='gbk')
                df_factorreturn.to_csv(outputpath_factor_return, index=False, encoding='gbk')
                df_stockpool.to_csv(outputpath_factor_stockpool, index=False, encoding='gbk')
                df_factorcov.to_csv(outputpath_factor_cov, index=False, encoding='gbk')
                df_factorrisk.to_csv(outputpath_factor_risk, index=False, encoding='gbk')
            else:
                print('factor_data在' + str(available_date) + '数据存在缺失')

    def index_factor_update_main(self):
        dic_index = self.index_dic_processing()
        outputpath_factor_index = glv.get('output_indexexposure')
        for index_type in ['上证50', '沪深300', '中证500', '中证1000', '中证2000', '中证A500']:
            index_short = dic_index[index_type]
            outputpath_factor_index1_base = os.path.join(outputpath_factor_index, index_short)
            gt.folder_creator2(outputpath_factor_index1_base)
            input_list=os.listdir(outputpath_factor_index1_base)
            if len(input_list)==0:
                if self.start_date > '2023-06-01':
                    start_date = '2023-06-01'
                else:
                    start_date = self.start_date
            else:
                    start_date=self.start_date
            df_config = self.source_priority_withdraw()
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            working_days_list=gt.working_days_list(start_date,self.end_date)
            for available_date in working_days_list:
                available_date=gt.intdate_transfer(available_date)
                fc=FactorData_prepare(available_date)
                for source_name in source_name_list:
                    outputpath_factor_index1 = os.path.join(outputpath_factor_index1_base,
                                                            str(index_short) + 'IndexExposure_' + available_date + '.csv')
                    if source_name == 'jy':
                        df_index_exposure = fc.jy_factor_index_exposure_update(index_type)
                    elif source_name == 'wind':
                        df_index_exposure = fc.wind_factor_index_exposure_update(index_type)
                    else:
                        raise ValueError
                    if len(df_index_exposure) != 0:
                        print(str(index_type) + 'factor_exposure使用的数据源是:' + str(source_name))
                        break
                if len(df_index_exposure) != 0:
                    df_index_exposure.to_csv(outputpath_factor_index1, index=False, encoding='gbk')
                else:
                    print('index_factor在' + str(available_date) + '数据存在缺失')
    def FactorData_update_main(self):
        self.factor_update_main()
        self.index_factor_update_main()
def FactorData_history_main(start_date,end_date):
    fu=FactorData_update(start_date,end_date)
    fu.factor_update_main()
    fu.index_factor_update_main()


