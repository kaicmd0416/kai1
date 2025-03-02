import pandas as pd
import os
import sys
import global_setting.global_dic as glv
from Time_tools.time_tools import time_tools
import global_tools_func.global_tools as gt
class checking:
    def __init__(self,time_zoon):
        self.time_zoon=time_zoon
        self.tt=time_tools()
    def rrScore_updateChecking(self):
        inputpath_score_config = glv.get('score_mode')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['score_type2'] = df_config['score_type'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['score_type2'] == 'rr')]['score_mode'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, 'rr_' + str(mode_type))
            mode_name = df_config[df_config['score_mode'] == mode_type]['mode_name'].tolist()[0]
            outputpath3 = os.path.join(outputpath, mode_name)
            target_date = self.tt.target_date_decision_705()
            available_date = gt.last_workday_calculate(target_date)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = gt.file_withdraw(outputpath2, available_date)
            inputpath_file3 = gt.file_withdraw(outputpath3, available_date)
            if inputpath_file2 == None or inputpath_file3 == None:
                print('fm_score在最新时间:'+str(target_date)+'更新出现错误')
            else:
                print('fm_score已经更新到最新日期:'+str(target_date))
    def combineScore_updateChecking(self):
        inputpath_score_config = glv.get('mode_dic')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['base_score2'] = df_config['base_score'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['base_score2'] == 'co')]['base_score'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, mode_type)
            target_date = self.tt.target_date_decision_705()
            available_date = gt.last_workday_calculate(target_date)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = gt.file_withdraw(outputpath2, available_date)
            if inputpath_file2 == None:
                print('combine_score在最新时间:' + str(target_date) + '更新出现错误')
            else:
                print('combine_score已经更新到最新日期:' + str(target_date))
    def MktData_updateChecking(self):
        outputpath_stock_return = glv.get('output_stockreturn')
        outputpath_stock_close = glv.get('output_stockclose')
        outputpath_index_return = glv.get('output_indexreturn')
        outputpath_indexcomponent = glv.get('output_indexcomponent')
        target_date = self.tt.target_date_decision_1515()
        target_date = gt.intdate_transfer(target_date)
        check_1 = gt.file_withdraw(outputpath_stock_return, target_date)
        check_2 = gt.file_withdraw(outputpath_stock_close, target_date)

        check_4 = gt.file_withdraw(outputpath_index_return, target_date)
        if check_1 == None:
            print('stock_return在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('stock_return已经更新到最新日期:' + str(target_date))
        if check_2 == None:
            print('stock_close在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('stock_close已经更新到最新日期:' + str(target_date))
        for index_type in ['hs300','sz50','zz500','zzA500','zz1000','zz2000']:
            outputpath_index=os.path.join(outputpath_indexcomponent,index_type)
            check_3 = gt.file_withdraw(outputpath_index, target_date)
            if check_3 == None:
                print(str(index_type)+' index_component在最新时间:' + str(target_date) + '更新出现错误')
            else:
                print(str(index_type)+' index_component已经更新到最新日期:' + str(target_date))
        if check_4 == None:
            print('index_return在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('index_return已经更新到最新日期:' + str(target_date))
    def FactorData_updateChecking(self):
        outputpath_factor_exposure = glv.get('output_factor_exposure')
        outputpath_factor_return = glv.get('output_factor_return')
        outputpath_factor_stockpool = glv.get('output_factor_stockpool')
        output_indexexposure = glv.get('output_indexexposure')
        target_date = self.tt.target_date_decision_1800()
        target_date = gt.intdate_transfer(target_date)
        check_1 = gt.file_withdraw(outputpath_factor_exposure, target_date)
        check_2 = gt.file_withdraw(outputpath_factor_return, target_date)
        check_3 = gt.file_withdraw(outputpath_factor_stockpool, target_date)
        index_type_list = os.listdir(output_indexexposure)
        for index in index_type_list:
            output_indexexposure2 = os.path.join(output_indexexposure, index)
            check_4 = gt.file_withdraw(output_indexexposure2, target_date)
            if check_4 == None:
                print(str(index)+'_factorExposure在最新时间:' + str(target_date) + '更新出现错误')
            else:
                print(str(index)+'_factorExposure已经更新到最新日期:' + str(target_date))
        if check_1 == None:
            print('factor_return在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('factor_return已经更新到最新日期:' + str(target_date))
        if check_2 == None:
            print('factor_return在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('factor_return已经更新到最新日期:' + str(target_date))
        if check_3 == None:
            print('factor_stockpool在最新时间:' + str(target_date) + '更新出现错误')
        else:
            print('factor_stockpool已经更新到最新日期:' + str(target_date))
    def checking_main(self):
        if self.time_zoon=='time_1':
            self.rrScore_updateChecking()
            self.combineScore_updateChecking()
        elif self.time_zoon=='time_2':
            self.MktData_updateChecking()
        elif self.time_zoon=='time_3':
            self.FactorData_updateChecking()
        else:
            print('当前时间没有程序需要运行更新脚本')






