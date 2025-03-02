import pandas as pd
import os
import global_setting.global_dic as glv
import warnings
warnings.filterwarnings("ignore")
import global_tools_func.global_tools as gt
from L4Data_update.tools_func import tools_func
from L4Data_update.L4Data_processing import L4Data_processing
class L4Holding_update:
    def __init__(self,product_code,available_date,df):
        self.product_code=product_code
        self.tf=tools_func()
        self.product_name=self.tf.product_NameCode_transfer(product_code)
        self.available_date=available_date
        self.df=df
        self.df=self.df.dropna(subset=['停牌信息'])
        self.lp=L4Data_processing(self.df,self.available_date,self.product_code)
    def outputpath_getting(self):
        if self.product_code=='SSS044':
            outputpath=glv.get('outputpath_RR500')
            file_name='_瑞锐500指增信息跟踪.xlsx'
        elif self.product_code=='SNY426':
            outputpath = glv.get('outputpath_RRJX')
            file_name='_瑞锐精选信息跟踪.xlsx'
        elif self.product_code=='SGS958':
            outputpath = glv.get('outputpath_XYHY_N01')
            file_name = '_惠盈一号指增信息跟踪.xlsx'
        elif self.product_code=='SZJ339':
            outputpath = glv.get('outputpath_SF500_N08')
            file_name='_盛元8号指增信息跟踪.xlsx'
        elif self.product_code=='SVU353':
            outputpath = glv.get('outputpath_GYZY_N01')
            file_name='_高益振英一号指增信息跟踪.xlsx'
        elif self.product_code=='SLA626':
            outputpath = glv.get('outputpath_RenRui_N01')
            file_name='_仁睿价值精选1号信息跟踪.xlsx'
        elif self.product_code=='STH580':
            outputpath = glv.get('outputpath_NJ300')
            file_name='_念觉300指增11号信息跟踪.xlsx'
        elif self.product_code=='SST132':
            outputpath = glv.get('outputpath_ZJ4')
            file_name='_念觉知行4号信息跟踪.xlsx'
        else:
            outputpath=None
            file_name=None
            print(str(self.product_code)+'不在脚本范围内')
        if outputpath==None or file_name==None:
            result_path_final=None
        else:
            gt.folder_creator2(outputpath)
            result_path_final = os.path.join(outputpath, str(self.available_date) + file_name)
        return result_path_final
    def L4Holding_processing(self):
        # 输入为 瑞锐中证500文件名
        # 输出为 解析后的四级估值表 分为股票 可转债 期货 期权 国债五个模块
        status='save'
        product_name=self.product_name
        date=self.available_date
        try:
            result=self.lp.process_stock_sheet()
        except:
            result=pd.DataFrame()
            status='not_save'
            print(str(date) + product_name + "产品股票sheet格式出现变动,请更正")
        try:
            result1 = self.lp.process_future_sheet()
        except:
            result1 = pd.DataFrame()
            status = 'not_save'
            print(str(date) + product_name + "产品期货sheet格式出现变动,请更正")
        try:
            result2 = self.lp.process_c_bond_sheet()
        except:
            result2 = pd.DataFrame()
            status = 'not_save'
            print(str(date) + product_name + "产品可转债sheet格式出现变动,请更正")
        try:
            result3 = self.lp.process_option_sheet()
        except:
            result3 = pd.DataFrame()
            status = 'not_save'
            print(str(date)+ product_name + "产品期权sheet格式出现变动,请更正")
        try:
            result4 = self.lp.process_bond_sheet()
        except:
            result4=pd.DataFrame()
            status = 'not_save'
            print(str(date)+ product_name + "产品国债sheet格式出现变动,请更正")
        result_path = self.outputpath_getting()
        if status=='not_save':
            print('更新错误')
        else:
            with pd.ExcelWriter(result_path, engine='openpyxl', mode='w') as writer:
                result.to_excel(writer, sheet_name='stock_tracking', index=False)
                result2.to_excel(writer, sheet_name='c_bond_tracking', index=False)
                result1.to_excel(writer, sheet_name='future_tracking', index=False)
                result3.to_excel(writer, sheet_name='option_tracking', index=False)
                result4.to_excel(writer, sheet_name='bond_tracking', index=False)




