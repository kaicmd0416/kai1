import pandas as pd
import os
import global_setting.global_dic as glv
import warnings
warnings.filterwarnings("ignore")
import global_tools_func.global_tools as gt
from L4Data_update.tools_func import tools_func
from L4Data_update.L4Data_processing import L4Data_processing
class L4Info_update:
    def __init__(self,product_code,available_date,df):
        self.product_code=product_code
        self.tf=tools_func()
        self.product_name=self.tf.product_NameCode_transfer(product_code)
        self.available_date=available_date
        self.df=df
        self.df=self.df.reset_index()
        self.df=self.df.fillna(0)
        self.lp = L4Data_processing(self.df, self.available_date, self.product_code)
    def outputpath_getting(self):
        if self.product_code=='SSS044':
            outputpath=glv.get('outputpath1_RR500')
            file_name='_瑞锐500指增信息跟踪.xlsx'
        elif self.product_code=='SNY426':
            outputpath = glv.get('outputpath1_RRJX')
            file_name='_瑞锐精选信息跟踪.xlsx'
        elif self.product_code=='SGS958':
            outputpath = glv.get('outputpath1_XYHY_N01')
            file_name = '_惠盈一号指增信息跟踪.xlsx'
        elif self.product_code=='SZJ339':
            outputpath = glv.get('outputpath1_SF500_N08')
            file_name='_盛元8号指增信息跟踪.xlsx'
        elif self.product_code=='SVU353':
            outputpath = glv.get('outputpath1_GYZY_N01')
            file_name='_高益振英一号指增信息跟踪.xlsx'
        elif self.product_code=='SLA626':
            outputpath = glv.get('outputpath1_RenRui_N01')
            file_name='_仁睿价值精选1号信息跟踪.xlsx'
        elif self.product_code=='STH580':
            outputpath = glv.get('outputpath1_NJ300')
            file_name='_念觉300指增11号信息跟踪.xlsx'
        elif self.product_code=='SST132':
            outputpath = glv.get('outputpath1_ZJ4')
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

    def L4Info_processing(self):
        status = 'save'
        product_name = self.product_name
        date = self.available_date
        try:
            result = self.lp.process_info_sheet()
        except:
            result = pd.DataFrame()
            status = 'not_save'
            print(str(date) + product_name + "产品信息格式出现变动,请更正")
        result_path = self.outputpath_getting()
        if status == 'not_save':
            print('更新错误')
        else:
            with pd.ExcelWriter(result_path, engine='openpyxl') as writer:
                result.to_excel(writer, sheet_name='info_tracking', index=False)

