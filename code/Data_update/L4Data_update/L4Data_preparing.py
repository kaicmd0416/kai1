import pandas as pd
import os
import global_setting.global_dic as glv
import warnings
warnings.filterwarnings("ignore")
import global_tools_func.global_tools as gt
from L4Data_update.tools_func import tools_func
class L4Data_preparing:
    def __init__(self,product_code,available_date):
        self.product_code = product_code
        self.tf = tools_func()
        self.product_name = self.tf.product_NameCode_transfer(product_code)
        self.available_date=available_date
    def extract(self):
        product_code=self.product_code
        time = self.available_date
        inputpath = glv.get('folder_path')
        filelist = os.listdir(inputpath)
        # 文件名检索 日期为特征值 定向提取单一文件 但输出为list
        time = gt.intdate_transfer(time)
        list1 = []
        list2 = []
        for i in filelist:
            if product_code == 'SVX619':
                time = gt.strdate_transfer(time)
            if product_code == 'SLA626':
                time = gt.strdate_transfer(time)
            if product_code in i:
                list1.append(i)
        for j in list1:
            if time in j:
                list2.append(j)
        if len(list1) == 0 or len(list2) == 0:
            print( "存在数据缺失")
        else:
            list2=[i for i in list2 if '.xlsx' in i or '.xls' in i]
        if len(list2) > 0:
            string1 = min(list2, key=len)
            list2 = [string1]
        if len(list2)>1:
            print('请检查估值表重名文件')
        return list2[0]
    def raw_L4_adit(self,df):
        if self.product_code=='SGS958' or self.product_code=='SVU353':
            df.rename(columns={'行情':'市价'},inplace=True)
            df.rename(columns={'市值-本币': '市值'}, inplace=True)
            df.rename(columns={'成本-本币': '成本'}, inplace=True)
            df.rename(columns={'成本占比': '成本占净值%'}, inplace=True)
            df.rename(columns={'市值占比': '市值占净值%'}, inplace=True)
            df.rename(columns={'估值增值-本币': '估值增值'}, inplace=True)
        elif self.product_code=='SST132':
            df.rename(columns={'行情': '市价'}, inplace=True)
            df.rename(columns={'成本占比': '成本占净值%'}, inplace=True)
            df.rename(columns={'市值占比': '市值占净值%'}, inplace=True)
        else:
            df=df
        return df
    def raw_L4_withdraw(self):
        folder_path = glv.get('folder_path')
        current_file=self.extract()
        a = os.path.join(folder_path, current_file)
        a = a.replace('\\', '\\\\')  # 路径拼接
        df = pd.read_excel(a, header=None)
        columns1,columns2=self.tf.product_loc_withdraw(self.product_code)
        new_columns = df.iloc[columns1]
        if self.product_code=='SST132':
            new_columns[7] = '成本'
            new_columns[11] = '市值'
            new_columns[14] = '估值增值'
        df = df.iloc[columns2:]
        df.columns = new_columns  # sheet1 stock_tracking
        df.set_index('科目代码', inplace=True)
        df=self.raw_L4_adit(df)
        return df

