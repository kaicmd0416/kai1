import pandas as pd
import os
import global_setting.global_dic as glv
import warnings
warnings.filterwarnings("ignore")
import global_tools_func.global_tools as gt
from L4Data_update.tools_func import tools_func
class L4Prod_update:
    def __init__(self,product_code,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
        self.product_code=product_code
        self.tf = tools_func()
        self.product_name = self.tf.product_NameCode_transfer(product_code)
    def holding_diff(self):
        start_date=self.start_date
        end_date=self.end_date
        product_code=self.product_code
        # 输入为 对比日1， 对比日2， 产品代码
        # 输出为 持仓差异对比excel
        if "SSS044" in product_code:
            folder_path = glv.get('outputpath_RR500')
            result_path = glv.get('outputpath2_RR500')
        if "SNY426" in product_code:
            folder_path = glv.get('outputpath_RRJX')
            result_path = glv.get('outputpath2_RRJX')
        if "SGS958" in product_code:
            folder_path = glv.get('outputpath_XYHY_N01')
            result_path = glv.get('outputpath2_XYHY_N01')
        if "SZJ339" in product_code:
            folder_path = glv.get('outputpath_SF500_N08')
            result_path = glv.get('outputpath2_SF500_N08')
        if "SVX619" in product_code:
            folder_path = glv.get('outputpath_SF1000_N01')
            result_path = glv.get('outputpath2_SF1000_N01')
        if "SVU353" in product_code:
            folder_path = glv.get('outputpath_GYZY_N01')
            result_path = glv.get('outputpath2_GYZY_N01')
        if "SLA626" in product_code:
            folder_path = glv.get('outputpath_RenRui_N01')
            result_path = glv.get('outputpath2_RenRui_N01')
        if 'STH580' in product_code:
            folder_path = glv.get(name='inputpath_NJ300')
            result_path = glv.get('outputpath2_NJ300')
        if 'SST132' in product_code:
            folder_path = glv.get(name='inputpath_NJZX_N04')
            result_path = glv.get('outputpath2_ZJ4')
        gt.folder_creator2(result_path)
        product_folder = os.listdir(folder_path)
        target_file_yesterday = None
        target_file_today = None
        start_date = gt.intdate_transfer(start_date)
        end_date = gt.intdate_transfer(end_date)
        for i in product_folder:
            if start_date in i:
                target_file_yesterday = i
            if end_date in i:
                target_file_today = i
        if target_file_today == None or target_file_yesterday == None:
            print("产品" + product_code + "在start_date" + start_date + "和end_date" + end_date + "中存在数据缺失。")
        else:
            inputpath_product_holding = os.path.join(folder_path, target_file_today)
            inputpath_product_holding_yesterday = os.path.join(folder_path, target_file_yesterday)
            df_future_today = pd.read_excel(inputpath_product_holding, sheet_name='future_tracking')
            df_future_yesterday = pd.read_excel(inputpath_product_holding_yesterday, sheet_name='future_tracking')
            df_option_today = pd.read_excel(inputpath_product_holding, sheet_name='option_tracking')
            df_option_yesterday = pd.read_excel(inputpath_product_holding_yesterday, sheet_name='option_tracking')
            df_holding_today = pd.concat([df_future_today, df_option_today], ignore_index=True)
            df_holding_yesterday = pd.concat([df_future_yesterday, df_option_yesterday], ignore_index=True)
            df_holding_today.rename(columns={'数量': '今日数量'}, inplace=True)
            df_holding_yesterday.rename(columns={'数量': '昨日数量'}, inplace=True)
            df_holding_today['今日方向'] = df_holding_today['方向'].tolist()
            df_holding_yesterday['昨日方向'] = df_holding_yesterday['方向'].tolist()
            union = df_holding_today.merge(df_holding_yesterday, on='科目名称', how='outer')
            product_holding_name = union['科目名称'].tolist()
            today_num = union['今日数量'].fillna(0).astype(int)
            yes_num = union['昨日数量'].fillna(0).astype(int)
            direction_t = union['今日方向'].fillna('无持仓').tolist()
            direction_y = union['昨日方向'].fillna('无持仓').tolist()
            diff = abs(today_num - yes_num)
            result = pd.DataFrame()
            result.insert(0, '科目名称', product_holding_name)
            result.insert(1, '今日方向', direction_t)
            result.insert(2, '今日数量', today_num)
            result.insert(3, '昨日方向', direction_y)
            result.insert(4, '昨日数量', yes_num)
            result.insert(5, '差值', diff)
            action = []
            for index, row in result.iterrows():
                if row['今日方向'] == row['昨日方向']:
                    if row['今日数量'] > row['昨日数量']:
                        s = '加仓'
                        action.append(s)
                    if row['今日数量'] < row['昨日数量']:
                        s = '减仓'
                        action.append(s)
                    if row['今日数量'] == row['昨日数量']:
                        s = '无变化'
                        action.append(s)
                else:
                    if row['昨日数量'] == 0:
                        s = '开仓'
                        action.append(s)
                    if row['今日数量'] == 0:
                        s = '平仓'
                        action.append(s)
                    if row['今日方向'] == 'long' and row['昨日方向'] == 'short':
                        s = '换仓'
                        action.append(s)
                    if row['昨日方向'] == 'long' and row['今日方向'] == 'short':
                        s = '换仓'
                        action.append(s)
            result.insert(5, '操作', action)
            if start_date == gt.intdate_transfer(gt.last_workday_calculate(pd.to_datetime(end_date))):
                result_path_final = os.path.join(result_path,
                                                 start_date + '-' + end_date + '_' + product_code + '_' + '持仓差异跟踪.xlsx')
                with pd.ExcelWriter(result_path_final, engine='openpyxl') as writer:
                    result.to_excel(writer, sheet_name='product_tracking', index=False)
            else:
                result_path1 = glv.get('manual')
                result_path_final = os.path.join(result_path1,
                                                 start_date + '-' + end_date + '_' + product_code + '_' + '持仓差异跟踪.xlsx')
                with pd.ExcelWriter(result_path_final, engine='openpyxl') as writer:
                    result.to_excel(writer, sheet_name='product_tracking', index=False)
