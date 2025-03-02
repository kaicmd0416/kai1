import os
import pandas as pd
import sys
import yaml
import re
from datetime import date
import datetime
import global_tools_func.global_tools as gt
from Optimizer_python.main.optimizer_main_python import Optimizer_main
from Optimizer_python.data_prepare.data_prepare import stable_data_preparing
import Optimizer_python.global_setting.global_dic as glv

def score_name_withdraw():
    # 初始化两个空列表来存储所有score和opt_type
    all_scores = []
    all_opt_types = []

    # 循环处理每个时间点
    for i in ['time_1', 'time_2', 'time_3']:
        inputpath_mode_dic = glv.get('mode_dic')
        df_mode_dic = pd.read_excel(inputpath_mode_dic)
        df_mode_dic['score_type'] = df_mode_dic['score_name'].apply(lambda x: str(x)[:4])

        # 根据不同的时间点筛选数据
        if i == 'time_1':
            df_mode_dic = df_mode_dic[df_mode_dic['score_type'] == 'vp02']
        elif i == 'time_2':
            df_mode_dic = df_mode_dic[df_mode_dic['score_type'] == 'vp01']
        elif i == 'time_3':
            df_mode_dic = df_mode_dic[(df_mode_dic['score_type'] == 'fm01') | (df_mode_dic['score_type'] == 'fm03') | (df_mode_dic['score_type'] == 'comb')]
        else:
            df_mode_dic = pd.DataFrame()

        # 如果DataFrame为空，则跳过
        if len(df_mode_dic) == 0:
            continue

        # 获取当前时间点的score和opt_type列表
        score_list = df_mode_dic['score_name'].tolist()
        # opt_type_list = df_mode_dic['base_score'].tolist()

        # 将当前列表追加到总的列表中
        all_scores.extend(score_list)
        # all_opt_types.extend(opt_type_list)

    return all_scores

def target_date_decision():
    if gt.is_workday2() == True:
        today = date.today()
        today=gt.strdate_transfer(today)
        next_day = gt.next_workday_calculate(today)
        critical_time = '19:30'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            return today
    else:
        today = date.today()
        next_day=gt.next_workday_calculate(today)
        return next_day
def select_target_date_decision(date):
    if gt.is_workday(date) == True:
        today=gt.strdate_transfer(date)
        return today
    else:
        print('填写的日期不是交易日，请重新填写')
        return 0
def update_optimizer_main(): #部署自动化
    score_name_list=score_name_withdraw()
    if len(score_name_list)!=0:
        target_date = target_date_decision()
        print(target_date)
        stable_data = stable_data_preparing()
        df_st, df_stockuniverse=stable_data.stable_data_preparing()
        opm=Optimizer_main(df_st, df_stockuniverse)
        outputpath_list = opm.optimizer_update_main(target_date, score_name_list)
        return outputpath_list
    else:
        print('没有符合条件的score_name，请检查配置文件和score')

def get_valid_date(prompt):
    while True:
        date_input = input(prompt)
        if re.match(r'^\d{8}$', date_input):
            return date_input
        else:
            print("日期格式不正确，请输入yyyymmdd格式的日期。")

def update_optimizer_main_manual():
    while True:
        start_date = get_valid_date("请输入开始日期（yyyymmdd格式）: ")
        end_date = get_valid_date("请输入结束日期（yyyymmdd格式）: ")

        # 检查开始日期是否大于结束日期
        if start_date > end_date:
            print("错误：开始日期不能大于结束日期，请重新输入。")
        else:
            break  # 如果日期有效，退出循环

    # 设置默认值
    default_n = 3
    # 获取用户输入，如果用户直接按回车键，使用默认值
    n = input(f"请输入分块数量（默认为{default_n}，直接回车继续）: ") or default_n
    # 将输入转换为整数
    n = int(n)
    start_date = gt.strdate_transfer(start_date)
    end_date = gt.strdate_transfer(end_date)
    working_day_list = gt.working_days_list(start_date, end_date)
    score_name_list= score_name_withdraw()
    for i in working_day_list:
        if len(score_name_list) != 0:
            target_date = select_target_date_decision(i)
            if target_date != 0:
                stable_data = stable_data_preparing()
                df_st, df_stockuniverse = stable_data.stable_data_preparing()
                opm = Optimizer_main(df_st, df_stockuniverse)
                outputpath_list = opm.optimizer_update_main(target_date, score_name_list)
                return outputpath_list,n
            else:
                continue
        else:
            print('没有符合条件的score_name，请检查配置文件和score')
            break  # 如果没有符合条件的score_name，退出循环
def score_name_withdraw2():
    inputpath_mode_dic = glv.get('mode_dic')
    df_mode_dic = pd.read_excel(inputpath_mode_dic)
    df_mode_dic['score_type'] = df_mode_dic['score_name'].apply(lambda x: str(x)[:4])
    score_list = df_mode_dic['score_name'].tolist()
    return score_list
def update_optimizer_main2(): #部署自动化
    score_name_list=score_name_withdraw2()
    if len(score_name_list)!=0:
        target_date = target_date_decision()
        stable_data = stable_data_preparing()
        df_st, df_stockuniverse=stable_data.stable_data_preparing()
        opm=Optimizer_main(df_st, df_stockuniverse)
        outputpath_list = opm.optimizer_update_main(target_date, score_name_list)
        return outputpath_list

if __name__ == '__main__':
    d = update_optimizer_main()
    print(d)
    # update_optimizer_main_manual()


