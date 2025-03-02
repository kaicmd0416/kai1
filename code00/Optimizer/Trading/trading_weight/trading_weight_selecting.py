import pandas as pd
import os
import sys
from datetime import date
import datetime
from Trading.global_setting import global_dic as glv
import global_tools_func.global_tools as gt

def target_date_decision():
    if gt.is_workday2() == True:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        critical_time = '20:00'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            today = gt.strdate_transfer(today)
            return today
    else:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        return next_day

def weight_sum_check(df):
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99:
        df['weight'] = df['weight'] / weight_sum
    else:
        df = df
    return df

def product_names_withdraw():
    inputpath_trading = glv.get('product_detail')
    xls = pd.ExcelFile(inputpath_trading)
    index_list = xls.sheet_names
    product_names = index_list[1:]
    return product_names

def benchmark_target_weight_withdraw(index_type, yesterday):
    inputpath = glv.get('trading_weight')
    inputpath = os.path.join(inputpath, index_type)
    target_date = target_date_decision()
    last_date = gt.last_workday_calculate(target_date)
    target_date = gt.intdate_transfer(target_date)
    last_date = gt.intdate_transfer(last_date)
    if yesterday == False:
        inputpath_target = gt.file_withdraw(inputpath, target_date)
        df_target = gt.readcsv(inputpath_target)
        return df_target
    else:
        inputpath_yes = gt.file_withdraw(inputpath, last_date)
        df_yes = gt.readcsv(inputpath_yes)
        return df_yes
def product_target_weight_withdraw(product_name, yesterday):
    inputpath = glv.get('product_weight')
    inputpath = os.path.join(inputpath, product_name)
    target_date = target_date_decision()
    last_date = gt.last_workday_calculate(target_date)
    target_date = gt.intdate_transfer(target_date)
    last_date = gt.intdate_transfer(last_date)
    if yesterday == False:
        inputpath_target = gt.file_withdraw(inputpath, target_date)
        df_target = gt.readcsv(inputpath_target)
        return df_target
    else:
        inputpath_yes = gt.file_withdraw(inputpath, last_date)
        df_yes = gt.readcsv(inputpath_yes)
        return df_yes

def benchmark_trading_weight_combination(index_type):
    df_final = pd.DataFrame()
    inputpath_stockuniverse = glv.get('stock_universe')
    inputpath = glv.get('portfolio_weight')
    inputpath_trading = glv.get('trading_detail')
    df_stockuniverse = gt.readcsv(inputpath_stockuniverse)
    stock_universe = df_stockuniverse['code'].tolist()
    df_final['code'] = stock_universe
    df_allading = pd.read_excel(inputpath_trading, sheet_name=index_type)
    score_type_list = df_allading['score_name'].tolist()
    weight_list = df_allading['weight'].tolist()
    target_date = target_date_decision()
    target_date = gt.intdate_transfer(target_date)
    for i in range(len(score_type_list)):
        name = score_type_list[i]
        weight = weight_list[i]
        inputpath2 = os.path.join(inputpath, name)
        inputpath2 = gt.file_withdraw(inputpath2, target_date)
        df_weight = gt.readcsv(inputpath2)
        df_weight = weight_sum_check(df_weight)
        df_weight.columns = ['code', name]
        df_weight[name] = weight * df_weight[name]
        df_final = df_final.merge(df_weight, on='code', how='left')
    df_final.fillna(0, inplace=True)
    df_final.set_index('code', inplace=True, drop=True)
    df_final['weight'] = df_final.apply(lambda x: x.sum(), axis=1)
    df_final = df_final[df_final['weight'] != 0]
    df_final.reset_index(inplace=True)
    df_final = df_final[['code', 'weight']]
    return df_final, target_date
def benchmark_turn_over_ratio_check():
    outputpath = glv.get('output_turnover')
    outputpath=os.path.join(outputpath,'benchmark')
    gt.folder_creator2(outputpath)
    inputpath_trading = glv.get('trading_detail')
    xls = pd.ExcelFile(inputpath_trading)
    index_list = xls.sheet_names
    df_turn_over = pd.DataFrame()
    turn_over_list = []
    target_date = target_date_decision()
    target_date = gt.intdate_transfer(target_date)
    for index_type in index_list:
        df1 = benchmark_target_weight_withdraw(index_type, yesterday=False)
        df2 = benchmark_target_weight_withdraw(index_type, yesterday=True)
        code_list1 = df1['code'].tolist()
        code_list2 = df2['code'].tolist()
        code_list = list(set(code_list1) | set(code_list2))
        df_final = pd.DataFrame()
        df_final['code'] = code_list
        df_final = df_final.merge(df1, on='code', how='left')
        df_final = df_final.merge(df2, on='code', how='left')
        df_final.fillna(0, inplace=True)
        df_final['difference'] = df_final['weight_x'] - df_final['weight_y']
        turn_over = abs(df_final['difference']).sum()
        turn_over_list.append(turn_over)
    df_turn_over['index_type'] = index_list
    df_turn_over['turn_over'] = turn_over_list
    outputpath = os.path.join(outputpath, 'benchmark_turnover_' + str(target_date) + '.xlsx')
    df_turn_over.to_excel(outputpath, index=False)
def benchmark_trading_weight_saving():
    outputpath = glv.get('trading_weight')
    gt.folder_creator(outputpath)
    inputpath_trading = glv.get('trading_detail')
    xls = pd.ExcelFile(inputpath_trading)
    sheet_list = xls.sheet_names
    for index_type in sheet_list:
        df_final, target_date = benchmark_trading_weight_combination(index_type)
        outputpath2 = os.path.join(outputpath, index_type)
        gt.folder_creator(outputpath2)
        index_shortname = gt.index_shortname(index_type)
        outputpath2 = os.path.join(outputpath2, str(index_shortname) + '_weight_' + target_date + '.csv')
        df_final.to_csv(outputpath2, index=False)

def product_trading_weight_saving(start_date,end_date):
    outputpath=glv.get('product_weight')
    inputpath_stockuniverse = glv.get('stock_universe')
    inputpath = glv.get('portfolio_weight')
    inputpath_trading = glv.get('product_detail')
    df_stockuniverse = gt.readcsv(inputpath_stockuniverse)
    stock_universe = df_stockuniverse['code'].tolist()
    product_name_list=product_names_withdraw()
    for product_name in product_name_list:
        outputpath2=os.path.join(outputpath,product_name)
        gt.folder_creator2(outputpath2)
        try:
            input_list=os.listdir(outputpath2)
        except:
            input_list=[]
        if len(input_list)<2:
            start_date='2025-02-11'
        df_final = pd.DataFrame()
        df_final['code'] = stock_universe
        df_allading = pd.read_excel(inputpath_trading, sheet_name=product_name)
        score_type_list = df_allading['score_name'].tolist()
        weight_list = df_allading['weight'].tolist()
        working_days_list=gt.working_days_list(start_date,end_date)
        for target_date in working_days_list:
            target_date=gt.intdate_transfer(target_date)
            for i in range(len(score_type_list)):
                name = score_type_list[i]
                weight = weight_list[i]
                inputpath2 = os.path.join(inputpath, name)
                inputpath2 = gt.file_withdraw(inputpath2, target_date)
                df_weight = gt.readcsv(inputpath2)
                df_weight = weight_sum_check(df_weight)
                df_weight.columns = ['code', name]
                df_weight[name] = weight * df_weight[name]
                df_final = df_final.merge(df_weight, on='code', how='left')
            outputpath3 = os.path.join(outputpath2, product_name + '_weight_' + target_date + '.csv')
            df_final.fillna(0, inplace=True)
            df_final.set_index('code', inplace=True, drop=True)
            df_final['weight'] = df_final.apply(lambda x: x.sum(), axis=1)
            df_final = df_final[df_final['weight'] != 0]
            df_final.reset_index(inplace=True)
            df_final = df_final[['code', 'weight']]
            df_final.to_csv(outputpath3, index=False)


def product_turn_over_ratio_check():
    outputpath = glv.get('output_turnover')
    outputpath=os.path.join(outputpath,'product')
    gt.folder_creator2(outputpath)
    product_name_list = product_names_withdraw()
    df_turn_over = pd.DataFrame()
    turn_over_list = []
    target_date = target_date_decision()
    target_date = gt.intdate_transfer(target_date)
    for product_name in product_name_list:
        df1 = product_target_weight_withdraw(product_name, yesterday=False)
        df2 = product_target_weight_withdraw(product_name, yesterday=True)
        code_list1 = df1['code'].tolist()
        code_list2 = df2['code'].tolist()
        code_list = list(set(code_list1) | set(code_list2))
        df_final = pd.DataFrame()
        df_final['code'] = code_list
        df_final = df_final.merge(df1, on='code', how='left')
        df_final = df_final.merge(df2, on='code', how='left')
        df_final.fillna(0, inplace=True)
        df_final['difference'] = df_final['weight_x'] - df_final['weight_y']
        turn_over = abs(df_final['difference']).sum()
        turn_over_list.append(turn_over)
    df_turn_over['score_name'] = product_name_list
    df_turn_over['turn_over'] = turn_over_list
    outputpath = os.path.join(outputpath, 'product_turnover_' + str(target_date) + '.xlsx')
    df_turn_over.to_excel(outputpath, index=False)
def trading_weight_combination_main(): #触发这个
    target_date=target_date_decision()
    benchmark_trading_weight_saving()
    product_trading_weight_saving(target_date,target_date)
    try:
         benchmark_turn_over_ratio_check()
         product_turn_over_ratio_check()
    except:
        print('目前文件缺少前一天的trading_weight')

#trading_weight_combination_main()

