import os
import pandas as pd
from Trading.global_setting import global_dic as glv
import global_tools_func.global_tools as gt
def trading_order_xuanye_mode_1(df_today,df_yes,df_close,target_date,account_money,trading_time,product_name):
    df_final=pd.DataFrame()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    product_name2=str(product_name)[:2]
    inputpath=os.path.join(inputpath,'模板')
    inputpath=os.path.join(inputpath,product_name2)
    inputpath = os.path.join(inputpath, 'trading_list模板.csv')
    outputpath=glv.get('output_trading_order')
    outputpath = os.path.join(outputpath, 'trading_order')
    outputpath= os.path.join(outputpath, product_name)
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,str(product_name)+'_'+target_date2+'_trading_list.csv')
    df = gt.readcsv(inputpath)
    df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN1', 'open_action', 'UN2', '涨停', 'UN3', 'UN4']
    df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴TWAP-'+str(target_date)
    df[df.columns.tolist()[2]].iloc[3] = trading_time
    code_list_today = df_today['code'].tolist()
    df_today=df_today.merge(df_close,on='code',how='left')
    df_today['quantity']=account_money*df_today['weight']/df_today['close']
    df_today['quantity']=round(df_today['quantity']/100,0)*100
    df_today=df_today[['code','quantity']]
    df_yes.rename(columns={'StockCode': 'code'}, inplace=True)
    df_weight = pd.DataFrame()
    code_list_yes = df_yes['code'].tolist()
    code_list = list(set(code_list_yes) | set(code_list_today))
    df_weight['code'] = code_list
    df_weight = df_weight.merge(df_yes, on='code', how='left')
    df_weight = df_weight.merge(df_today, on='code', how='left')
    df_weight.fillna(0,inplace=True)
    df_weight['difference']=df_weight['quantity']-df_weight['HoldingQty']
    list_difference=df_weight['difference'].tolist()
    list_action=[]
    for i in list_difference:
        if i>0:
            list_action.append('买入')
        elif i<0:
            list_action.append('卖出')
        else:
            list_action.append('不动')
    df_weight['action']=list_action
    df_weight['difference']=abs(df_weight['difference'])
    df_weight=df_weight[df_weight['action']!='不动']
    df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
    df_weight.loc[
        (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
            'difference']] = 200
    code_list_today=df_weight['code'].apply(lambda x: str(x)[:-3]).tolist()
    quantity_list=df_weight['difference'].tolist()
    action_list=df_weight['action'].tolist()
    for i in range(len(df.columns.tolist())):
        columns_name=df.columns.tolist()[i]
        initial_list=df[columns_name].tolist()
        if i==1:
            final_list=initial_list+code_list_today
        elif i==2:
            final_list=initial_list+quantity_list
        elif i==3:
            final_list=initial_list+action_list
        elif i==5:
            final_list=initial_list+[0]*len(df_weight)
        elif i==7:
            final_list=initial_list+['FALSE']*len(df_weight)
        else:
            final_list=initial_list+[None]*len(df_weight)
        df_final[columns_name]=final_list
    df_final.columns=df.columns.tolist()[:2]+[None]*8
    df_final.to_csv(outputpath,index=False,encoding='utf_8_sig')
    return df_final,df_today
def trading_order_xuanye_mode_2(df_today,df_yes,df_close,target_date,account_money,trading_time,product_name):
    df_final=pd.DataFrame()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    product_name2=str(product_name)[:2]
    inputpath=os.path.join(inputpath,'模板')
    inputpath=os.path.join(inputpath,product_name2)
    inputpath = os.path.join(inputpath, 'trading_list_v2模板.csv')
    outputpath = glv.get('output_trading_order')
    outputpath = os.path.join(outputpath, product_name)
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,str(product_name)+'_'+target_date2+'_trading_list.csv')
    df = gt.readcsv(inputpath)
    df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN2', '涨停', 'UN3', 'UN4']
    target_date2=str(target_date)[:4]+str(target_date)[5:7]+str(target_date)[8:10]
    df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴VWAP-'+str(target_date2)
    df[df.columns.tolist()[2]].iloc[3] = trading_time
    code_list_today = df_today['code'].tolist()
    df_today=df_today.merge(df_close,on='code',how='left')
    df_today['quantity']=account_money*df_today['weight']/df_today['close']
    df_today['quantity']=round(df_today['quantity']/100,0)*100
    df_today=df_today[['code','quantity']]
    df_yes.rename(columns={'StockCode': 'code'}, inplace=True)
    df_weight = pd.DataFrame()
    code_list_yes = df_yes['code'].tolist()
    code_list = list(set(code_list_yes) | set(code_list_today))
    df_weight['code'] = code_list
    df_weight = df_weight.merge(df_yes, on='code', how='left')
    df_weight = df_weight.merge(df_today, on='code', how='left')
    df_weight.fillna(0,inplace=True)
    df_weight['difference']=df_weight['quantity']-df_weight['HoldingQty']
    list_difference=df_weight['difference'].tolist()
    list_action=[]
    for i in list_difference:
        if i>0:
            list_action.append('买入')
        elif i<0:
            list_action.append('卖出')
        else:
            list_action.append('不动')
    df_weight['action']=list_action
    df_weight['difference']=abs(df_weight['difference'])
    df_weight=df_weight[df_weight['action']!='不动']
    df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
    df_weight.loc[
        (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
            'difference']] = 200
    code_list_today=df_weight['code'].apply(lambda x: str(x)[:-3]).tolist()
    quantity_list=df_weight['difference'].tolist()
    action_list=df_weight['action'].tolist()
    for i in range(len(df.columns.tolist())):
        columns_name=df.columns.tolist()[i]
        initial_list=df[columns_name].tolist()
        if i==1:
            final_list=initial_list+code_list_today
        elif i==2:
            final_list=initial_list+quantity_list
        elif i==3:
            final_list=initial_list+action_list
        elif i==5:
            final_list=initial_list+['FALSE']*len(df_weight)
        else:
            final_list=initial_list+[None]*len(df_weight)
        df_final[columns_name]=final_list
    df_final.columns=df.columns.tolist()[:2]+[None]*6
    df_final.to_csv(outputpath,index=False,encoding='utf_8_sig')
    return df_final,df_today
def writing_t0_list_mode_1(df_weight,target_date,T0_money,end_time,product_name):
    outputpath=glv.get('output_t0_list')
    outputpath=os.path.join(outputpath,product_name)
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,'t0_list_'+target_date2+'.csv')
    df_final=pd.DataFrame()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    product_name2 = str(product_name)[:2]
    inputpath = os.path.join(inputpath, '模板')
    inputpath = os.path.join(inputpath, product_name2)
    inputpath = os.path.join(inputpath, 'T0_模板.csv')
    df = gt.readcsv(inputpath)
    df.columns = df.columns.tolist()[:2] + ['quantity']
    df[df.columns.tolist()[1]].iloc[1] = '恒泰AI日内1号-' + str(target_date)
    df[df.columns.tolist()[1]].iloc[2] = target_date
    df[df.columns.tolist()[2]].iloc[4] = T0_money
    df[df.columns.tolist()[2]].iloc[5] = end_time
    code_list_today=df_weight['stock_code'].apply(lambda x: str(x)[:-3]).tolist()
    quantity_list=df_weight['amount'].tolist()
    for i in range(len(df.columns.tolist())):
        columns_name=df.columns.tolist()[i]
        initial_list=df[columns_name].tolist()
        if i==1:
            final_list=initial_list+code_list_today
        elif i==2:
            final_list=initial_list+quantity_list
        else:
            final_list=initial_list+[None]*len(df_weight)
        df_final[columns_name] = final_list
    df_final.columns = df.columns.tolist()[:2] + [None]
    df_final.to_csv(outputpath, index=False, encoding='utf_8_sig')
    return df_final
def writing_t0_list_mode_2(df_weight,target_date,T0_money,product_name):
    outputpath=glv.get('output_t0_list')
    outputpath=os.path.join(outputpath,product_name)
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,'t0_list_'+target_date2+'.csv')
    df_final=pd.DataFrame()
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    product_name2 = str(product_name)[:2]
    inputpath = os.path.join(inputpath, '模板')
    inputpath = os.path.join(inputpath, product_name2)
    inputpath = os.path.join(inputpath, 'T0_模板_v2.csv')
    df = pd.read_csv(inputpath)
    df.columns = df.columns.tolist()[:2] + ['quantity']
    target_date2=str(target_date)[:4]+str(target_date)[5:7]+str(target_date)[8:10]
    df[df.columns.tolist()[1]].iloc[1] = '恒泰AI日内2号-' + str(target_date2)
    df[df.columns.tolist()[1]].iloc[2] = target_date
    df[df.columns.tolist()[2]].iloc[5] = T0_money
    code_list_today=df_weight['stock_code'].apply(lambda x: str(x)[:-3]).tolist()
    quantity_list=df_weight['amount'].tolist()
    for i in range(len(df.columns.tolist())):
        columns_name=df.columns.tolist()[i]
        initial_list=df[columns_name].tolist()
        if i==1:
            final_list=initial_list+code_list_today
        elif i==2:
            final_list=initial_list+quantity_list
        else:
            final_list=initial_list+[None]*len(df_weight)
        df_final[columns_name] = final_list
    df_final.columns = df.columns.tolist()[:2] + [None]
    df_final.to_csv(outputpath, index=False, encoding='utf_8_sig')
    return df_final
def t0_trading_xy_main(df1,df2,target_date,T0_money, end_time,product_name,t0_mode):#df1是昨日持仓，df2是今日持仓
    df1.columns = ['stock_code', 'quantity_yesterday']
    df2.columns = ['stock_code', 'quantity_today']
    result = pd.merge(df1, df2, on='stock_code', how='outer')
    result.reset_index(inplace=True, drop=True)
    result = result.fillna(0)
    stock_list = result['stock_code'].tolist()
    x = pd.DataFrame(result['stock_code'])
    x['amount'] = 0
    for i in stock_list:
        index = result[result['stock_code'] == i].index.tolist()[0]
        if result.loc[index, 'quantity_yesterday'] - result.loc[index, 'quantity_today'] == 0:
            if result.loc[index, 'quantity_yesterday'] == 0:
                x.drop(index=index, inplace=True)
            else:
                x.loc[index, 'amount'] += result.loc[index, 'quantity_yesterday']
        elif result.loc[index, 'quantity_yesterday'] - result.loc[index, 'quantity_today'] < 0:
            if result.loc[index, 'quantity_yesterday'] == 0:
                x.drop(index=index, inplace=True)
            else:
                x.loc[index, 'amount'] += result.loc[index, 'quantity_yesterday']
        elif result.loc[index, 'quantity_yesterday'] - result.loc[index, 'quantity_today'] > 0:
            if result.loc[index, 'quantity_today'] == 0:
                x.drop(index=index, inplace=True)
            else:
                x.loc[index, 'amount'] += result.loc[index, 'quantity_today']
    if t0_mode=='mode_v1':
           writing_t0_list_mode_1(x, target_date, T0_money, end_time, product_name)
    else:
           writing_t0_list_mode_2(x,target_date,T0_money,product_name)

def trading_order_xy_main(df_today, df_yes, df_close, target_time,stock_money,trading_time,product_name,trading_mode):
    if trading_mode=='mode_v1':
        df_final,df_today= trading_order_xuanye_mode_1(df_today, df_yes, df_close, target_time, stock_money, trading_time,
                                                    product_name)
    else:
        df_final,df_today= trading_order_xuanye_mode_2(df_today, df_yes, df_close, target_time, stock_money,trading_time,
                                                         product_name)
    return df_today
