import Optimizer_Backtesting.global_setting.global_dic as glv
import pandas as pd
import global_tools_func.global_tools as gt
import os
def trading_portfolio_withdraw():
    inputpath_trading = glv.get('trading_config')
    xls = pd.ExcelFile(inputpath_trading)
    index_list = xls.sheet_names
    product_names = index_list[1:]
    final_list=[]
    for product_name in product_names:
        df=pd.read_excel(inputpath_trading,sheet_name=product_name)
        score_name_list=df['score_name'].tolist()
        final_list=list(set(final_list)|set(score_name_list))
    return final_list
def exposure_proportion_checking(df1):
    df=df1.copy()
    df['diff_1']=df['upper_constraint']-df['proportion']
    df['diff_2']=df['proportion']-df['lower_constraint']
    df['status']='Error'
    df.loc[(df['diff_1']>=0)&(df['diff_2']>=0),['status']]='Right'
    df1['status']=df['status'].tolist()
    return df1
def exposure_proportion_checking2(df1):
    df=df1.copy()
    df['diff_1']=df['TE_constraint']-df['TE']
    df['status']='Error'
    df.loc[(df['diff_1']>=0),['status']]='Right'
    df1['status2']=df['status'].tolist()
    return df1
def portfolio_checking(score_name,target_date):
    barra_header=['portfolio','index','proportion','TE','portfolio_score','weight_sum']
    industry_header=['portfolio','index','proportion']
    inputpath_portfolio = glv.get('portfolio_data')
    daily_inputpath = os.path.join(inputpath_portfolio, score_name)
    daily_inputpath = os.path.join(daily_inputpath, target_date)
    daily_parameter=os.path.join(daily_inputpath,'parameter_selecting.xlsx')
    daily_barra_risk=os.path.join(daily_inputpath,'barra_risk.csv')
    daily_industry_risk=os.path.join(daily_inputpath,'industry_risk.csv')
    daily_constraint_upper=os.path.join(daily_inputpath,'factor_constraint_upper.csv')
    daily_constraint_lower = os.path.join(daily_inputpath, 'factor_constraint_lower.csv')
    df_style=pd.read_csv(daily_barra_risk,header=None)
    df_industry=pd.read_csv(daily_industry_risk,header=None)
    df_style.columns=barra_header
    df_industry.columns=industry_header
    df_style_name=pd.read_excel(daily_parameter,sheet_name='style')
    df_industry_name = pd.read_excel(daily_parameter, sheet_name='industry')
    style_name=df_style_name['factor_name'].tolist()
    industry_name=df_industry_name['factor_name'].tolist()
    df_style['factor_name']=style_name
    df_industry['factor_name']=industry_name
    df_style.fillna(0,inplace=True)
    df_industry.fillna(0,inplace=True)
    df_upper=pd.read_csv(daily_constraint_upper)
    df_lower=pd.read_csv(daily_constraint_lower)
    df_upper=df_upper.T
    df_lower=df_lower.T
    df_upper.reset_index(inplace=True)
    df_lower.reset_index(inplace=True)
    df_upper.columns=['factor_name','upper_constraint']
    df_lower.columns=['factor_name','lower_constraint']
    TE=df_upper[df_upper['factor_name']=='TE']['upper_constraint'].tolist()[0]
    df_style_upper=df_upper[df_upper['factor_name'].isin(style_name)]
    df_style_lower=df_lower[df_lower['factor_name'].isin(style_name)]
    df_industry_upper = df_upper[df_upper['factor_name'].isin(industry_name)]
    df_industry_lower = df_lower[df_lower['factor_name'].isin(industry_name)]
    df_style=df_style.merge(df_style_upper,on='factor_name',how='left')
    df_style = df_style.merge(df_style_lower, on='factor_name', how='left')
    df_industry = df_industry.merge(df_industry_upper, on='factor_name', how='left')
    df_industry = df_industry.merge(df_industry_lower, on='factor_name', how='left')
    df_style['TE_constraint']=TE
    df_style=exposure_proportion_checking(df_style)
    df_industry=exposure_proportion_checking(df_industry)
    return df_style,df_industry
def portfolio_Error_raising(target_date):
    outputpath=glv.get('output_check2')
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,'PortfolioCheck_'+str(target_date2)+'.xlsx')
    inputpath_check=glv.get('output_check')
    inputpath_dic=glv.get('mode_dic')
    trading_list=trading_portfolio_withdraw()
    df_dic=pd.read_excel(inputpath_dic)
    portfolio_list=df_dic['score_name'].tolist()
    df_final=pd.DataFrame()
    for portfolio in portfolio_list:
        if portfolio in trading_list:
            is_trading='Trading'
        else:
            is_trading='None_Trading'
        daily_input=os.path.join(inputpath_check,portfolio)
        daily_style=os.path.join(daily_input,'style')
        daily_industry=os.path.join(daily_input,'industry')
        daily_style=gt.file_withdraw(daily_style,target_date2)
        daily_industry=gt.file_withdraw(daily_industry,target_date2)
        df1=gt.readcsv(daily_style)
        df2=gt.readcsv(daily_industry)
        df1=df1[(df1['status']=='Error')]
        df2 = df2[(df2['status'] == 'Error')]
        df1=df1[['factor_name','proportion','upper_constraint','lower_constraint']]
        df2=df2[['factor_name','proportion','upper_constraint','lower_constraint']]
        df_daily=pd.concat([df1,df2])
        df_daily['portfolio_name']=portfolio
        df_daily['is_trading']=is_trading
        if len(df_daily)>0:
            df_final=pd.concat([df_final,df_daily])
    try:
        df_trading = df_final[df_final['is_trading'] == 'Trading']
        df_trading.sort_values(by='factor_name', inplace=True)
    except:
        print('Opt结果的Trading都满足要求')
        df_trading = pd.DataFrame()
    try:
        df_non = df_final[df_final['is_trading'] != 'Trading']
        df_non.sort_values(by='factor_name', inplace=True)
    except:
        print('Opt结果的Non_Trading都满足要求')
        df_non=pd.DataFrame()

    with pd.ExcelWriter(outputpath) as writer:
        df_trading.to_excel(writer,sheet_name='Trading',index=False)
        df_non.to_excel(writer,sheet_name='None_Trading',index=False)


