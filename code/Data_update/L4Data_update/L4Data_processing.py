import pandas as pd
from L4Data_update.tools_func import tools_func
class L4Data_processing:
    def __init__(self,df,date,product_code):
        self.product_code=product_code
        self.df=df
        self.date=date
        self.tf = tools_func()
        self.product_name = self.tf.product_NameCode_transfer(product_code)
        self.tf = tools_func()
    def create_output_df(self,data_dict):
        """通过字典直接构建输出DataFrame，避免多次insert"""
        return pd.DataFrame(data_dict)
    def process_stock_sheet(self):
        """处理股票相关Sheet（Sheet1）"""
        df=self.df.copy()
        date=self.date
        df = df[df['科目名称'].str.len() <= 5]  # 根据需求调整条件
        df = df[~df['科目名称'].str.contains('T2')]
        df1 = df[df['科目名称'].str.contains('转')]  # df抓取所有转债
        df = df[~df.index.isin(df1.index)]
        if self.product_code=='SGS958' or self.product_code=='SVU353' or self.product_code=='SST132':
            code_list = df.index.str[-9:-3].tolist()
        else:
            code_list = df.index.str[-6:].tolist()
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': code_list,
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist()
        }
        df=self.create_output_df(dic)
        return df

    def process_future_sheet(self):
        """处理期货Sheet（Sheet2）"""
        df=self.df.copy()
        df=df.reset_index()
        date=self.date
        df = df[df['科目名称'].str.len() >= 6]
        df = df.drop(df[df['科目名称'].str.contains('国债')].index)
        df1 = df[df['科目名称'].str.contains('期货')]
        df2 = df[df['科目名称'].str.contains('I')]
        future_df = pd.concat([df1, df2], axis=0)
        code_list = future_df['科目代码'].tolist()
        code_list2=[]
        for i in range(len(code_list)):
            current_element = code_list[i]
            if self.product_code=='SST132':
                 scode=current_element
            elif self.product_code=='SVU353' or self.product_code=='SGS958':
                scode = current_element[-10:-4]
            else:
                 scode = current_element[-6:]
            code_list2.append(scode)
        type2 = []
        direction = []
        for k in future_df['市值'].tolist():
            if k > 0:
                b = 'long'
                direction.append(b)
                type2.append('中金所_多头')
            if k < 0:
                b = 'short'
                direction.append(b)
                type2.append('中金所_空头')
        dic={
            '日期': [date] * len(future_df),
            '产品名称': [self.product_code] * len(future_df),
            '种类': ['衍生工具'] * len(future_df),
            '种类名称': type2,
            '代码': future_df['科目代码'].tolist(),
            '方向': direction,
            '科目名称': code_list2,
            '数量': future_df['数量'].tolist(),
            '单位成本': future_df['单位成本'].tolist(),
            '成本': future_df['成本'].tolist(),
            '成本占净值%': future_df['成本占净值%'].tolist(),
            '市价': future_df['市价'].tolist(),
            '市值': future_df['市值'].tolist(),
            '市值占净值%': future_df['市值占净值%'].tolist(),
            '估值增值': future_df['估值增值'].tolist(),
            '停牌信息': future_df['停牌信息'].tolist()
        }
        df = self.create_output_df(dic)
        return df

    def process_c_bond_sheet(self):
        """处理可转债Sheet（Sheet3）"""
        df = self.df.copy()
        date = self.date
        df = df[df['科目名称'].str.len() <= 5]
        df = df[df['科目名称'].str.contains('转')]
        if self.product_code=='SGS958' or self.product_code=='SVU353' or self.product_code=='SST132':
            code_list = df.index.str[-9:-3].tolist()
        else:
            code_list = df.index.str[-6:].tolist()
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': code_list,
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist()
        }
        df = self.create_output_df(dic)
        return df

    def process_option_sheet(self):
        """处理期权Sheet（Sheet4）"""
        df = self.df.copy()
        df = df.reset_index()
        date = self.date
        product_name = self.product_name
        df = df[df['科目名称'].str.len() >= 6]
        df = df[df['科目名称'].str.contains('-')]
        if self.product_code=='STH580':
            df.loc[:, '科目名称'] = df.loc[:, '科目名称'].apply(self.tf.option_name_transfer_NJ300)
        else:
             df.loc[:, '科目名称'] = df.loc[:, '科目名称'].apply(self.tf.option_name_transfer)
        option_value = df['市值'].tolist()
        direction = []
        for j in option_value:
            if j > 0:
                z = 'long'
                direction.append(z)
            if j < 0:
                z = 'short'
                direction.append(z)
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '种类': ['衍生工具'] * len(df),
            '代码': df['科目代码'].tolist(),
            '科目名称': df['科目名称'].tolist(),
            '方向': direction,
            '数量': df['数量'].tolist(),
            '单位成本': df['单位成本'].tolist(),
            '成本': df['成本'].tolist(),
            '成本占净值%': df['成本占净值%'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist(),
            '市值占净值%': df['市值占净值%'].tolist(),
            '估值增值': df['估值增值'].tolist(),
            '停牌信息': df['停牌信息'].tolist(),
        }
        df = self.create_output_df(dic)
        return df

    def process_bond_sheet(self):
        """处理可转债Sheet（Sheet5）"""
        df = self.df.copy()
        date = self.date
        if self.product_code=='SSS044' or self.product_code=='SNY426' or self.product_code=='SLA626' or self.product_code=='SZJ339' :
            df = df[df['科目名称'].str.len() >= 6]
        else:
            df = df[df['科目名称'].str.len() < 6]
        df = df[(df['科目名称'].str.contains('国债'))|(df['科目名称'].str.contains('T2'))]
        if self.product_code=='SGS958' or self.product_code=='SVU353':
            code_list = df.index.str[-9:-3].tolist()
        else:
            code_list = df.index.str[-6:].tolist()
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': code_list,
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist()
        }
        df = self.create_output_df(dic)
        return df
    def data_check(self,date,product_name,pure_value,property,liabilities,security,stock,bond,derivative,unit_pure,accumulated_pure,accumulated_pure_d):
        if len(date)==0:
            print('date info为空请检查代码')
            date=[None]
        if len(product_name)==0:
            print('product_name info为空请检查代码')
            product_name=[None]
        if len(pure_value)==0:
            print('pure_value info为空请检查代码')
            pure_value=[None]
        if len(property)==0:
            print('property info为空请检查代码')
            property=[None]
        if len(liabilities)==0:
            print('liabilities info为空请检查代码')
            liabilities=[None]
        if len(security)==0:
            print('security info为空请检查代码')
            security=[None]
        if len(stock)==0:
            print('stock info为空请检查代码')
            stock=[None]
        if len(bond)==0:
            print('bond info为空请检查代码')
            bond=[None]
        if len(derivative)==0:
            print('derivative info为空请检查代码')
            derivative = [None]
        if len(unit_pure)==0:
            print('unit_pure info为空请检查代码')
            unit_pure = [None]
        if len(accumulated_pure)==0:
            print('accumulated_pure info为空请检查代码')
            accumulated_pure = [None]
        if len(accumulated_pure_d)==0:
            print('accumulated_pure_d info为空请检查代码')
            accumulated_pure_d = [None]
        return date,product_name,pure_value,property,liabilities,security,stock,bond,derivative,unit_pure,accumulated_pure,accumulated_pure_d


    def product_info_transfer(self):
        if self.product_code=='SGS958' or self.product_code=='SVU353':#惠盈,高益
            pure_value_name = '资产净值'
            property_name = '资产合计'
            liabilities_name = '负债合计'
            security_name = '证券投资合计'
            stock_name ='其中股票投资'
            bond_name = '其中债券投资'
            unit_pure_name = '单位净值'
            accumulated_pure_name = '累计单位净值'
            accumulated_pure_d_name = '日净值增长率'
            derivative_name= '其中其他衍生工具投资'
        elif self.product_code=='SZJ339':#sf500
            pure_value_name = '基金资产净值:'
            property_name = '资产类合计:'
            liabilities_name = '负债类合计:'
            security_name = '证券投资合计:'
            stock_name = '其中股票投资:'
            bond_name = '其中债券投资:'
            unit_pure_name = '今日单位净值：'
            accumulated_pure_name = '累计单位净值:'
            accumulated_pure_d_name = '净值日增长率(%):'
            derivative_name = '其他衍生工具市值'
        elif self.product_code=='SLA626': #任瑞
            pure_value_name = '基金资产净值:'
            property_name = '资产类合计:'
            liabilities_name = '负债类合计:'
            security_name = '证券投资合计:'
            stock_name = '其中股票投资:'
            bond_name = '其中债券投资:'
            unit_pure_name = '基金单位净值：'
            accumulated_pure_name = '累计单位净值:'
            accumulated_pure_d_name = '净值日增长率(%)'
            derivative_name = '其中其他衍生工具投资'
        elif self.product_code=='STH580': #瑞景
            pure_value_name = '基金资产净值:'
            property_name = '资产类合计:'
            liabilities_name = '负债类合计:'
            security_name = '证券投资合计:'
            stock_name = '其中股票投资:'
            bond_name = '其中债券投资:'
            unit_pure_name = '今日单位净值：'
            accumulated_pure_name = '累计单位净值:'
            if self.date>= 20240828:
                accumulated_pure_d_name = '净值日增长率'
            else:
                  accumulated_pure_d_name = '净值日增长率(%)'
            derivative_name = '其中其他衍生工具投资'
        elif self.product_code=='SST132': #知行
            pure_value_name = '资产净值'
            property_name ='资产合计'
            liabilities_name = '负债合计'
            security_name = '证券投资合计'
            stock_name = '其中股票投资'
            bond_name = '其中债券投资:'
            unit_pure_name = '今日单位净值'
            accumulated_pure_name = '累计单位净值'
            accumulated_pure_d_name = '日净值增长率'
            derivative_name = '其中其他衍生工具投资'
        else:
            pure_value_name = '基金资产净值:'
            property_name = '资产类合计:'
            liabilities_name = '负债类合计:'
            security_name = '证券投资合计:'
            stock_name = '其中股票投资:'
            bond_name = '其中债券投资:'
            unit_pure_name = '今日单位净值：'
            accumulated_pure_name = '累计单位净值:'
            accumulated_pure_d_name = '净值日增长率'
            derivative_name='其中其他衍生工具投资'
        return pure_value_name,property_name,liabilities_name,security_name,stock_name,bond_name,unit_pure_name,accumulated_pure_name,accumulated_pure_d_name,derivative_name

    def process_info_sheet(self):
        pure_value_name, property_name, liabilities_name, security_name, stock_name, bond_name, unit_pure_name, accumulated_pure_name, accumulated_pure_d_name, derivative_name=self.product_info_transfer()
        df = self.df.copy()
        date = self.date
        product_name=self.product_code
        pure_value = df.loc[df['科目代码'] == pure_value_name, ['市值']].values
        pure_value = pure_value.flatten()
        property = df.loc[df['科目代码'] == property_name, ['市值']].values
        property = property.flatten()
        liabilities = df.loc[df['科目代码'] == liabilities_name, ['市值']].values
        liabilities = liabilities.flatten()
        security = df.loc[df['科目代码'] == security_name, ['市值']].values
        security = security.flatten()
        stock = df.loc[df['科目代码'] == stock_name, ['市值']].values
        stock = stock.flatten()
        bond = df.loc[df['科目代码'] == bond_name, ['市值']].values
        bond = bond.flatten()
        unit_pure = df.loc[df['科目代码'] == unit_pure_name, ['科目名称']].values
        unit_pure = unit_pure.flatten()
        accumulated_pure = df.loc[df['科目代码'] ==accumulated_pure_name, ['科目名称']].values
        accumulated_pure = accumulated_pure.flatten()
        accumulated_pure_d = df.loc[df['科目代码'] == accumulated_pure_d_name, ['科目名称']].values
        accumulated_pure_d = accumulated_pure_d.flatten()
        derivative = df.loc[df['科目代码'] == derivative_name, ['市值']].values
        derivative = derivative.flatten()
        date, product_name, pure_value, property, liabilities, security, stock, bond, derivative, unit_pure, accumulated_pure, accumulated_pure_d=self.data_check(date,
        product_name, pure_value, property, liabilities, security, stock, bond, derivative, unit_pure, accumulated_pure, accumulated_pure_d)
        dic = {
            '持仓日期':[date],
            '产品名称': [self.product_code],
            '资产净值': pure_value,
            '资产总值': property,
            '资产负债': liabilities,
            '证券市值': security,
            '股票市值':  stock,
            '债券市值':  bond,
            '其他衍生工具市值': derivative,
            '产品净值':  unit_pure,
            '产品累计净值': accumulated_pure,
            '产品日涨跌幅': accumulated_pure_d
        }
        df = self.create_output_df(dic)
        return df