import pandas as pd
import global_setting.global_dic as glv
class tools_func:
    def option_name_transfer_NJ300(self,option_name):
        # 输入为 产品下期权名
        # 输出为 对应产品代码
        if option_name[0:5] == '沪深300':
            if option_name[5] == '沽':
                if option_name[6] == '1':
                    option_name_new = 'IO241' + option_name[7] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'IO240' + option_name[6] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[6] == '1':
                    option_name_new = 'IO241' + option_name[7] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'IO240' + option_name[6] + '-C-' + option_name[-4:] + '.CFE'
        elif option_name[0:4] == '上证50':
            if option_name[4] == '沽':
                if option_name[5] == '1':
                    option_name_new = 'HO241' + option_name[6] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'HO240' + option_name[5] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[5] == '1':
                    option_name_new = 'HO241' + option_name[6] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'HO240' + option_name[5] + '-C-' + option_name[-4:] + '.CFE'
        elif option_name[0:6] == '中证1000':
            if option_name[6] == '沽':
                if option_name[7] == '1':
                    option_name_new = 'MO241' + option_name[8] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'MO240' + option_name[7] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[7] == '1':
                    option_name_new = 'MO241' + option_name[8] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'MO240' + option_name[7] + '-C-' + option_name[-4:] + '.CFE'
        else:
            print('期权名字格式特殊转换失败，请手动改正')
            option_name_new = option_name
        return option_name_new

    def option_name_transfer(self,option_name):
        # 输入为 盛丰1000外其他产品下期权名
        # 输出为 对应产品代码
        if option_name[0:5] == '沪深300':
            option_name_new = 'IO' + option_name[-11:] + '.CFE'
        elif option_name[0:4] == '上证50':
            option_name_new = 'HO' + option_name[-11:] + '.CFE'
        elif option_name[0:6] == '中证1000':
            option_name_new = 'MO' + option_name[-11:] + '.CFE'
        else:
            print('期权名字格式特殊转换失败，请手动改正')
            option_name_new = option_name
        return option_name_new
    def product_NameCode_transfer(self,product_code):
        inputpath=glv.get('L4_config')
        df=pd.read_excel(inputpath)
        product_name=df[df['product_code']==product_code]['product_name'].tolist()[0]
        return product_name
    def product_CodeName_transfer(self,product_name):
        inputpath=glv.get('L4_config')
        df=pd.read_excel(inputpath)
        product_code=df[df['product_name']==product_name]['product_code'].tolist()[0]
        return product_code
    def product_loc_withdraw(self,product_code):
        inputpath = glv.get('L4_config')
        df = pd.read_excel(inputpath)
        columns = df[df['product_code'] == product_code]['columns'].tolist()[0]
        columns2 = df[df['product_code'] == product_code]['df'].tolist()[0]
        return columns,columns2
