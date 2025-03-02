import Product_tracking.global_setting.global_dic as glv
import pandas as pd
def product_name_transfer(product_name):
    inputpath=glv.get('product_detail')
    df=pd.read_excel(inputpath)
    product_name2=df[df['product_name']==product_name]['other_name'].tolist()[0]
    return product_name2
def product_type(product_name):
    inputpath=glv.get('product_detail')
    df=pd.read_excel(inputpath)
    product_discription=df[df['product_name']==product_name]['product_discription'].tolist()[0]
    return product_discription
def product_list():
    inputpath = glv.get('product_detail')
    df = pd.read_excel(inputpath)
    product_list=df['product_name'].tolist()
    return product_list

