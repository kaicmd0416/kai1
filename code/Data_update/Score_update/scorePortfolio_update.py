import os
import pandas as pd
import global_setting.global_dic as glv
import global_tools_func.global_tools as gt
class scorePortfolio_update:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
    def se_date_withdraw(self,input_list):
        input_s = input_list[0]
        index = input_s.rindex('_')
        start_date = gt.strdate_transfer(str(input_s)[index + 1:-4])
        return start_date
    def rr_top_update_main(self):
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        inputpath = glv.get('output_score')
        inputpath_a1 = os.path.join(inputpath, 'a1')
        inputpath_a3 = os.path.join(inputpath, 'a3')
        outputpath = glv.get('output_portfolio')
        outputpath_test = os.path.join(outputpath, 'a1_top' + str(5))
        try:
            os.listdir(outputpath_test)
        except:
            start_date = '2024-01-06'
        working_list = gt.working_days_list(start_date, end_date)
        for date in working_list:
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_workday_calculate(date)
            available_date = gt.intdate_transfer(available_date)
            daily_a1 = gt.file_withdraw(inputpath_a1, available_date)
            daily_a3 = gt.file_withdraw(inputpath_a3, available_date)
            df_a1 = gt.readcsv(daily_a1)
            df_a3 = gt.readcsv(daily_a3)
            for number in [5, 100, 200, 300]:
                outputpath_a1 = os.path.join(outputpath, 'a1_top' + str(number))
                outputpath_a3 = os.path.join(outputpath, 'a3_top' + str(number))
                gt.folder_creator2(outputpath_a1)
                gt.folder_creator2(outputpath_a3)
                daily_outputpath_a1 = os.path.join(outputpath_a1, 'a1_top' + str(number) + '_' + str(date2) + '.csv')
                daily_outputpath_a3 = os.path.join(outputpath_a3, 'a3_top' + str(number) + '_' + str(date2) + '.csv')
                slice_a1 = df_a1.loc[:number - 1]
                slice_a3 = df_a3.loc[:number - 1]
                slice_a1['weight'] = slice_a1['final_score'] / slice_a1['final_score'].sum()
                slice_a3['weight'] = slice_a3['final_score'] / slice_a3['final_score'].sum()
                slice_a1 = slice_a1[['code', 'weight']]
                slice_a3 = slice_a3[['code', 'weight']]
                slice_a1.to_csv(daily_outputpath_a1, index=False)
                slice_a3.to_csv(daily_outputpath_a3, index=False)
    def ubp_top_update_main(self):
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        outputpath = glv.get('output_portfolio')
        outputpath = os.path.join(outputpath, 'ubp500')
        gt.folder_creator2(outputpath)
        inputpath_score = glv.get('input_score')
        inputpath_score = os.path.join(inputpath_score, 'rr_score')
        inputpath_score = os.path.join(inputpath_score, 'UBP_500alpha')
        input_list = os.listdir(outputpath)
        if len(input_list) == 0:
            input_list2=os.listdir(inputpath_score)
            input_list2.sort()
            start_date = self.se_date_withdraw(input_list2)
        working_days_list = gt.working_days_list(start_date, end_date)
        for date in working_days_list:
            print(date)
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_weeks_lastday2(date)
            available_date = gt.intdate_transfer(available_date)
            daily_inputpath = gt.file_withdraw(inputpath_score, available_date)
            try:
                df = pd.read_excel(daily_inputpath, header=None)
            except:
                df = pd.DataFrame()
            if len(df) != 0:
                df = df.iloc[3:]
                df.columns = ['code', 'chiname', 'weight']
                df = df[df['weight'] != 0]
                df = df[['code', 'weight']]
                daily_outputpath = os.path.join(outputpath, 'UBP500alpha_' + str(date2) + '.csv')
                df.to_csv(daily_outputpath, index=False)
            else:
                print('ubp_500' + str(date) + '暂时没有数据')
    def scorePortfolio_update_main(self):
        self.ubp_top_update_main()
        self.rr_top_update_main()