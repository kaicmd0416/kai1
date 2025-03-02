import os
import pandas as pd
import global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import datetime
inputpath_score=glv.get('input_score')
outputpath_score=glv.get('output_score')
inputpath_score_config=glv.get('score_mode')
class rrScore_update:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
    def raw_rr_time_checking(self,df_score, target_date):
        available_date = df_score['valuation_date'].unique().tolist()[0]
        available_date2 = gt.last_weeks_lastday2(target_date)
        if available_date != available_date2:
            print(
                'rr_score的最近更新日期是:' + str(available_date) + '上周最后一个工作日的日期是' + str(available_date2))
            raise ValueError
    def raw_rr_saving_history(self,mode_type):  # 提取score，更新的时候要把最后一周的文件删掉
        df_config = pd.read_excel(inputpath_score_config)
        mode_name = df_config[df_config['score_mode'] == mode_type]['mode_name'].tolist()[0]
        outputpath_score3 = os.path.join(outputpath_score, mode_name)
        gt.folder_creator2(outputpath_score3)
        inputpath_score2 = os.path.join(inputpath_score, 'rr_score')
        outputpath_score2 = os.path.join(outputpath_score, 'rr_' + str(mode_type))
        gt.folder_creator2(outputpath_score2)
        inputpath_score2 = os.path.join(inputpath_score2, 'W' + str(mode_type) + '_his_Ranking.xlsx')
        df_score = pd.read_excel(inputpath_score2, header=None)
        df_score.columns = ['valuation_date', 'code']
        df_score = df_score[df_score['valuation_date'] >= '2024-01-01']
        date_list2 = df_score['valuation_date'].unique().tolist()
        start_date = date_list2[0]
        end_date = gt.last_weeks_lastday()
        end_date = gt.last_workday_calculate(end_date)
        working_day_list = gt.working_days_list(start_date, end_date)
        a = None
        for date in working_day_list:
            print(date)
            if date in date_list2:
                a = date
            slice_df_score = df_score[df_score['valuation_date'] == a]
            slice_df_score = gt.rr_score_processing(slice_df_score)
            slice_df_score['valuation_date'] = date
            date2 = gt.intdate_transfer(date)
            outputpath_saving = os.path.join(outputpath_score2, 'rr_' + str(date2) + '.csv')
            outputpath_saving2 = os.path.join(outputpath_score3, 'rr_' + str(date2) + '.csv')
            slice_df_score.to_csv(outputpath_saving, index=False)
            slice_df_score.to_csv(outputpath_saving2, index=False)

    def raw_rr_updating(self,mode_type,start_date):  # 提取score
        df_config = pd.read_excel(inputpath_score_config)
        mode_name = df_config[df_config['score_mode'] == mode_type]['mode_name'].tolist()[0]
        inputpath_score2 = os.path.join(inputpath_score, 'rr_score')
        outputpath_score2 = os.path.join(outputpath_score, 'rr_' + str(mode_type))
        outputpath_score3 = os.path.join(outputpath_score, mode_name)
        gt.folder_creator2(outputpath_score3)
        working_day_list = gt.working_days_list(start_date, self.end_date)
        rr_list = os.listdir(inputpath_score2)
        df_rr = pd.DataFrame()
        df_rr['rr'] = rr_list
        df_rr = df_rr[df_rr.rr.str.contains(str(mode_type))]
        df_rr['rr2'] = df_rr['rr'].apply(lambda x: str(x)[:8])
        df_rr = df_rr[df_rr['rr2'] == 'ThisWeek']
        df_rr.sort_values(by='rr')
        file_name = df_rr['rr'].tolist()[0]
        inputpath_score_update = os.path.join(inputpath_score2, file_name)
        df_score = pd.read_excel(inputpath_score_update, header=None)
        df_score.columns = ['valuation_date', 'code']
        df_score = gt.rr_score_processing(df_score)
        df_score_original = df_score.copy()
        for target_date in working_day_list:
            self.raw_rr_time_checking(df_score_original, target_date)
            available_date = gt.last_workday_calculate(target_date)
            df_score['valuation_date'] = available_date
            available_date2 = gt.intdate_transfer(available_date)
            outputpath_saving = os.path.join(outputpath_score2, 'rr_' + str(available_date2) + '.csv')
            outputpath_saving2 = os.path.join(outputpath_score3, 'rr_' + str(available_date2) + '.csv')
            df_score.to_csv(outputpath_saving, index=False)
            df_score.to_csv(outputpath_saving2, index=False)

    def rr_update_main(self):
        df_config = pd.read_excel(inputpath_score_config)
        df_config['score_type2'] = df_config['score_type'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[df_config['score_type2'] == 'rr']['score_mode'].tolist()
        for mode_type in mode_type_using_list:
            outputpath_score2 = os.path.join(outputpath_score, 'rr_' + str(mode_type))
            try:
                inputlist = os.listdir(outputpath_score2)
            except:
                inputlist = []
            today = datetime.datetime.now()
            today = gt.strdate_transfer(today)
            lastweeks_lastday = gt.last_weeks_lastday2(today)
            start_date2 = gt.next_workday_calculate(lastweeks_lastday)
            if len(inputlist) == 0:
                self.raw_rr_saving_history(mode_type)
                self.raw_rr_updating(mode_type, start_date2)
            else:
                if self.start_date<start_date2:
                    print('输入start_date有误已自动调整到:'+str(start_date2))
                else:
                    start_date2=self.start_date
                self.raw_rr_updating(mode_type, start_date2)




