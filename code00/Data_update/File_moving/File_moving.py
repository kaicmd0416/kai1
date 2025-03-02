import global_tools_func.global_tools as gt
import global_setting.global_dic as glv
class File_moving:
    def __init__(self,time_zoon):
        self.time_zoon=time_zoon
    def data_other_moving(self):
        input = glv.get('input_destination')
        output = glv.get('output_destination')
        gt.folder_creator2(output)
        gt.move_specific_files(input, output)
    def data_product_moving(self):
        input=glv.get('input_prod')
        output=glv.get('output_prod')
        gt.move_specific_files2(input, output)
        print('product_detail已经复制完成')
    def data_realtime_moving(self):
        input=glv.get('input_realtime')
        output=glv.get('output_realtime')
        gt.move_specific_files2(input, output)
        print('realtime_data已经复制完成')
    def data_cbond_moving(self):
        input=glv.get('input_cbond')
        output=glv.get('output_cbond')
        gt.move_specific_files2(input, output)
        print('convertible_bond已经复制完成')
    def file_moving_update_main(self):
        if self.time_zoon=='time_2':
            self.data_realtime_moving()
            self.data_cbond_moving()
        elif self.time_zoon=='time_3':
            self.data_product_moving()
    def file_moving_history_main(self):
        self.data_other_moving()
        self.data_realtime_moving()
        self.data_cbond_moving()
        self.data_product_moving()


