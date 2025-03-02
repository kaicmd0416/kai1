from MktData_update.Mktdata_update import indexReturn_update,indexComponent_update,stockData_update
import global_tools_func.global_tools as gt
def MktData_update_main(start_date,end_date):

    working_days_list=gt.working_days_list(start_date,end_date)
    for available_date in working_days_list:
        IRU = indexReturn_update(available_date,available_date)
        ICU=indexComponent_update(available_date,available_date)
        SDU=stockData_update(available_date,available_date)
        SDU.stock_data_update_main()
        IRU.index_return_update_main()
        ICU.index_component_update_main()