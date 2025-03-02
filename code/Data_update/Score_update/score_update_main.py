from Score_update.rrScore_update import rrScore_update
from Score_update.scoreCombination_update import combineScore_update
from Score_update.scorePortfolio_update import scorePortfolio_update
import global_tools_func.global_tools as gt
def score_update_main(score_type,start_date,end_date): #这里面的date是target_date
    rr=rrScore_update(start_date,end_date)
    su=scorePortfolio_update(start_date,end_date)
    cu=combineScore_update(start_date,end_date)
    if score_type=='fm':
          rr.rr_update_main()
          su.scorePortfolio_update_main()
          cu.score_combination_main()
    else:
        print('非生产时间段')