import pandas as pd
import os
from Optimizer_Backtesting.main.backtest_main import backtesting_main
def history_config_withdraw():
    inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    inputpath = os.path.join(inputpath, 'config_history.xlsx')
    df = pd.read_excel(inputpath)
    return df
def history_running_main():
    df_config=history_config_withdraw()
    bm=backtesting_main()
    bm.optimizer_history_backtesting_main(df_config)

if __name__ == '__main__':
    history_running_main()
    pass
