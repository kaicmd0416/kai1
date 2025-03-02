from Trading.trading_weight.trading_weight_selecting import trading_weight_combination_main
from Trading.trading_order.trading_order_processing import trading_xy_main,trading_rr_main
def trading_weight_main():#portfolio_weight准备完毕之后触发这个
    trading_weight_combination_main()
def xy_trading_main():
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    t0_mode='mode_v2'
    trading_mode='mode_v2'
    trading_xy_main(trading_mode,t0_mode)
def rr_trading_main():
    trading_rr_main()

if __name__ == '__main__':
    trading_weight_main()
    # xy_trading_main()
    #rr_trading_main()
