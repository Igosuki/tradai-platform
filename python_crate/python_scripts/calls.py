from strategy import *
from datetime import datetime

class CallStrat(Strategy):
    def __new__(cls, conf, ctx):
        dis = super().__new__(cls, conf)
        dis.conf = conf
        return dis
        pass

    def init(self):
        return

    def whoami(self):
        return "CallStrat"

    def eval(self, event):
        return [signal(PositionKind.Short, OperationKind.Close, TradeKind.Buy, event.low(
        ), 'BTC_USDT', 'Binance', True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None)]

    def models(self):
        return ()

    def channels(self):
        return ()

mstrategy(CallStrat)

# import sys, pprint
# sys.stdout = LoggingStdout()
# sys.displayhook = pprint.pprint
# display(sys.modules[__name__])
# print(sys.modules[__name__])
#print("loaded strategy class %s" % type(sys.modules[__name__].__dict__.__strat_class__).__name__)

if __name__ == '__main__':
    s = Strat({})
    s.eval({})

