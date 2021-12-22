import asyncio
import logging
from datetime import datetime, date

from strategy import Strategy, TradeSignal, Channel, PositionKind, backtest, OperationKind, TradeKind, AssetType, \
    OrderType, uuid

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.DEBUG)

class MeanReverting(Strategy):
    def __new__(cls, conf):
        dis = super().__new__(cls, conf)
        dis.conf = conf
        return dis
        pass

    def __init__(self, conf):
        self.initialized = False
        self.opened = False
        self.trade_count = 0

    def whoami(self):
        return "mean_reverting_py"

    def init(self):
        self.initialized = True
        print("init")

    def eval(self, event):
        #event.debug()
        if self.trade_count < 10:
            if self.opened is True:
                self.opened = False
                return [TradeSignal(PositionKind.Long, OperationKind.Close, TradeKind.Sell, 1.0, 'BTC_USDT', 'Binance', True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), 1.0, None, None, None)]
            else:
                self.opened = True
                self.trade_count += 1
                return [TradeSignal(PositionKind.Long, OperationKind.Open, TradeKind.Buy, 1.0, 'BTC_USDT', 'Binance', True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), 1.0, None, None, None)]

    def models(self):
        print("models")
        return ()

    def channels(self):
        return ((Channel("orderbooks", "Binance", "BTC_USDT"),))

async def backtest_run(*args, **kwargs):
    return await backtest.it_backtest(*args, **kwargs)

if __name__ == '__main__':
    strat = MeanReverting({})
    positions = asyncio.run(backtest_run("mr_py_test", lambda ctx: strat, date(2021, 8, 1), date(2021, 8, 9)))
    for position in positions:
        position.debug()
