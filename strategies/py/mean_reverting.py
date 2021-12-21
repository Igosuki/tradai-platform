import asyncio

from strategy import Strategy, TradeSignal, Channel, PositionKind, backtest, OperationKind, TradeKind, AssetType, \
    OrderType, uuid
from inspect import getmembers
from datetime import datetime

class MeanReverting(Strategy):
    def __new__(cls, conf):
        dis = super().__new__(cls, conf)
        dis.conf = conf
        return dis
        pass

    def init(self):
        print("initi")

    def eval(self, event):
        return [TradeSignal(PositionKind.Long, OperationKind.Open, TradeKind.Buy, 1.0, 'BTC_USDT', 'Binance', True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), 1.0, None, None, None)]

    def models(self):
        print("models")
        return ()

    def channels(self):
        return ((Channel("orderbooks", "Binance", "BTC_USDT"),))

async def backtest_run(*args, **kwargs):
    await backtest.it_backtest(*args, **kwargs)

if __name__ == '__main__':
    strat = MeanReverting({})
    signals = strat.eval({})
    print(signals)
    #asyncio.run(backtest_run("mr_py_test", ))
