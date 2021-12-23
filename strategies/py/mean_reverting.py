import asyncio
import logging
from datetime import datetime, date

from strategy import Strategy, TradeSignal, Channel, PositionKind, backtest, OperationKind, TradeKind, AssetType, \
    OrderType, uuid, ta, model

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

class MeanReverting(Strategy):
    def __new__(cls, conf, ctx):
        dis = super().__new__(cls, conf)
        dis.conf = {
            'pair': 'BTC_USDT',
            'short_window_size': 100,
            'long_window_size': 1000,
            'sample_freq': 60, # seconds
            'threshold_short': 0.01,
            'threshold_long': -0.01,
            'threshold_eval_freq': 1,
            'dynamic_threshold': True,
            'threshold_window_size': 1000,
            'stop_loss': 0.1,
            'stop_gain': 0.075,
            'xch': 'Binance',
            'order_conf': {
                'dry_mode': True,
                'order_mode': 'limit',
                'asset_type': AssetType.Spot,
                'execution_instruction': None
            }
        }
        db = ctx.db
        dis.apo_model = model.persistent_ta("BTC_USDT_apo", db, ta.macd_apo(100, 1000))
        return dis
        pass

    def __init__(self, conf, ctx):
        self.initialized = False

    def whoami(self):
        return "mean_reverting_py"

    def init(self):
        self.initialized = True
        print("init")

    def eval(self, event):
        #event.debug()
        self.apo_model.next(event.vwap())
        apo = self.apo_model.values()[0]
        signals = []
        if event.low() > 0 and apo < 0.0:
            signals.append(TradeSignal(PositionKind.Short, OperationKind.Close, TradeKind.Buy, event.low(), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.high() > 0 and apo > 0.0:
            signals.append(TradeSignal(PositionKind.Long, OperationKind.Close, TradeKind.Sell, event.high(), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.low() > 0 and apo < self.conf['threshold_long']:
            signals.append(TradeSignal(PositionKind.Long, OperationKind.Open, TradeKind.Buy, event.low(), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.high() > 0 and apo > self.conf['threshold_short']:
            signals.append(TradeSignal(PositionKind.Short, OperationKind.Open, TradeKind.Sell, event.high(), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        return signals

    def models(self):
        print("models")
        return ()

    def channels(self):
        return ((Channel("orderbooks", "Binance", "BTC_USDT"),))

async def backtest_run(*args, **kwargs):
    return await backtest.it_backtest(*args, **kwargs)

if __name__ == '__main__':
    positions = asyncio.run(backtest_run("mr_py_test", lambda ctx: MeanReverting({}, ctx), date(2021, 8, 1), date(2021, 8, 9)))
    print('took %d positions' % len(positions))
