import sys, pprint
import asyncio
import logging
import json
from datetime import datetime, date

from strategy import Strategy, signal, Channel, PositionKind, backtest, OperationKind, TradeKind, AssetType, \
    OrderType, uuid, ta, windowed_ta, model, mstrategy, LoggingStdout

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)
sys.stdout = LoggingStdout()
sys.displayhook = pprint.pprint

class KlineLogger(Strategy):
    def __new__(cls, conf, ctx):
        print(json.dumps(conf))
        dis = super().__new__(cls, conf)
        dis.conf = {
            'pair': 'BTC_USDT',
            'xch': 'Binance',
            'kline_freq': 60,  # seconds
            'order_conf': {
                'dry_mode': True,
                'order_mode': 'limit',
                'asset_type': AssetType.Spot,
                'execution_instruction': None
            }
        }
        # dis.kline = ...
        dis.initialized = False
        return dis
        pass

    def __init__(self, conf, ctx):
        pass

    def whoami(self):
        return "py_kline_logger_%s_%s" % (self.conf['xch'], self.conf['pair'],)

    def init(self):
        if self.initialized is not True:
            self.initialized = True
            print(f"Initialized {self.whoami()}")

    async def eval(self, event):
        #event.debug()
        signals = []
        return signals

    def models(self):
        return {}

    def channels(self):
        return ((Channel("candles", self.conf['xch'], self.conf['pair']),))


async def backtest_run(*args, **kwargs):
    return await backtest.it_backtest(*args, **kwargs)

def print_and_zero(log):
    print(log)
    return 0.0

KLINE_LOGGER_DRAW_ENTRIES = [(
    "Prices and EMA",
    lambda x:
        [
            (
                "price",
                x['prices'][('Binance', 'BTC_USDT')] if ('Binance', 'BTC_USDT') in x['prices'] else 0.0),
            ])
    ,
]

PRINT_DRAW_ENTRIES = [("print", lambda x: [("zero", print_and_zero(x))])]

if __name__ == '__main__':
    conf = {
        'pair': 'BTC_USDT',
        'short_window_size': 100,
        'long_window_size': 1000,
        'sample_freq': 60,  # seconds
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
    positions = asyncio.run(backtest_run("mr_py_test", lambda ctx: KlineLogger(
        conf, ctx), date(2021, 8, 1), date(2021, 8, 9), KLINE_LOGGER_DRAW_ENTRIES))
    print('took %d positions' % len(positions))


mstrategy(KlineLogger)

Strat = KlineLogger
