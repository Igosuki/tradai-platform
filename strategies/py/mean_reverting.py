import asyncio
import logging
import json
import pprint
import sys
from datetime import datetime, date

import jsonpickle
from tradai import Strategy, signal, Channel, PositionKind, backtest, OperationKind, TradeKind, AssetType, \
    OrderType, ta, windowed_ta, model, mstrategy, uuid, LoggingStdout
import pyarrow as pa

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)
sys.stdout = LoggingStdout()
sys.displayhook = pprint.pprint

class MeanReverting(Strategy):
    def __new__(cls, conf, ctx):
        print(jsonpickle.encode(conf))
        dis = super().__new__(cls, conf)
        dis.conf = {
            'pair': 'BURGER_USDT',
            'short_window_size': 100,
            'long_window_size': 1000,
            'sample_freq': 60,  # seconds
            'threshold_short': 0.02,
            'threshold_long': -0.02,
            'threshold_eval_freq': 1,
            'dynamic_threshold': True,
            'threshold_window_size': 1000,
            'stop_loss': 0.1,
            'stop_gain': 0.075,
            'xch': 'binance',
            'order_conf': {
                'dry_mode': True,
                'order_mode': 'limit',
                'asset_type': AssetType.Spot,
                'execution_instruction': None
            }
        }
        db = ctx.db

        dis.ppo_model = model.persistent_ta("ppo_%s" % dis.conf['pair'], db, ta.ppo(
            dis.conf['short_window_size'], dis.conf['long_window_size']))
        if dis.conf['dynamic_threshold'] is True:
            dis.threshold_model = model.persistent_window_ta("thresholds_%s" % dis.conf['pair'], db, dis.conf['threshold_window_size'], windowed_ta.thresholds(
                dis.conf['threshold_short'], dis.conf['threshold_long']))
        dis.initialized = False
        return dis
        pass

    def __init__(self, conf, ctx):
        pass

    def whoami(self):
        return "py_mean_reverting_%s" % (self.conf['pair'],)

    def init(self):
        if self.initialized is not True:
            self.ppo_model.try_load()
            self.threshold_model.try_load()
            self.initialized = True
            print(f"Initialized {self.whoami()}")

    async def eval(self, event):
        #event.debug()
        self.ppo_model.next(event.vwap())
        ppo = self.ppo_model.values()[0]
        if ppo is None:
            return
        if self.conf['dynamic_threshold']:
            self.threshold_model.next(ppo)
            threshold_long = self.threshold_model.values()[0]
            threshold_short = self.threshold_model.values()[1]
        else:
            threshold_long = self.conf['threshold_long']
            threshold_short = self.conf['threshold_short']

        signals = []
        if event.low() > 0 and ppo < 0.0:
            signals.append(signal(PositionKind.Short, OperationKind.Close, TradeKind.Buy, event.low(
            ), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.high() > 0 and ppo > 0.0:
            signals.append(signal(PositionKind.Long, OperationKind.Close, TradeKind.Sell, event.high(
            ), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.low() > 0 and ppo < threshold_long:
            signals.append(signal(PositionKind.Long, OperationKind.Open, TradeKind.Buy, event.low(
            ), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))
        if event.high() > 0 and ppo > threshold_short:
            signals.append(signal(PositionKind.Short, OperationKind.Open, TradeKind.Sell, event.high(
            ), self.conf['pair'], self.conf['xch'], True, AssetType.Spot, OrderType.Limit, datetime.now(), uuid.uuid4(), None, None, None, None))

        return signals

    def models(self):
        return self.ppo_model.export() + self.threshold_model.export()

    def channels(self):
        return (Channel("orderbooks", self.conf['xch'], self.conf['pair'], time_unit='minute', units=1),)

def print_and_zero(log):
    print(log)
    return 0.0

MEAN_REVERTING_DRAW_ENTRIES = [(
    "Prices and EMA",
    lambda x:
        [
            (
                "mid_price",
                x['prices'][('Binance', 'BTC_USDT')] if ('Binance', 'BTC_USDT') in x['prices'] else 0.0),
            ("short_ema", x['model']['short_ema']['current']),
            ("long_ema", x['model']['long_ema']['current'])])
    ,
    ("APO", lambda x: [("ppo", x['model']['ppo']), ("threshold_low", x['model']['low']), ("threshold_short", x['model']['high'])]),
    (
        "Portfolio Return",
        lambda x: [("pfl_return", x['snapshot']['current_return'])]),
    ("Portfolio PnL", lambda x: [("pnl", x['snapshot']['pnl'])]),
    ("Portfolio Value", lambda x: [("value", x['snapshot']['value'])]),
    (
        "Nominal (units)",
        lambda x: [("nominal",
                    x['nominal_positions'][('Binance', 'BTC_USDT')] if ('Binance', 'BTC_USDT') in x['nominal_positions'] else 0.0)])
    #,("print", lambda x: [("zero", print_and_zero(x))])
]

PRINT_DRAW_ENTRIES = [("print", lambda x: [("zero", print_and_zero(x))])]

async def backtest_run(*args, **kwargs):
    return await backtest.backtest_with_range(*args, **kwargs)

async def market_events_df(*args, **kwargs):
    return await backtest.market_events_df(*args, **kwargs)


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
    #asyncio.run(market_events_df((Channel("orderbooks", 'binance', 'BTC_USDT', time_unit='minute', units=1, tick_rate_millis=60000),), datetime(2022, 1, 1), datetime(2022, 1, 2)))
    report: backtest.BacktestReport = asyncio.run(backtest_run("mr_py_test", lambda ctx: MeanReverting(
        conf, ctx), datetime(2021, 10, 18), datetime(2021, 11, 30)))
    report.write_html()
    for table in ["snapshots", "models", "events"]:
        for array in report.events_df(table):
            flattened = array.flatten()
            fields = [field.name for field in array.type]
            table = pa.Table.from_arrays(flattened, names=fields)
            print(table.to_pandas().head())

mstrategy(MeanReverting)

Strat = MeanReverting
