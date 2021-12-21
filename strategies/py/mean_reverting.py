import asyncio

from strategy import Strategy, TradeSignal, Channel, backtest


class Strat(Strategy):
    def __new__(cls, conf):
        dis = super().__new__(cls, conf)
        dis.conf = conf
        return dis
        pass

    def init(self):
        print("initi")

    def eval(self, event):
        return [TradeSignal('short', 'open', 'buy', 1.0, 'BTC_USDT', 'binance', False, 'spot')]

    def update_model(self, event):
        print("update_model")

    def models(self):
        print("models")
        return ()

    def channels(self):
        print("channels")
        return ()

async def backtest_run():
    await backtest.it_backtest("hello_world")

if __name__ == '__main__':
    print("0", Channel("orderbooks", "Binance", "BTC_USDT").__doc__)
    print("2", Channel.__text_signature__)
    print("4", TradeSignal.__doc__)
    asyncio.run(backtest_run())
    s = Strat({})
    s.eval({})
