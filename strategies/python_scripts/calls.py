from strategies import PythonStrat, TradeSignal

class Strat(PythonStrat):
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

if __name__ == '__main__':
    s = Strat({})
    s.eval({})
