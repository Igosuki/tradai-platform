from .strategy import backtest


def __getattr__(name):
    return getattr(backtest, name)
