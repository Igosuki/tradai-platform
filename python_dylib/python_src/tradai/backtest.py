from .tradai import backtest


def __getattr__(name):
    return getattr(backtest, name)
