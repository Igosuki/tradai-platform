from .tradai import backtest


def __getattr__(name):
    return getattr(backtest, name)

async def run_it_backtest(*args, **kwargs):
    return await backtest.it_backtest(*args, **kwargs)

async def run_single_backtest(*args, **kwargs):
    return await backtest.run_single_backtest(*args, **kwargs)
