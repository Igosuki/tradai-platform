from .tradai import ta


def __getattr__(name):
    return getattr(ta, name)
