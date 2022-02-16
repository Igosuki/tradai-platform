from .tradai import model


def __getattr__(name):
    return getattr(model, name)
