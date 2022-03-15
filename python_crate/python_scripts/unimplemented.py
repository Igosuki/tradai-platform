from tradai import *

class Strat(Strategy):
    def __new__(cls, conf, ctx):
        return super().__new__(cls, conf)
        pass
    def whoam(self):
        return "Strat"

mstrategy(Strat)
