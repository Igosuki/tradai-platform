from strategies import PythonStrat

class Strat(PythonStrat):
    def __new__(cls, conf):
        return super().__new__(cls, conf)
        pass

