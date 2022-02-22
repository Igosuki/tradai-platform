from talib import stream
import numpy as np
# note that all ndarrays must be the same length!
inputs = {
    'open': np.random.random(100),
    'high': np.random.random(100),
    'low': np.random.random(100),
    'close': np.random.random(100),
    'volume': np.random.random(100)
}

macd, macdsignal, macdhist = stream.MACD(inputs['close'], fastperiod=12, slowperiod=26, signalperiod=9)

print(macd)
print(macdsignal)
print(macdhist)
