from tinkoff.invest import HistoricCandle
from tinkoff.invest import Client, CandleInterval
from datetime import datetime, timedelta, timezone
import pytz
import plotly.graph_objects as go

import pandas as pd
from datetime import datetime

def cast_money(v):
    return v.units + v.nano / 1e9 # nano - 9 нулей


class intervals:

    def __init__(self, interval):
        utc = pytz.UTC
        total_time = timedelta(days=365*4)
        self.start_time = datetime.utcnow().replace(tzinfo=utc) - total_time
        self.delta = 1
        self.interval = interval
        if interval == CandleInterval.CANDLE_INTERVAL_5_MIN:
            self.delta = 8
        if interval == CandleInterval.CANDLE_INTERVAL_15_MIN:
            self.delta = 24
        if interval == CandleInterval.CANDLE_INTERVAL_HOUR:
            self.delta = 24
        if interval == CandleInterval.CANDLE_INTERVAL_DAY:
            self.delta = 24*7

        self.deltas = []
        for it in range(int(total_time.days * 24 / self.delta) + 1):
            self.deltas.append(self.start_time + timedelta(hours=self.delta * it))

        self.delta = timedelta(hours=self.delta)



def create_df(candles : [HistoricCandle]):
    df = [{
        'time': c.time,
        'volume': c.volume,
        'open': cast_money(c.open),
        'close': cast_money(c.close),
        'high': cast_money(c.high),
        'low': cast_money(c.low),
    } for c in candles]

    return df




df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')
def plot_candles(candles):
    fig = go.Figure(data=[go.Candlestick(x=candles['time'],
                    open=candles['open'],
                    high=candles['High'],
                    low=candles['Low'],
                    close=candles['Close'])])

    fig.show()