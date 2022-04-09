from tinkoff.invest import HistoricCandle
from tinkoff.invest import Client, CandleInterval
from datetime import datetime, timedelta, timezone
import pytz

def cast_money(v):
    return v.units + v.nano / 1e9 # nano - 9 нулей


class intervals:

    def __init__(self, interval):
        utc = pytz.UTC
        total_time = timedelta(days=365*3)
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