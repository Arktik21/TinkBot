from tinkoff.invest import HistoricCandle


def cast_money(v):
    return v.units + v.nano / 1e9 # nano - 9 нулей


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