import os

from tinkoff.investments.api.market import MarketInstrumentsAPI
from tinkoff.invest import CandleInterval, Client, InstrumentStatus, RequestError
from keys import token
import pandas as pd
from datetime import datetime, timedelta
Status = InstrumentStatus.INSTRUMENT_STATUS_ALL
import time
import pytz

utc=pytz.UTC
with Client(token.main_ro) as client:
    bonds = client.instruments.bonds(instrument_status=Status)
    currencies = client.instruments.currencies(instrument_status=Status)
    shares = client.instruments.shares(instrument_status=Status)
    futures = client.instruments.futures(instrument_status=Status)



    df_bonds = pd.DataFrame([{
        'figi': bond.figi,
        'ticker': bond.ticker
    }for bond in bonds.instruments])

    df_currencies = pd.DataFrame([{
        'figi': currencie.figi,
        'ticker': currencie.ticker
    }for currencie in currencies.instruments])

    df_shares = pd.DataFrame([{
        'figi': share.figi,
        'ticker': share.ticker
    }for share in shares.instruments])

    df_futures = pd.DataFrame([{
        'figi': future.figi,
        'ticker': future.ticker,
        'start_time': future.first_trade_date
    }for future in futures.instruments])

for _, item in df_futures.iterrows():



    try:
        with Client(token.main_ro) as client:
            time_start = item['start_time']
            while True:
                df_candle = pd.DataFrame([{
                    'close': [],
                    'high': [],
                    'low': [],
                    'open': [],
                    'value': [],
                    'time': []
                } for future in futures.instruments])

                time_end = time_start + timedelta(days=7)
                if time_start >= utc.localize(datetime.utcnow()):
                    break
                candles = client.market_data.get_candles(
                    figi = item['figi'],
                    from_ = datetime.utcnow() - timedelta(days = 7),
                    to=datetime.utcnow(),
                    interval=CandleInterval.CANDLE_INTERVAL_HOUR)

                time_start += timedelta(days=7)

            for candle in candles.candles:
                df_candle = pd.DataFrame([{
                    'close': candle.figi,
                    'high': candle.high,
                    'low': candle.low,
                    'open': candle.open,
                    'value':candle.volueme,
                    'time':candle.time
                } for future in futures.instruments])

        time.sleep(1/100)
    except RequestError as e:
        print(str(e))



