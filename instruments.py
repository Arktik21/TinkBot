from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import keys
from keys import token
from tinkoff.invest import CandleInterval, Client

class instrument_params:
    figi = ""
    token = ""

class instrument:
    def __init__(self, params):

        self.figi = params.figi
        self.token = params.token

    def get_full_history(self):
        with Client(self.token) as client:
            candles = client.get_all_candles(figi = self.figi,
                                             from_ = datetime.utcnow() - timedelta(days=365),
                                             to=datetime.utcnow() - timedelta(days=363),
                                             interval=CandleInterval.CANDLE_INTERVAL_HOUR)
            for candle in candles:
                print(candle)



if __name__ == '__main__':
    current_instrument = instrument_params
    current_instrument.token = keys.token.main_ro
    current_instrument.figi = "BBG004730N88"

    instrument_init = instrument(current_instrument)
    instrument_init.get_full_history()
