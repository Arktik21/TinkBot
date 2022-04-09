import os
import tinkoff
import inspect
from tinkoff.investments.api.market import MarketInstrumentsAPI
from tinkoff.invest import CandleInterval, Client, InstrumentStatus, RequestError
from keys import token
import pandas as pd
from tools import cast_money
from datetime import datetime, timedelta
Status = InstrumentStatus.INSTRUMENT_STATUS_ALL
import time
import pytz

utc=pytz.UTC

# collect all instruments
class Markets:
    def __init__(self, token):
        self.token = token
        self.instruments = dict()

    def update_markets(self):
        instruments = dict()
        with Client(self.token) as client:
            instruments['bonds'] = client.instruments.bonds(instrument_status=Status)
            instruments['currencies'] = client.instruments.currencies(instrument_status=Status)
            instruments['shares'] = client.instruments.shares(instrument_status=Status)
            instruments['futures'] = client.instruments.futures(instrument_status=Status)
            instr = dict()

            for instrument, market in instruments.items():
                instr[instrument] = dict()
                for item in market.instruments:
                    instr[instrument][item.figi] = dict()
                    for att in dir(item):
                        if att.startswith('__') | att.startswith('_'):
                            continue
                        value = getattr(item, att)
                        if isinstance(value, tinkoff.invest.schemas.MoneyValue) | isinstance(value, tinkoff.invest.schemas.Quotation):
                            value = cast_money(value)

                        instr[instrument][item.figi][att] = value
        self.instruments = pd.DataFrame.from_dict(instr)
        self.to_csv('market/market_list.csv')

    def to_csv(self, path):
        self.instruments.to_csv(path, index=False)

    def from_csv(self, path):
        return pd.read_csv(path)



if __name__ == "__main__":
    market = Markets(token.main_ro)
    market.update_markets()
