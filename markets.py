import os
import tinkoff
import inspect
from tinkoff.investments.api.market import MarketInstrumentsAPI
from tinkoff.invest import CandleInterval, Client, InstrumentStatus, RequestError
from keys import token
import pandas as pd
import pickle
from tools import cast_money, create_df
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
        self.to_csv(self.instruments, 'market/market_list.csv')
        self.to_pickle(instr, 'market/market_list.pickle')

    def load_markets(self):
        #self.instruments = self.from_csv('market/market_list.csv')
        self.instruments = self.from_picle('market/market_list.pickle')

    def to_pickle(self,item, path):
        with open(path, 'wb') as handle:
            pickle.dump(item, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def from_picle(self, path):
        with open(path, 'rb') as handle:
            b = pickle.load(handle)
        return b

    def to_csv(self, item, path):
        item.to_csv(path, index=False)

    def from_csv(self, path):
        return pd.read_csv(path)

    #сбор истории всего рынка
    def collect_history_market(self):
        for instrument_name, instrument_items in self.instruments.items():
            print('loading ',instrument_name)
            self.collect_history_instrument(instrument_items)
            print('loaded ', instrument_name)

    #Сбор истории конкретного инструмента (бонды, фьючи, акции итд)
    def collect_history_instrument(self,instrument):
        for item_figi, item in instrument.items():
            self.collect_history_item(item_figi)

    #сбор истории конкретной позиции по FIGI
    def collect_history_item(self, instrument_figi):
        try:
            with Client(self.token) as client:
                r = client.market_data.get_candles(
                    figi=instrument_figi,
                    from_=datetime.utcnow() - timedelta(days=7),
                    to=datetime.utcnow(),
                    interval=CandleInterval.CANDLE_INTERVAL_HOUR  # см. utils.get_all_candles
                )
                # print(r)

                df = create_df(r.candles)

        except RequestError as e:
            print(str(e))

if __name__ == "__main__":
    market = Markets(token.main_ro)
    #market.update_markets()
    market.load_markets()
    market.collect_history_market()