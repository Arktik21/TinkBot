import os
import tinkoff
import inspect
from tinkoff.investments.api.market import MarketInstrumentsAPI
from tinkoff.invest import CandleInterval, Client, InstrumentStatus, RequestError
from keys import token
import pandas as pd
import pickle
from tools import cast_money, create_df, intervals
from datetime import datetime, timedelta, timezone
Status = InstrumentStatus.INSTRUMENT_STATUS_ALL
import time
import pytz
#now = datetime.now(timezone.utc)

utc=pytz.UTC

# collect all instruments
class Markets:
    def __init__(self, token):
        self.token = token
        self.instruments = dict()
        self.candle_intervals = [CandleInterval.CANDLE_INTERVAL_5_MIN,
                                 CandleInterval.CANDLE_INTERVAL_15_MIN,
                                 CandleInterval.CANDLE_INTERVAL_HOUR,
                                 CandleInterval.CANDLE_INTERVAL_DAY]

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
                        if isinstance(value, tinkoff.invest.schemas.MoneyValue) | \
                           isinstance(value, tinkoff.invest.schemas.Quotation):
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
            self.collect_history_item(item)

    #сбор истории конкретной позиции по FIGI
    def collect_history_item(self, instrument):
        instrument_figi = instrument['figi']
        item_history = []
        for load_interval in self.candle_intervals:
            interval = intervals(load_interval)

            candle_history = []
            for start_interval in interval.deltas:
                candles = self.collect_candles(instrument_figi=instrument_figi,
                                               start=start_interval,
                                               delta=interval.delta,
                                               candle_interval=interval.interval)

                candle_history += candles
                print(start_interval)
            item_history.append(candle_history)
        self.to_pickle(item=item_history,
                       path='history/'+instrument_figi+'.picle')




    def collect_candles(self,instrument_figi, start, delta, candle_interval):
        while True:
            try:
                with Client(self.token) as client:
                    r = client.market_data.get_candles(
                        figi=instrument_figi,
                        from_=start,
                        to=start+delta,
                        interval=candle_interval  # см. utils.get_all_candles
                    )
                    time.sleep(1/10)
                    return create_df(r.candles)
            except RequestError as e:
                print(str(e))
                time.sleep(5)
                continue
            break


if __name__ == "__main__":
    market = Markets(token.main_ro)
    #market.update_markets()
    market.load_markets()
    market.collect_history_market()