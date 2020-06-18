from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import pandas as pd

from config import PROXIES
from util import get_timeframe_delta


class Fetcher(ABC):
    def __init__(self, exchange_class, use_proxy):
        self.exchange = exchange_class()
        if use_proxy:
            self.exchange.proxies = PROXIES

    def get_symbols(self, base_currencies, quote_currencies, symbol_types):
        df_symbol = pd.DataFrame(self.exchange.load_markets()).T
        for col, filters in [('base', base_currencies), ('quote', quote_currencies), ('type', symbol_types)]:
            if filters is None:
                continue
            df_symbol = df_symbol[df_symbol[col].isin(filters)]
        return df_symbol

    def fetch_candle(self, symbol: str, date: datetime, timeframe: str, symbol_type: str) -> pd.DataFrame:
        dfs = []
        date = date.replace(hour=0, minute=0, second=0)
        start_time = date
        timeframe_dlt = get_timeframe_delta(timeframe)
        while start_time < date + timedelta(hours=24):
            df = self.request_candle(symbol, start_time, timeframe, symbol_type)
            if df.empty:
                # no more data
                break
            df.sort_values('candle_begin_time', inplace=True)
            start_time = df.iloc[-1]['candle_begin_time'] + timeframe_dlt
            dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        df = pd.concat(dfs).drop_duplicates('candle_begin_time')
        df = df[df['candle_begin_time'] < date + timedelta(hours=24)]
        return df

    @abstractmethod
    def request_candle(self, symbol: str, start_time: datetime, timeframe: str, symbol_type: str) -> pd.DataFrame:
        pass
