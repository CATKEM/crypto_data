from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from config import PROXIES


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
        return df_symbol[['symbol', 'type']]

    @abstractmethod
    def fetch_candle(self, symbol: str, date: datetime, timeframe: str) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def max_candles_per_request(self):
        pass