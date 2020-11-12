import logging
import os
import time
from datetime import datetime, timedelta

import ccxt
import pandas as pd

from fetch.fetcher import Fetcher
from util import get_timeframe_delta

SPOT_COLUMNS = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']

FUTURES_COLUMNS = SPOT_COLUMNS + ['quote_volume']

MAX_CANDLES = {'spot': 200, 'futures': 300, 'swap': 200}


class Okex(Fetcher):
    def __init__(self, use_proxy):
        super().__init__(ccxt.okex, use_proxy, 'okex')

    def request_candle(self, symbol: str, start_time: datetime, timeframe: str, symbol_type: str) -> pd.DataFrame:
        columns = SPOT_COLUMNS if symbol_type == 'spot' else FUTURES_COLUMNS
        max_candles_req = self.max_candles_per_request(symbol_type)
        getter = getattr(self.exchange, f'{symbol_type}GetInstrumentsInstrumentIdHistoryCandles')
        timeframe_dlt = get_timeframe_delta(timeframe)
        end_time = start_time + timeframe_dlt * (max_candles_req - 1)
        data = getter({
            'instrument_id': symbol,
            'granularity': timeframe_dlt.seconds,
            'start': start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end': end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        })
        df = pd.DataFrame(data, columns=columns)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
        return df

    def max_candles_per_request(self, symbol_type):
        return MAX_CANDLES[symbol_type]
