from datetime import timedelta, datetime

import ccxt
import pandas as pd
import pytz

from fetch.fetcher import Fetcher

COLUMN_NAMS = [
    'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'candle_close_time', 'quote_volume', 'trade_num',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', '_'
]

SECONDS_PER_DAY = 3600 * 24


class Binance(Fetcher):
    def __init__(self, use_proxy):
        super().__init__(ccxt.binance, use_proxy, 'binance')

    def request_candle(self, symbol: str, start_time: datetime, timeframe: str, symbol_type: str) -> pd.DataFrame:
        data = self.exchange.publicGetKlines({
            'symbol': symbol,
            'interval': timeframe,
            'startTime': int(start_time.replace(tzinfo=pytz.utc).timestamp()) * 1000,
            'limit': self.max_candles_per_request(symbol_type)
        })
        df = pd.DataFrame(data, columns=COLUMN_NAMS).drop(columns='_')
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
        df['candle_close_time'] = pd.to_datetime(df['candle_close_time'], unit='ms')
        return df

    def max_candles_per_request(self, symbol_type):
        return 1000
