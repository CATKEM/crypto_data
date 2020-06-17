from datetime import timedelta, datetime

import ccxt
import pandas as pd
import pytz

from fetcher import Fetcher

COLUMN_NAMS = [
    'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'candle_close_time', 'quote_volume', 'trade_num',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', '_'
]

SECONDS_PER_DAY = 3600 * 24


class Binance(Fetcher):
    def __init__(self, use_proxy):
        super().__init__(ccxt.binance, use_proxy)

    def fetch_candle(self, symbol: str, date: datetime, timeframe: str) -> pd.DataFrame:
        last_end_time = date.replace(hour=0, minute=0, second=0)
        dfs = []
        while last_end_time < date + timedelta(hours=24):
            data = self.exchange.publicGetKlines({
                'symbol': symbol.replace('/', ''),
                'interval': timeframe,
                'startTime': int(last_end_time.replace(tzinfo=pytz.utc).timestamp()) * 1000,
                'limit': self.max_candles_per_request
            })
            df = pd.DataFrame(data, columns=COLUMN_NAMS).drop(columns='_')
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
            df['candle_close_time'] = pd.to_datetime(df['candle_close_time'], unit='ms')

            dfs.append(df)
            last_end_time = df.iloc[-1]['candle_begin_time']
        df = pd.concat(dfs).drop_duplicates('candle_begin_time')
        df = df[df['candle_begin_time'] < date + timedelta(hours=24)]
        return df

    @property
    def max_candles_per_request(self):
        return 1000

