import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import pandas as pd

from config import DATA_DIR, PROXIES
from util import get_timeframe_delta, send_dingding_msg, update_data


class Fetcher(ABC):
    def __init__(self, exchange_class, use_proxy, name):
        self.exchange = exchange_class()
        self.name = name
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

    def fetch(self, timeframe, begin_date, end_date, base_currencies, quote_currencies, symbol_types):
        symbols = self.get_symbols(base_currencies, quote_currencies, symbol_types)
        dates = pd.date_range(begin_date, end_date)
        fails = []
        for date in dates:
            logging.info(f'Fetching {date.strftime("%Y%m%d")}')
            for _, row in symbols.iterrows():
                df = None
                for _ in range(3):
                    try:
                        df = self.fetch_candle(row['id'], date, timeframe, row['type'])
                        break
                    except:
                        logging.error(f"Failed to fetch {date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                        time.sleep(1)
                if df is None:
                    fails.append(f"{date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                elif not df.empty:
                    hdf_dir = os.path.join(DATA_DIR, self.name, row['type'], date.strftime('%Y%m%d'))
                    os.makedirs(hdf_dir, exist_ok=True)
                    hdf_path = os.path.join(hdf_dir, f'{row["symbol"].replace("/", "-")}_{timeframe}.hdf')
                    update_data(hdf_path, df.set_index('candle_begin_time'))
                time.sleep(1)
        if fails:
            if len(fails) <= 3:
                msg = f'Failed to fetch {", ".join(fails)}'
            else:
                msg = f'Failed to fetch {", ".join(fails[:3])},... +{len(fails) - 3} more'
        else:
            msg = f'Successfully fetched {self.name} {begin_date} {end_date} {timeframe} data'
        send_dingding_msg(msg)

    @abstractmethod
    def request_candle(self, symbol: str, start_time: datetime, timeframe: str, symbol_type: str) -> pd.DataFrame:
        pass
