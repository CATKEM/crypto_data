import glob
import logging
import os
import time

import fire
import pandas as pd

from binance import Binance
from config import DATA_DIR
from fetcher import Fetcher
from okex import Okex
from util import send_dingding_msg, update_data

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


class Main:
    def __init__(self, use_proxy=False):
        self.binance = Binance(use_proxy)
        self.okex = Okex(use_proxy)

    def fetch(self,
              exchange_name,
              timeframe,
              begin_date,
              end_date=None,
              base_currencies=None,
              quote_currencies=None,
              symbol_types=None):
        exchange: Fetcher = getattr(self, exchange_name)
        symbols = exchange.get_symbols(base_currencies, quote_currencies, symbol_types)
        if end_date is None:
            end_date = begin_date
        begin_date, end_date = str(begin_date), str(end_date)
        dates = pd.date_range(begin_date, end_date)
        fails = []
        for date in dates:
            logging.info(f'Fetching {date.strftime("%Y%m%d")}')
            for _, row in symbols.iterrows():
                df = None
                for _ in range(3):
                    try:
                        df = exchange.fetch_candle(row['id'], date, timeframe, row['type'])
                        break
                    except:
                        logging.error(f"Failed to fetch {date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                        time.sleep(1)
                if df is None:
                    fails.append(f"{date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                elif not df.empty:
                    hdf_dir = os.path.join(DATA_DIR, exchange_name, row['type'], date.strftime('%Y%m%d'))
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
            msg = f'Successfully fetched {exchange_name} {begin_date} {end_date} {timeframe} data'
        send_dingding_msg(msg)

    def convert_csv_to_hdf(self, exchange, symbol_type, begin_date, end_date=None, skiprows=0):
        if end_date is None:
            end_date = begin_date
        begin_date, end_date = pd.to_datetime(str(begin_date)), pd.to_datetime(str(end_date))
        dates = os.listdir(os.path.join(DATA_DIR, exchange, symbol_type))
        dates = sorted(d for d in dates if begin_date <= pd.to_datetime(d) <= end_date)
        for date in dates:
            logging.info(f'Converting {date}')
            paths = glob.glob(os.path.join(DATA_DIR, exchange, symbol_type, date, '*.csv'))
            for path in paths:
                df = pd.read_csv(path, skiprows=skiprows)
                df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
                hdf_path = f'{path[:-4]}.hdf'
                update_data(hdf_path, df.set_index('candle_begin_time'))

fire.Fire(Main)
