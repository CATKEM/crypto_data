import logging
import os
import time

import fire
import pandas as pd

from binance import Binance
from fetcher import Fetcher
from util import update_data, send_dingding_msg
from config import DATA_DIR

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


class Main:
    def __init__(self, use_proxy=False):
        self.binance = Binance(use_proxy)

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
                        df = exchange.fetch_candle(row['symbol'], date, timeframe)
                        break
                    except:
                        logging.error(f"Failed to fetch {date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                        time.sleep(1)
                if df is None:
                    fails.append(f"{date.strftime('%Y%m%d')} {row['symbol']} {row['type']}")
                else:
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


fire.Fire(Main)
