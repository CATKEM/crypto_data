import glob
import logging
import os

import pandas as pd

from config import DATA_DIR
from util import update_data

def convert_csv_to_hdf(exchange, symbol_type, begin_date, end_date, skiprows):
    begin_date, end_date = pd.to_datetime(begin_date), pd.to_datetime(end_date)
    dates = os.listdir(os.path.join(DATA_DIR, exchange, symbol_type))
    dates = sorted(d for d in dates if begin_date <= pd.to_datetime(d) <= end_date and not d.endswith('.hdf'))
    for date in dates:
        logging.info(f'Converting {date}')
        paths = glob.glob(os.path.join(DATA_DIR, exchange, symbol_type, date, '*.csv'))
        for path in paths:
            df = pd.read_csv(path, skiprows=skiprows)
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
            hdf_path = f'{path[:-4]}.hdf'
            update_data(hdf_path, df.set_index('candle_begin_time'))


def merge_hdfs(exchange, symbol_type):
    dates = os.listdir(os.path.join(DATA_DIR, exchange, symbol_type))
    dates = sorted(d for d in dates if not d.endswith('.hdf'))
    paths = glob.glob(os.path.join(DATA_DIR, exchange, symbol_type, '*', '*.hdf'))
    names = set(os.path.basename(p).split('.')[0] for p in paths)
    for name in names:
        logging.info(f'Merging {name}')
        paths = glob.glob(os.path.join(DATA_DIR, exchange, symbol_type, '*', f'{name}.hdf'))
        dfs = [pd.read_hdf(p) for p in paths]
        df = pd.concat(dfs).sort_index()
        df.to_hdf(os.path.join(DATA_DIR, exchange, symbol_type, f'{name}.hdf'), 'data')
