import glob
import logging
import os
import time

import pkgutil
import importlib

import fire
import pandas as pd
import fetch
# import fetch.exchanges
from fetch import Fetcher

from config import DATA_DIR
from util import send_dingding_msg, update_data
from convert import convert_csv_to_hdf, merge_hdfs

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


class Main:
    @staticmethod
    def get_begin_end_dates(begin_date, end_date):
        if end_date is None:
            end_date = begin_date
        return str(begin_date), str(end_date)

    def list_exchanges(self):
        for info in pkgutil.iter_modules(fetch.exchanges.__path__):
            module = importlib.import_module('.' + info.name, fetch.exchanges.__name__)
            class_name = info.name.capitalize()
            if hasattr(module, class_name) and issubclass(getattr(module, class_name), Fetcher):
                print(info.name)

    def fetch(self,
              exchange_name,
              timeframe,
              begin_date,
              end_date=None,
              base_currencies=None,
              quote_currencies=None,
              symbol_types=None,
              use_proxy=False):
        begin_date, end_date = self.get_begin_end_dates(begin_date, end_date)
        module = importlib.import_module('.' + exchange_name, fetch.exchanges.__name__)
        exchange: Fetcher = getattr(module, exchange_name.capitalize())(use_proxy)
        exchange.fetch(timeframe, begin_date, end_date, base_currencies, quote_currencies, symbol_types)

    def convert_csv_to_hdf(self, exchange, symbol_type, begin_date, end_date=None, skiprows=0):
        begin_date, end_date = self.get_begin_end_dates(begin_date, end_date)
        convert_csv_to_hdf(exchange, symbol_type, begin_date, end_date, skiprows)

    def merge_hdfs(self, exchange, symbol_type):
        merge_hdfs(exchange, symbol_type)


fire.Fire(Main)
