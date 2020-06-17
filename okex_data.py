import logging
import os
import time

import ccxt
import pandas as pd
import retry

LIMIT_PER_DAY = {'5m': 288, '15m': 96}

PAGE_LIMITS = {'swap': 200, 'futures': 300, 'spot': 200}

PROXIES = {
    "http": "socks5h://127.0.0.1:1080",
    "https": "socks5h://127.0.0.1:1080"
}

SPOT_QUOTE_CURRENCIES = ['BTC', 'ETH', 'OKB', 'USDT']

COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


def get_okex():
    exchange = ccxt.okex3()
    exchange.proxies = PROXIES
    return exchange


@retry.retry(tries=3, backoff=2, delay=3)
def get_okex_data(symbol: str, start: int, end: int, timeframe_min: int, sym_type: str):
    exchange = get_okex()
    markets = []
    page_limit = PAGE_LIMITS[sym_type]
    method_name = sym_type + 'GetInstrumentsInstrumentIdCandles'
    getter = getattr(exchange, method_name)
    while start <= end:
        print(start, end)
        end_round = start + 60 * (page_limit - 1)
        markets.extend(getter({
            'instrument_id': symbol,
            'granularity': timeframe_min * 60,
            'start': pd.to_datetime(start, unit='s').strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end': pd.to_datetime(end_round, unit='s').strftime('%Y-%m-%dT%H:%M:%SZ')
        }))
        start = end_round + 60
    if len(markets[0]) == len(COLS):
        df_market = pd.DataFrame(markets, columns=COLS)
    else:
        df_market = pd.DataFrame(markets, columns=COLS + ['currency_volume'])
    df_market['timestamp'] = pd.to_datetime(df_market['timestamp'], format='%Y-%m-%dT%H:%M:%S.%fZ')
    df_market = df_market[df_market['timestamp'] <= pd.to_datetime(end, unit='s')]
    df_market.sort_values('timestamp', inplace=True)
    return df_market


def save_data(df, exchange, sym_type, sym, tf):
    date = df['timestamp'].min().strftime('%Y%m%d')
    fmt = '%Y%m%d-%H%M%S'
    sym = sym.replace('/', '-')
    dir_path = os.path.join('csv_data', exchange, sym_type, date)
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path,
                        f'{sym}_{tf}_{df["timestamp"].min().strftime(fmt)}_{df["timestamp"].max().strftime(fmt)}.csv')
    df.to_csv(path)


def main():
    exchange = get_okex()
    symbols = exchange.load_markets()
    df_symbol = pd.DataFrame(symbols).T
    main_currencies = set(df_symbol[(df_symbol['type'] == 'swap') | (df_symbol['type'] == 'futures')]['base'])
    symbols = df_symbol[
        ((df_symbol['type'] == 'spot') &
         df_symbol['quote'].isin(SPOT_QUOTE_CURRENCIES) & df_symbol['base'].isin(main_currencies)) |
        (df_symbol['type'] == 'swap') |
        (df_symbol['type'] == 'futures')
        ]
    for _, r in symbols.iterrows():
        symbol = r['id']
        sym_type = r['type']
        logging.info('Fetching Okex %s', symbol)
        df_market = get_okex_data(symbol, 1587081600, 1587106800, 1, sym_type)
        save_data(df_market, 'okex', sym_type, symbol, '1m')
        time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s',
                        level=logging.INFO,
                        datefmt='%Y%m%d %H:%M:%S')
    main()
