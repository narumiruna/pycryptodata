import ssl
import pandas as pd

ssl._create_default_https_context = ssl._create_unverified_context

URL = 'https://www.cryptodatadownload.com'


def _convert_dict(input_dict: dict) -> dict:
    output_dict = {}

    for _tuple, value in input_dict.items():
        for key in _tuple:
            output_dict[key] = value

    return output_dict


EXCHANGE = {
    'binance': 'Binance',
    'bitfinex': 'Bitfinex',
}

TIME_PERIOD = _convert_dict({
    ('1d', 'd', 'daily'): 'd',
    ('1h', 'h', 'hourly'): '1h',
    ('1m', 'm', 'minute'): 'minute',
})


def get_url(exchange: str, symbol: str, time_period: str):
    exchange = EXCHANGE[exchange.lower()]
    symbol = symbol.replace('/', '').upper()
    time_period = TIME_PERIOD[time_period.lower()]

    url = f'{URL}/cdd/{exchange}_{symbol}_{time_period}.csv'
    return url


def download_cryptodata(url: str) -> pd.DataFrame:
    df = pd.read_csv(url,
                     skiprows=1,
                     parse_dates=['date'],
                     date_parser=pd.to_datetime)
    df.drop_duplicates('date', keep='first', inplace=True)
    df.set_index('date', drop=True, inplace=True)
    df.sort_index(inplace=True)
    return df


def main():
    url = get_url('bitfinex', 'BTCUSD', '1h')
    df = download_cryptodata(url)
    print(df)


if __name__ == '__main__':
    main()
