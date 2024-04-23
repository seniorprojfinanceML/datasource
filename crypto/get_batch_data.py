from binance_historical_data import BinanceDataDumper
from datetime import date

data_dumper = BinanceDataDumper(
    path_dir_where_to_dump=".",
    asset_class="spot",  # spot, um, cm
    data_type="klines",  # aggTrades, klines, trades
    data_frequency="1m",
)

start_date = date(2024, 3, 20)

if __name__ == '__main__':
    data_dumper.dump_data(
    tickers=["ETHUSDT","BTCUSDT","BNBUSDT","ADAUSDT", "DOGEUSDT","AGIXUSDT"],
    date_start=None,
    date_end=None,
    is_to_update_existing=True,
    tickers_to_exclude=["UST"],
)