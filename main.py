from datetime import datetime, date
from alpha_vantage.timeseries import TimeSeries
from utilits.enums import TimeSeriesInterval, SettingKeys
from utilits import csv_utilits as csv, db_utilits as db
import os

# Чтение API ключа Alpha Vantage из окружающей переменной
alphavantage_key = os.environ.get('ALPHAVANTAGE_KEY', "3W9T6S0SUX2HUMJB")

time_series = TimeSeries(key=alphavantage_key, output_format='csv')

def download_time_series(interval):
    # Получение списка валют
    settings = db.read_settings()
    symbols = settings[SettingKeys.SYMBOLS.value].split(",")
    print(f'symbols: {symbols}')

    for symbol in symbols:
        data, meta_data = [], None

        if interval == TimeSeriesInterval.INTRADAY:
            print(f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min interval')
            data, meta_data = time_series.get_intraday(symbol,
                                                       interval=f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min')

            # Поскольку Alpha Vantage API не позволяет указать конкретный день,
            # здесь вы можете дополнительно фильтровать данные за последний день
            data = filter_dates(data)

        if interval == TimeSeriesInterval.MONTHLY:
            data, meta_data = time_series.get_monthly(symbol)

        csv_file = "data.csv"

        csv.download(csv_file, data, symbol)

        if settings[SettingKeys.OBJECT_STORAGE.value].lower() == 'hdfs':
            csv.save_to_hdfs(csv_file, symbol)
        else:
            csv.save_to_s3(csv_file, symbol)

def filter_dates(data):
    result = []
    for idx, row in enumerate(data):
        if idx == 0:
            result.append(row)
        else:
            date_object = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').date()
            today = date.today()
            delta = today - date_object
            if delta.days == 1:
                result.append(row)

    return result

def main():
    time_series_interval = TimeSeriesInterval.INTRADAY
    try:
        time_series_interval = TimeSeriesInterval[db.read_settings()["time_series_interval"].upper()]
    except KeyError:
        db.set_variable("time_series_interval", "INTRADAY")

    if time_series_interval == TimeSeriesInterval.INTRADAY:
        download_time_series(TimeSeriesInterval.INTRADAY)
    else:
        download_time_series(TimeSeriesInterval.MONTHLY)

if __name__ == "__main__":
    main()
