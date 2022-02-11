import pandas as pd
import requests
import zipfile
import os
from math import radians, cos, sin, asin, sqrt
from typing import Tuple, Text

WEATHER_STATION_PATH = 'data_samples/Weather_stations_loc.xlsx'
HISTORICAL_POWER_DATA_PATH = 'data_samples/hist_data_both.xlsx'
TMP_DATA_PATH = 'tmp/'


class RESMeasuringPoint:
    """
    This class is data preprocessor to prepare filled Dataframe ready to be stored in Database.

    """

    def __init__(self, name: Text, installed_power: float, cords: Tuple[float, float], hist_data_path, res_type: Text):
        """
        :param name: Name of measuring point or serial number of meter or any other unique key.
        :param installed_power: Value of measuring point installed power in kW.
        :param cords: Coordinates of the RES installation.
        :param hist_data_path: Path to data of the historical Power Generation of RES installation.
        Data should be provided as .xlsx file with 2 columns with headers where:
        - first column contains datatime of samples instances in order of occurrence
        - second column contains power level samples registered in that time in kW
        :param res_type: 'wind' for wind turbines or 'pv' for solar plants.
        """

        self.__name = name
        self.__installed_power = installed_power
        self.__hist_data_path = hist_data_path
        self.__hist_df = self.hist_df_parser()
        self.__lon = cords[0]
        self.__lat = cords[1]
        self.__meteo_code = self.find_nearest_meteo_station()
        self.__weather = self.download_hist_weather_data()
        self.res_type = res_type

    @property
    def res_type(self):
        return self.__res_type

    @res_type.setter
    def res_type(self, res_type):
        if res_type not in ('wind', 'pv'):
            raise ValueError()
        self.__res_type = res_type

    def data_collector(self):
        """
        Prepare Dataframe ready to be stored in database.

        Example of Dataframe first rows will look like this:
    ___________________________________________________________________________________________________________
    |sample time | power lvl | clouds | wind speed | wind dir | sun | temp | RES type | name | installed power|
    -----------------------------------------------------------------------------------------------------------
    |            |           |        |           |          |     |      |          |       |                |
    -----------------------------------------------------------------------------------------------------------
        """
        sampling_period = self.__hist_df['datetime'].iloc[0] - self.__hist_df['datetime'].iloc[1]
        weather_df = self.__weather.resample(str(int(sampling_period.total_seconds())) + 'S').interpolate()
        df_merged = pd.merge(self.__hist_df, weather_df, on='datetime')
        df_merged['RES type'] = self.res_type
        df_merged['name'] = self.__name
        df_merged['installed power'] = self.__installed_power
        return df_merged

    def find_nearest_meteo_station(self):
        """
        Fetches the nearest meteo station available in the IMGW database
        """
        df = pd.read_excel(WEATHER_STATION_PATH)
        nearest_ws = df.iloc[df.apply(lambda x: self.haversine(x['lat'], x['lon'], self.__lon, self.__lat),
                                      axis=1).idxmin()]
        return nearest_ws

    @staticmethod
    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance in kilometers between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r

    def download_hist_weather_data(self) -> pd.DataFrame:
        """
        Fetches historical weather data from IMGW web page
        """
        years = [year for year in range(self.__hist_df['datetime'].iloc[-1].year,
                                        self.__hist_df['datetime'].iloc[0].year)]

        dfs = []
        for year in years:
            url = (
                f'https://dane.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/{year}/'
                f'{year}_{self.__meteo_code[0]}_s.zip')

            r = requests.get(url, allow_redirects=True, stream=True)
            if r.ok:
                print(r.status_code)
            else:
                raise ValueError(f'Error getting data from IMGW: '
                                 f'Response Code {r.status_code}')

            with open(f'tmp/{year}_{self.__meteo_code[0]}_s.zip', 'wb') as file:
                file.write(r.content)

            with zipfile.ZipFile(f'tmp/{year}_{self.__meteo_code[0]}_s.zip', 'r') as zip_ref:
                zip_ref.extractall(TMP_DATA_PATH)

            df = pd.read_csv(f'tmp/s_t_{self.__meteo_code[0]}_{year}.csv', encoding='unicode_escape',
                             low_memory=False, header=None, infer_datetime_format=True,
                             parse_dates={'datetime': [2, 3, 4, 5]}, index_col=['datetime'])

            df = df.loc[:, [21, 23, 25, 29, 69]]
            df = df.rename(columns={21: 'clouds', 23: 'wind_dir', 25: 'wind_speed', 29: 'temp', 69: 'sun'})

            os.remove(f'tmp/{year}_{self.__meteo_code[0]}_s.zip')
            os.remove(f'tmp/s_t_{self.__meteo_code[0]}_{year}.csv')
            dfs.append(df)
        dfs = pd.concat(dfs)
        return dfs

    def hist_df_parser(self):
        hist_df = pd.read_excel(self.__hist_data_path, skiprows=1, names=['datetime', 'power'])
        hist_df['datetime'] = pd.to_datetime(hist_df['datetime'])
        return hist_df

    def get_weather(self):
        return self.__weather


point = RESMeasuringPoint('first', 5000.5, (53.007, 14.822), HISTORICAL_POWER_DATA_PATH, 'wind')
print(point.data_collector())


