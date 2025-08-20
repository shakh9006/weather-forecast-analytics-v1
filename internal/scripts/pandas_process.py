import io
import sys

import logging
import pandas as pd

from datetime import datetime, timedelta

sys.path.append("/opt/airflow/internal")

from database.default import connect_to_db, close_connection
from scripts.to_celsius import kelvin_to_celsius
from scripts.storage.get_countries import get_country_by_location, get_country_by_city
from project_config.config import ODS_SCHEMA

def process_data_with_pandas(data):
    logging.info(f"Processing {len(data)} weather data with pandas")

    processed_data = []

    for item in data:
        processed_data.append(item)

    df = pd.DataFrame(processed_data)

    logging.info(f"DataFrame created with shape: {df.shape}")

    return df
    
def parse_openweathermap_forecast(parquet_data: io.BytesIO, start_date: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_data)
    logging.info(f"Clearing OpenWeatherMap forecast from {df.shape[0]} rows n/a")

    daily_stats_list = []

    for _, row in df.iterrows():
        coord = row['city']['coord']
        city = row['city']['name']

        country = get_country_by_location(coord['lat'], coord['lon'])

        if not country:
            country = get_country_by_city(city)

        if not country:
            continue

        raw_data = row['list']
        weather_list = pd.DataFrame(raw_data.tolist())
        weather_list = weather_list[['main', 'wind', 'dt_txt']]

        main_df = weather_list["main"].apply(pd.Series)
        wind_df = weather_list["wind"].apply(pd.Series)

        weather_df = pd.concat([weather_list.drop(columns=["main", "wind"]), main_df, wind_df], axis=1)

        weather_df["dt_txt"] = pd.to_datetime(weather_df["dt_txt"])
        weather_df["forecast_date"] = weather_df["dt_txt"].dt.strftime("%Y-%m-%d")

        daily_stats = weather_df.groupby("forecast_date").agg(
            avg_temp=("temp", "mean"),
            min_temp=("temp_min", "min"),
            max_temp=("temp_max", "max"),
            wind_speed=("speed", "mean"),
            humidity=("humidity", "mean")
        ).reset_index()

        daily_stats['avg_temp'] = kelvin_to_celsius(daily_stats['avg_temp'])
        daily_stats['min_temp'] = kelvin_to_celsius(daily_stats['min_temp'])
        daily_stats['max_temp'] = kelvin_to_celsius(daily_stats['max_temp'])
        daily_stats['load_date'] = start_date
        daily_stats['provider'] = 'openweathermap'
        daily_stats['country_name'] = country['country_name']
        daily_stats['city_name'] = country['city']
        daily_stats['latitude'] = country['latitude']
        daily_stats['longitude'] = country['longitude']

        daily_stats_list.append(daily_stats)

    logging.info(f"Daily stats list: {len(daily_stats_list)}")
    if daily_stats_list:
        dead_line = pd.to_datetime(start_date) + timedelta(days=3)
        combined_df = pd.concat(daily_stats_list, ignore_index=True)
        combined_df = combined_df[(combined_df['load_date'] != combined_df['forecast_date']) & (combined_df['forecast_date'] <= dead_line.strftime("%Y-%m-%d"))]
        combined_df.reset_index(drop=True, inplace=True)
        logging.info(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df
    else:
        logging.warning("No daily stats data to process")
        return pd.DataFrame()

def parse_openweathermap_current_weather(parquet_data: io.BytesIO, start_date: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_data)
    logging.info(f"Clearing OpenWeatherMap forecast from {df.shape[0]} rows n/a")

    daily_stats_list = []

    for _, row in df.iterrows():
        coord = row['coord']

        country = get_country_by_location(coord['lat'], coord['lon'])

        if not country:
            country = get_country_by_city(row['name'])
        
        if not country:
            continue

        daily_stats = pd.Series(dtype=object)

        daily_stats['avg_temp'] = kelvin_to_celsius(row['main']['temp'])
        daily_stats['min_temp'] = kelvin_to_celsius(row['main']['temp_min'])
        daily_stats['max_temp'] = kelvin_to_celsius(row['main']['temp_max'])
        daily_stats['wind_speed'] = row['wind']['speed']
        daily_stats['humidity'] = row['main']['humidity']
        daily_stats['load_date'] = start_date
        daily_stats['provider'] = 'openweathermap'
        daily_stats['country_name'] = country['country_name']
        daily_stats['city_name'] = country['city']
        daily_stats['latitude'] = country['latitude']
        daily_stats['longitude'] = country['longitude']

        daily_stats_list.append(daily_stats)

    logging.info(f"Daily stats list: {len(daily_stats_list)}")

    if daily_stats_list:
        combined_df = pd.DataFrame(daily_stats_list)
        logging.info(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df
    else:
        logging.warning("No daily stats data to process")
        return pd.DataFrame()

    
def parse_weatherbit_forecast(parquet_data: io.BytesIO, start_date: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_data)
    logging.info(f"Clearing WeatherBit forecast from {df.shape[0]} rows n/a")

    daily_stats_list = []

    for _, row in df.iterrows():
        city = row['city_name']
        lat = row['lat']
        lon = row['lon']

        country = get_country_by_location(lat, lon)
        if not country:
            country = get_country_by_city(city)

        if not country:
            continue

        raw_data = row['data']
        weather_list = pd.DataFrame(raw_data.tolist())
        weather_list = weather_list[['max_temp', 'min_temp', 'temp', 'datetime', 'wind_spd', 'rh']].rename(columns={
            'temp': 'avg_temp',
            'datetime': 'forecast_date',
            'wind_spd': 'wind_speed',
            'rh': 'humidity'
        })

        weather_list["forecast_date"] = pd.to_datetime(weather_list["forecast_date"]).dt.strftime("%Y-%m-%d")    
        weather_list['country_name'] = country['country_name']
        weather_list['city_name'] = country['city']
        weather_list['latitude'] = country['latitude']
        weather_list['longitude'] = country['longitude']
        weather_list['load_date'] = start_date
        weather_list['provider'] = 'weatherbit'

        daily_stats_list.append(weather_list)

    logging.info(f"Daily stats list: {len(daily_stats_list)}")
    if daily_stats_list:
        dead_line = pd.to_datetime(start_date) + timedelta(days=3)
        combined_df = pd.concat(daily_stats_list, ignore_index=True)
        combined_df = combined_df[(combined_df['load_date'] != combined_df['forecast_date']) & (combined_df['forecast_date'] <= dead_line.strftime("%Y-%m-%d"))]
        combined_df.reset_index(drop=True, inplace=True)
        logging.info(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df
    else:
        logging.warning("No daily stats data to process")
        return pd.DataFrame()
    
def parse_weatherbit_current_weather(parquet_data: io.BytesIO, start_date: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_data)
    logging.info(f"Clearing WeatherBit current weather from {df.shape[0]} rows n/a")

    daily_stats_list = []

    for _, row in df.iterrows():
        city = row['city_name']
        lat = row['lat']
        lon = row['lon']

        country = get_country_by_location(lat, lon)

        if not country:
            country = get_country_by_city(city)

        if not country:
            continue

        raw_data = row['data']
        weather_list = pd.DataFrame(raw_data.tolist())
        weather_list = weather_list[['max_temp', 'min_temp', 'temp', 'wind_spd', 'rh']].rename(columns={
            'temp': 'avg_temp',
            'wind_spd': 'wind_speed',
            'rh': 'humidity'
        })

        weather_list['country_name'] = country['country_name']
        weather_list['city_name'] = country['city']
        weather_list['latitude'] = country['latitude']
        weather_list['longitude'] = country['longitude']
        weather_list['load_date'] = start_date
        weather_list['provider'] = 'weatherbit'

        daily_stats_list.append(weather_list)

    logging.info(f"Daily stats list: {len(daily_stats_list)}")
    if daily_stats_list:
        combined_df = pd.concat(daily_stats_list, ignore_index=True)
        logging.info(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df
    else:
        logging.warning("No daily stats data to process")
        return pd.DataFrame()

def load_forceast_to_postgres(df: pd.DataFrame) -> None:
    logging.info(f"Loading {df.shape[0]} weather forecast to database")
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {ODS_SCHEMA}.weather_forecast")
        existing_data = cursor.fetchall()
        logging.info(f"Found {len(existing_data)} existing records in database")
                
        filtered_data = None

        if not existing_data:
            filtered_data = df
        else:
            existing_data = pd.DataFrame(existing_data, columns=["forecast_id", "provider", "city_name", "country_name", "latitude", "longitude", "avg_temp", "min_temp", "max_temp", "wind_speed", "humidity", "forecast_date", "load_date", "created_at"])
            logging.info(f"Existing data shape: {existing_data.shape}")
            
            existing_data['load_date'] = pd.to_datetime(existing_data['load_date']).dt.strftime("%Y-%m-%d")
            existing_data['forecast_date'] = pd.to_datetime(existing_data['forecast_date']).dt.strftime("%Y-%m-%d")
            
            df_normalized = df.copy()
            df_normalized['load_date'] = pd.to_datetime(df_normalized['load_date']).dt.strftime("%Y-%m-%d")
            df_normalized['forecast_date'] = pd.to_datetime(df_normalized['forecast_date']).dt.strftime("%Y-%m-%d")
            
            existing_keys = existing_data[["city_name", "provider", "load_date", "forecast_date"]].copy()
            existing_keys['exists'] = True
            
            merged = df_normalized.merge(
                existing_keys,
                on=["city_name", "provider", "load_date", "forecast_date"],
                how="left"
            )
            
            logging.info(f"Merged data shape: {merged.shape}")
            logging.info(f"Records with 'exists' flag: {merged['exists'].notna().sum()}")
            logging.info(f"Records without 'exists' flag: {merged['exists'].isna().sum()}")
            
            filtered_data = merged[merged['exists'].isna()].drop(columns=['exists'])
            logging.info(f"Filtered out {len(df_normalized) - len(filtered_data)} duplicate records")

        if not filtered_data.empty:
            filtered_data["load_date"] = pd.to_datetime(filtered_data["load_date"]).dt.date
            filtered_data["forecast_date"] = pd.to_datetime(filtered_data["forecast_date"]).dt.date

            cols = [
                "city_name", "load_date", "forecast_date",
                "avg_temp", "min_temp", "max_temp",
                "wind_speed", "humidity", "latitude", "longitude",
                "provider", "country_name"
            ]

            records = filtered_data[cols].values.tolist()

            insert_query = f"""
                INSERT INTO {ODS_SCHEMA}.weather_forecast
                (city_name, load_date, forecast_date, avg_temp, min_temp, max_temp,
                wind_speed, humidity, latitude, longitude, provider, country_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, records)
            conn.commit()
            logging.info(f"Successfully loaded {len(filtered_data)} rows to database")
        else:
            logging.info("No data to load")

        return None
    except Exception as e:
        logging.error(f"Error loading weather forecast to database: {e}")
        return None
    finally:
        close_connection(conn)

def load_current_weather_to_postgres(df: pd.DataFrame) -> None:
    logging.info(f"Loading {df.shape[0]} weather current to database")
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {ODS_SCHEMA}.weather_actual")
        existing_data = cursor.fetchall()
        logging.info(f"Found {len(existing_data)} existing records in database")
                
        filtered_data = None

        if not existing_data:
            filtered_data = df
        else:
            existing_data = pd.DataFrame(existing_data, columns=["actual_id", "provider", "city_name", "country_name", "latitude", "longitude", "avg_temp", "min_temp", "max_temp", "wind_speed", "humidity", "load_date", "created_at"])
            logging.info(f"Existing data shape: {existing_data.shape}")
            
            existing_data['load_date'] = pd.to_datetime(existing_data['load_date']).dt.strftime("%Y-%m-%d")
            
            df_normalized = df.copy()
            df_normalized['load_date'] = pd.to_datetime(df_normalized['load_date']).dt.strftime("%Y-%m-%d")
            
            existing_keys = existing_data[["city_name", "provider", "load_date"]].copy()
            existing_keys['exists'] = True
            
            merged = df_normalized.merge(
                existing_keys,
                on=["city_name", "provider", "load_date"],
                how="left"
            )
            
            logging.info(f"Merged data shape: {merged.shape}")
            logging.info(f"Records with 'exists' flag: {merged['exists'].notna().sum()}")
            logging.info(f"Records without 'exists' flag: {merged['exists'].isna().sum()}")
            
            filtered_data = merged[merged['exists'].isna()].drop(columns=['exists'])
            logging.info(f"Filtered out {len(df_normalized) - len(filtered_data)} duplicate records")

        if not filtered_data.empty:
            filtered_data["load_date"] = pd.to_datetime(filtered_data["load_date"]).dt.date
            cols = [
                "city_name", "load_date",
                "avg_temp", "min_temp", "max_temp",
                "wind_speed", "humidity", "latitude", "longitude",
                "provider", "country_name"
            ]

            records = filtered_data[cols].values.tolist()

            insert_query = f"""
                INSERT INTO {ODS_SCHEMA}.weather_actual
                (city_name, load_date, avg_temp, min_temp, max_temp,
                wind_speed, humidity, latitude, longitude, provider, country_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, records)
            conn.commit()
            logging.info(f"Successfully loaded {len(filtered_data)} rows to database")
        else:
            logging.info("No data to load")

        return None
    except Exception as e:
        logging.error(f"Error loading weather current to database: {e}")
        return None
    finally:
        close_connection(conn)