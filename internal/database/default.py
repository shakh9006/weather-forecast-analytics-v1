import logging
import psycopg2

from project_config.config import POSTGRES_DWH_CONN_ID, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

def connect_to_db():
    logging.info("Connecting to database...")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_DWH_CONN_ID,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        logging.info("Connected to database successfully")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise e
    
def create_schemas(conn):
    logging.info("Creating schemas if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS ods;
        """)
        conn.commit()
        logging.info("Schemas created successfully")
    except psycopg2.Error as e:
        logging.error(f"Error occured while creating schemas: {e}")
        raise e

def create_tables(conn):
    logging.info("Creating tables if not exists...")

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ods.weather_forecast (
              forecast_id SERIAL PRIMARY KEY,
              provider VARCHAR(255) NOT NULL,
              city_name VARCHAR(255) NOT NULL,
              country_name VARCHAR(255) NOT NULL,
              latitude FLOAT NOT NULL,
              longitude FLOAT NOT NULL,
              avg_temp FLOAT NOT NULL,
              min_temp FLOAT NOT NULL,
              max_temp FLOAT NOT NULL,
              wind_speed FLOAT NOT NULL,
              humidity FLOAT NOT NULL,
              forecast_date DATE NOT NULL,
              load_date DATE NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
                       
            CREATE TABLE IF NOT EXISTS ods.weather_actual (
              actual_id SERIAL PRIMARY KEY,
              provider VARCHAR(255) NOT NULL,
              city_name VARCHAR(255) NOT NULL,
              country_name VARCHAR(255) NOT NULL,
              latitude FLOAT NOT NULL,
              longitude FLOAT NOT NULL,
              avg_temp FLOAT NOT NULL,
              min_temp FLOAT NOT NULL,
              max_temp FLOAT NOT NULL,
              wind_speed FLOAT NOT NULL,
              humidity FLOAT NOT NULL,
              load_date DATE NOT NULL,    
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        logging.info("Tables created successfully")
    except psycopg2.Error as e:
        logging.error(f"Error occured while creating tables: {e}")
        raise e

def close_connection(conn):
    if conn:
        conn.close()
        logging.info("Connection closed successfully")

def main():
    conn = connect_to_db()
    create_schemas(conn)
    create_tables(conn)
    close_connection(conn)

if __name__ == "__main__":
    main()