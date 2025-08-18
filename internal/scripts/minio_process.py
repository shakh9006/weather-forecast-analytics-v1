import io
import sys
import logging
import pandas as pd

from typing import Optional
from minio import Minio

sys.path.append("/opt/airflow/internal")

from project_config.config import (
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE
)

def get_minio_client():
    logging.info(f"Getting minio client for {MINIO_ENDPOINT}")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def save_to_minio(df: pd.DataFrame, start_date: str, end_date: str, provider_name: str, prefix: str) -> Optional[str]:
    logging.info(f"Saving DataFrame to MinIO for date: {start_date} to {end_date}")

    try:
        client = get_minio_client()

        if not client:
            logging.error('minIO client is not found')

        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logging.info(f"Created bucket {MINIO_BUCKET}")

        file_name = f"{provider_name}_{start_date}_{end_date}_00-00-00.gz.parquet"
        object_path = f"raw/{prefix}/{file_name}"

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        client.put_object(
            MINIO_BUCKET,
            object_path,
            parquet_buffer,
            length=len(parquet_buffer.getvalue()),
            content_type='application/octet-stream'
        )

        logging.info(f"Successfully saved {object_path} to MinIO")
        return object_path
    except Exception as e:
        logging.error(f"Error occurred while saving DataFrame to MinIO: {e}")
        return None
    
def read_from_minio(object_path: str, provider_name: str, prefix: str) -> Optional[io.BytesIO]:
    logging.info(f"Reading data for {provider_name} and {prefix} from MinIO for file: {object_path}")

    try:
        client = get_minio_client()

        response = client.get_object(MINIO_BUCKET, object_path)

        logging.info(f"Response: {response}")

        if not response:
            logging.error(f"File {object_path} not found for {provider_name} and {prefix}")
            return None

        parquet_data = io.BytesIO(response.read())

        logging.info(f"Successfully loaded from {object_path} for {provider_name} and {prefix}")
        
        return parquet_data
    except Exception as e:
        logging.error(f"Error occurred while reading from MinIO for {provider_name} and {prefix}: {e}")
        return None