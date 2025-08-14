import logging
import pandas as pd

def process_data_with_pandas(data):
    logging.info(f"Processing {len(data)} weather data with pandas")

    processed_data = []

    for item in data:
        processed_data.append(item)

    df = pd.DataFrame(processed_data)

    logging.info(f"DataFrame created with shape: {df.shape}")

    return df