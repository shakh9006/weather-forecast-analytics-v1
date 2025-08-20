# Weather Forecast Analytics Pipeline

A data engineering pipeline that extracts 3-day weather forecast data from multiple providers and analyzes forecast accuracy.

## Project Overview

This project implements a complete data pipeline that:

- Extracts weather forecast and actual weather data from multiple APIs
- Stores raw data in S3-compatible storage (MinIO)
- Processes and transforms data using dbt
- Analyzes forecast accuracy for different providers
- Visualizes results in Metabase dashboards

## Architecture

![Architecture Diagram](documentation/image-2.png.png)

The pipeline follows a modern data engineering architecture:

1. **Data Extraction**: Weather data is pulled from OpenWeatherMap and WeatherBit APIs
2. **Raw Storage**: Data is stored in MinIO (S3-compatible storage)
3. **Data Warehouse**: PostgreSQL serves as the main data warehouse
4. **Transformation**: dbt handles data modeling and transformations
5. **Orchestration**: Apache Airflow manages the entire pipeline
6. **Visualization**: Metabase provides analytics dashboards

## Technologies Used

- **Orchestration**: Apache Airflow
- **Data Warehouse**: PostgreSQL
- **Object Storage**: MinIO (S3-compatible)
- **Data Transformation**: dbt (data build tool)
- **Visualization**: Metabase
- **Containerization**: Docker & Docker Compose
- **Language**: Python
- **Weather APIs**: OpenWeatherMap, WeatherBit

## Data Models

### Staging Layer (ODS)

- `stg_ods__weather_forecast`: Cleaned weather forecast data
- `stg_ods__weather_actual`: Cleaned actual weather data

### Mart Layer

- `dim_cities`: City dimension table
- `dim_locations`: Location dimension table
- `forecast_accuracy`: Main accuracy analysis table
- `provider_accuracy_trend`: Provider performance trends

## Forecast Accuracy Analysis

The pipeline analyzes forecast accuracy by comparing predicted vs actual temperatures for:

- 1-day ahead forecasts
- 2-day ahead forecasts
- 3-day ahead forecasts

Accuracy is calculated as: `100 - (|forecast_temp - actual_temp| * 10)`

## Sample Results

![Forecast Accuracy Results](documentation/image-1.png.png)

The analysis shows forecast accuracy for different cities and providers over the last 3 days, helping to identify which weather services provide the most reliable predictions.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+

### Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd weather-forecast-analytics-v1
```

2. Create environment file:

```bash
cp env.example .env
# Edit .env with your API keys
```

3. Start the services:

```bash
docker-compose up -d
```

4. Access the services:

- Airflow UI: http://localhost:8080 (airflow/airflow)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Metabase: http://localhost:3000
- PostgreSQL: localhost:5432 (postgres/postgres)

### Running the Pipeline

1. Initialize the database:

```bash
# Run the init_database DAG in Airflow
```

2. Extract data:

```bash
# Run the extract_raw_from_api_to_s3 DAG
```

3. Load and transform:

```bash
# Run the load_raw_from_s3_to_pg DAG
# Run the transform_with_dbt DAG
```

## Project Structure

```
weather-forecast-analytics-v1/
├── dags/                          # Airflow DAGs
├── dbt/weather_forecast_analytics/ # dbt models and configurations
├── internal/                      # Custom Python modules
│   ├── database/                  # Database connections
│   ├── scripts/                   # Data processing scripts
│   └── project_config/            # Configuration files
├── config/                        # Airflow configuration
├── docker-compose.yaml            # Docker services definition
└── requirements.txt               # Python dependencies
```

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
