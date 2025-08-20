{{
  config(
    materialized='table'
  )
}}

WITH base_data AS (
    SELECT 
        f.provider,
        f.city_name,
        f.forecast_date,
        f.load_date,
				ROUND(f.avg_temp::NUMERIC, 2) as forecast_temp,
        ROUND(a.avg_temp::NUMERIC, 2) as actual_temp,
        ROUND(ABS(f.avg_temp - a.avg_temp)::NUMERIC, 2) as temp_error,
        CASE 
            WHEN ABS(f.avg_temp - a.avg_temp) = 0 THEN 100
            ELSE ROUND(GREATEST(0, 100 - (ABS(f.avg_temp - a.avg_temp) * 10))::NUMERIC, 2)
        END as accuracy_percent,
        (f.forecast_date - f.load_date) as forecast_horizon_days
        
    FROM {{ ref('stg_ods__weather_forecast') }} f
    JOIN {{ ref('stg_ods__weather_actual') }} a
        ON f.city_name = a.city_name 
        AND f.forecast_date = a.load_date
        AND f.provider = a.provider
    WHERE (f.forecast_date - f.load_date) BETWEEN 1 AND 3
),

date_ranges AS (
    SELECT DISTINCT
        forecast_date,
        forecast_horizon_days,
        ROW_NUMBER() OVER (ORDER BY forecast_date DESC) as date_rank
    FROM base_data
    WHERE forecast_horizon_days IN (1, 2, 3)
),

recent_dates AS (
    SELECT 
        forecast_date,
        forecast_horizon_days,
        CASE 
            WHEN forecast_horizon_days = 1 THEN 'day_1'
            WHEN forecast_horizon_days = 2 THEN 'day_2' 
            WHEN forecast_horizon_days = 3 THEN 'day_3'
        END as day_label
    FROM date_ranges 
    WHERE date_rank <= 3
),

pivot_prep AS (
    SELECT 
        bd.provider,
        bd.city_name,
        rd.day_label,
        rd.forecast_date,
        AVG(bd.actual_temp) as actual_temp,
        AVG(bd.forecast_temp) as forecast_temp,
        AVG(bd.accuracy_percent) as accuracy_percent
    FROM base_data bd
    JOIN recent_dates rd ON bd.forecast_date = rd.forecast_date 
                         AND bd.forecast_horizon_days = rd.forecast_horizon_days
    GROUP BY bd.provider, bd.city_name, rd.day_label, rd.forecast_date
)

SELECT 
		city_name,
    CASE
    	WHEN provider = 'openweathermap' THEN 'OpenWeatherMap'
      ELSE 'WeatherBit'
    END AS provider,
    
    MAX(CASE WHEN day_label = 'day_3' THEN 
        ROUND(actual_temp, 1) || ' (' || ROUND(forecast_temp, 1) || ') ' || ROUND(accuracy_percent, 0) || '%'
    END) as three_days_ago,

		MAX(CASE WHEN day_label = 'day_2' THEN 
        ROUND(actual_temp, 1) || ' (' || ROUND(forecast_temp, 1) || ') ' || ROUND(accuracy_percent, 0) || '%'
    END) as two_days_ago,
    
    MAX(CASE WHEN day_label = 'day_1' THEN 
        ROUND(actual_temp, 1) || ' (' || ROUND(forecast_temp, 1) || ') ' || ROUND(accuracy_percent, 0) || '%'
    END) as one_day_ago,
    
    ROUND(AVG(accuracy_percent), 0) || '%' as overall_accuracy
FROM pivot_prep
GROUP BY provider, city_name
ORDER BY provider, city_name