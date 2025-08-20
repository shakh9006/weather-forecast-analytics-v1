{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT city_name
FROM {{ ref('stg_ods__weather_actual') }}