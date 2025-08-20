{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT provider
FROM {{ ref('stg_ods__weather_actual') }}