select * 
from {{ source('ods', 'weather_forecast') }}