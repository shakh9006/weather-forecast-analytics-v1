select * 
from {{ source('ods', 'weather_actual') }}