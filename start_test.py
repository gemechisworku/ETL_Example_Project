import re
import numpy as np
import pandas as pd
from field_data_processor import FieldDataProcessor
from weather_data_processor import WeatherDataProcessor
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

config_params = {
    # Paste in your previous dictionary data in here
    "sql_query": """
            SELECT *
            FROM geographic_features
            LEFT JOIN weather_features USING (Field_ID)
            LEFT JOIN soil_and_crop_features USING (Field_ID)
            LEFT JOIN farm_management_features USING (Field_ID)
            """, # Insert your SQL query
    "db_path": 'sqlite:///Maji_Ndogo_farm_survey_small.db', # Insert the db_path of the database
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'}, # Insert the disctionary of columns we want to swop the names of, 
    "values_to_rename": {'cassaval': 'cassava', 'cassava ': 'cassava', 'wheatn': 'wheat', 'wheat ': 'wheat', 'teaa': 'tea', 'tea ': 'tea'}, # Insert the croptype renaming dictionary
    "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv", # Insert the weather data mapping CSV here
    
    # Add two new keys
    "weather_csv_path":  "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the URL for the weather station data
    "regex_patterns" :  {
        'Rainfall': r'(\d+(\.\d+)?)\s?mm',
         'Temperature': r'(\d+(\.\d+)?)\s?C',
        'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    }, # Insert the regex pattern we used to process the messages
} # Paste in your config_params dictionary here

field_processor = FieldDataProcessor(config_params)
field_processor.process()
field_df = field_processor.df

weather_processor = WeatherDataProcessor(config_params)
weather_processor.process()
weather_df = weather_processor.weather_df


weather_df.to_csv('sampled_weather_df.csv', index=False)
field_df.to_csv('sampled_field_df.csv', index=False)

!pytest validate_data.py -v

import os# Define the file paths
weather_csv_path = 'sampled_weather_df.csv'
field_csv_path = 'sampled_field_df.csv'

# Delete sampled_weather_df.csv if it exists
if os.path.exists(weather_csv_path):
    os.remove(weather_csv_path)
    print(f"Deleted {weather_csv_path}")
else:
    print(f"{weather_csv_path} does not exist.")

# Delete sampled_field_df.csv if it exists
if os.path.exists(field_csv_path):
    os.remove(field_csv_path)
    print(f"Deleted {field_csv_path}")
else:
    print(f"{field_csv_path} does not exist.")