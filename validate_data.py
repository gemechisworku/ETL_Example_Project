import pytest
import pandas as pd
from weather_data_processor import WeatherDataProcessor
from field_data_processor import FieldDataProcessor

@pytest.fixture(scope="module")
def data(request):
    config_params = {
        "sql_query": """
        SELECT *
        FROM geographic_features
        LEFT JOIN weather_features USING (Field_ID)
        LEFT JOIN soil_and_crop_features USING (Field_ID)
        LEFT JOIN farm_management_features USING (Field_ID)
        """,
        "db_path": 'sqlite:///Maji_Ndogo_farm_survey_small.db',
        "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'},
        "values_to_rename": {'cassaval': 'cassava', 'cassava ': 'cassava', 'wheatn': 'wheat', 'wheat ': 'wheat', 'teaa': 'tea', 'tea ': 'tea'}, # Insert the croptype renaming dictionary
        "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv",
        "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv",
        'regex_patterns': {
            'Rainfall': r'(\d+(\.\d+)?)\s?mm',
            'Temperature': r'(\d+(\.\d+)?)\s?C',
            'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
        }
    }
    weather_processor = WeatherDataProcessor(config_params)
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    weather_processor.process()
    field_df = field_processor.df
    weather_df = weather_processor.weather_df

    
    return weather_df, field_df

def test_read_weather_DataFrame_shape(data):
    weather_df, _ = data
    assert weather_df is not None
    assert weather_df.shape[0] > 0  # Ensure the DataFrame has rows

def test_read_field_DataFrame_shape(data):
    _, field_df = data
    assert field_df is not None
    assert field_df.shape[0] > 0  # Ensure the DataFrame has rows

def test_weather_DataFrame_columns(data):
    weather_df, _ = data
    actual_columns = weather_df.columns.tolist()
    expected_columns = ['Weather_station_ID', 'Message']
    assert all(col in actual_columns for col in expected_columns)

def test_field_DataFrame_columns(data):
    _, field_df = data
    actual_columns = field_df.columns.tolist()
    expected_columns = ['Field_ID', 'Elevation', 'Crop_type']  # Adjust expected columns according to your data
    assert all(col in actual_columns for col in expected_columns)

def test_field_DataFrame_non_negative_elevation(data):
    _, field_df = data
    negative_elevations = field_df[field_df['Elevation'] < 0]
    assert (field_df['Elevation'] >= 0).all()

def test_crop_types_are_valid(data):
    _, field_df = data
    valid_crop_types = ['cassava', 'wheat', 'tea', 'potato', 'maize', 'rice', 'banana', 'coffee']  # Adjust according to your valid crop types
    assert field_df['Crop_type'].isin(valid_crop_types).all()

def test_positive_rainfall_values(data):
    _, field_df = data
    assert (field_df['Rainfall'] >= 0).all()

