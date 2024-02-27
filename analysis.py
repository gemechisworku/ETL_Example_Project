from scipy.stats import ttest_ind
import numpy as np

# Now, the measurements_to_compare can directly use 'Temperature', 'Rainfall', and 'Pollution_level'
measurements_to_compare = ['Temperature', 'Rainfall', 'Pollution_level']

def filter_field_data(df, station_id, measurement):
    df = df[(df['Weather_station'] == station_id)][measurement]
    return df

def filter_weather_data(df, station_id, measurement):
    df = df[(df['Weather_station_ID'] == station_id) & (df['Measurement'] == measurement)]['Value']
    return df

def run_ttest(Column_A, Column_B):
    t_statistic, p_value = ttest_ind(Column_A, Column_B, equal_var=False)
    return t_statistic, p_value

def print_ttest_results(station_id, measurement, p_val, alpha):
    """
    Interprets and prints the results of a t-test based on the p-value.
    """
    if p_val <= alpha:
        print(f"   Significant difference in {measurement} detected at Station  {station_id}, (P-Value: {p_val:.5f} < {alpha}). Null hypothesis rejected.")
    else:
        print(f"   No significant difference in {measurement} detected at Station  {station_id}, (P-Value: {p_val:.5f} > {alpha}). Null hypothesis not rejected.")

def hypothesis_results(field_df, weather_df, list_measurements_to_compare, alpha = 0.05):
    unique_station_ids = sorted(weather_df['Weather_station_ID'].unique())
    for station_id in unique_station_ids:
        for measurement in measurements_to_compare:
            # Filter data for the specific station and measurement
            field_values = filter_field_data(field_df, station_id, measurement)
            weather_values = filter_weather_data(weather_df, station_id, measurement)
            
            # Perform t-test
            t_statistic, p_value = run_ttest(field_values, weather_values)
            
            # Print results
            print_ttest_results(station_id, measurement, p_value, alpha)
        print()