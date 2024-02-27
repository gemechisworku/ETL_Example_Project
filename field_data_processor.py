### START FUNCTION
"""
Module: FieldDataProcessor

This module contains the FieldDataProcessor class, which is responsible for processing field data.

The class provides methods to ingest data from a SQL database, rename columns, apply corrections to specific columns,
perform weather station mapping, and integrate weather station data into the field data.
"""
import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV

class FieldDataProcessor:
    """
    A class to process field data.

    Args:
        config_params (dict): A dictionary containing configuration parameters.
            It should include 'db_path' for the path to the database,
            'sql_query' for the SQL query to retrieve data,
            'columns_to_rename' for columns to be renamed,
            'values_to_rename' for values to be renamed,
            and 'weather_mapping_csv' for the path to the weather mapping CSV file.
        logging_level (str, optional): The logging level. Defaults to "INFO".

    Attributes:
        db_path (str): The path to the database.
        sql_query (str): The SQL query to retrieve data.
        columns_to_rename (dict): Dictionary specifying columns to be renamed.
        values_to_rename (dict): Dictionary specifying values to be renamed.
        weather_map_data (str): The path to the weather mapping CSV file.
        df (DataFrame): DataFrame to store field data.
        engine (Engine): Database engine object.
        logger (Logger): Logger object for logging messages.

    Methods:
        __init__: Initializes the FieldDataProcessor object.
        initialize_logging: Initializes logging configuration.
        ingest_sql_data: Retrieves data from the database.
        rename_columns: Renames specified columns.
        apply_corrections: Applies corrections to specified columns.
        weather_station_mapping: Reads weather mapping data from CSV.
        process: Executes data processing steps.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the FieldDataProcessor object.

        Args:
            config_params (dict): A dictionary containing configuration parameters.
                It should include 'db_path' for the path to the database,
                'sql_query' for the SQL query to retrieve data,
                'columns_to_rename' for columns to be renamed,
                'values_to_rename' for values to be renamed,
                and 'weather_mapping_csv' for the path to the weather mapping CSV file.
            logging_level (str, optional): The logging level. Defaults to "INFO".
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']
        self.df = None
        self.engine = None
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Initializes logging configuration.

        Args:
            logging_level (str): The logging level.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Retrieves data from the database.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Renames specified columns.
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped this column: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns.

        Args:
            column_name (str): The name of the column to apply corrections to. Defaults to 'Crop_type'.
            abs_column (str): The name of the column for which absolute values are calculated. Defaults to 'Elevation'.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        """
        Reads weather mapping data from CSV.

        Returns:
            DataFrame: DataFrame containing weather mapping data.
        """
        return read_from_web_CSV(self.weather_map_data)

    def process(self):
        """
        Executes data processing steps.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        weather_map_df = self.weather_station_mapping()
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
        self.df = self.df.drop(columns="Unnamed: 0")
        self.logger.info("Data processing completed.")

### END FUNCTION
