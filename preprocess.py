import json
import os
import pandas as pd
import logging
from config import STAGING, PRE_PROCESS, PRODUCT_LINE

logger = logging.getLogger(__name__)


def read_file(filename):
    """
    This function will do following task
    1. Reads the json file and returns the file content
    """
    with open(os.path.join(os.getcwd(), STAGING['FOLDER_PATH'], filename)) as data_file:
        json_data = json.load(data_file)
    logger.info('Reads the json file and retruns the file content')
    return json_data


def convert_json_to_csv(json_data):
    """
    This function will do following task
    1. Converts json into dataframe using and return the dataframe
    """
    df_main = pd.DataFrame(json_data)
    df_main.drop('attributes', inplace=True, axis=1)
    df_attributes = pd.json_normalize(json_data, 'attributes')
    df = pd.concat([df_main, df_attributes], axis=1)
    logger.info('Converted json to csv format')
    return df


def data_quality_check(df):
    """
    This function will do following task
    1. Will do basic quality check on number field and replaces with zero for error records
    """
    pd.to_numeric(df['QUANTITYORDERED'], errors='ignore').notnull().all()
    pd.to_numeric(df['PRICEEACH'], errors='ignore').notnull().all()
    pd.to_numeric(df['SALES'], errors='ignore').notnull().all()
    pd.to_numeric(df['MSRP'], errors='ignore').notnull().all()

    df['QUANTITYORDERED'] = df['QUANTITYORDERED'].fillna(0)
    df['PRICEEACH'] = df['PRICEEACH'].fillna(0)
    df['SALES'] = df['SALES'].fillna(0)
    df['MSRP'] = df['MSRP'].fillna(0)
    logger.info('Data quality check completed')
    return df


def add_additional_columns(df):
    """
    This function will do following task
    1. Generates additional columns for further processing
    """
    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], format='%m/%d/%Y %H:%M')
    df['YEAR'] = df['ORDERDATE'].dt.year
    df['MONTH'] = df['ORDERDATE'].dt.month
    df['DAY'] = df['ORDERDATE'].dt.day
    logger.info('Additional columns YEAR, MONTH, DAY has been added')
    return df


def filter_data(df):
    """
    This function will do following task
    1. Filters the data for the product line given in the config file
    """
    df = df[df['PRODUCTLINE'].isin(PRODUCT_LINE)]
    logger.info(f'Filtered data for product line {PRODUCT_LINE}')
    return df


def export_to_csv(df, filename):
    """
    This function will do following task
    1. Exports the dataframe to csv file mentioned in the argument
    """
    df.to_csv(os.path.join(os.getcwd(), PRE_PROCESS['FOLDER_PATH'], str(filename.split('.')[0]) + '.csv'))
    logger.info(f'exported data to storage')
    return 1


def process_file(filename):
    """
    This function will do following task
    1. Reads the json file
    2. Converts the json to dataframe
    3. Does the basic data quality
    4. Generates additional columns
    5. Filters the data with config value
    6. Exports the data to folder in CSV format
    """
    json_data = read_file(filename)
    df_data = convert_json_to_csv(json_data)
    df_qc = data_quality_check(df_data)
    df_curated = add_additional_columns(df_qc)
    df_filtered = filter_data(df_curated)
    export_to_csv(df_filtered, filename)
    logger.info(f'{filename} File has been processed')
    return df_filtered
