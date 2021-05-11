import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_delimited_file_data(file_path,sep):
    """
    This function will do following task
    1. Reads the filename with path as input
    2. Reads field delimted as input
    3. Reads the input file using pandas and returns data as dataframe
    """
    files = pd.read_csv(file_path, sep=sep)
    logger.info('Gets the list of file names')
    return files

