import os
import logging
from shutil import copyfile
from config import STAGING

logger = logging.getLogger(__name__)


def ingest_to_windows_staging(source_path, filename):
    """
    This function will do following task
    1. Moves the file from source to staging in windows
    """
    copyfile(os.path.join(os.getcwd(), source_path, filename),
             os.path.join(os.getcwd(), os.curdir, STAGING['FOLDER_PATH'], filename))
    logger.info('Moves the soruce file to staging environment - for WINDOWS')


def ingest_to_staging(source_path, filename, source_type):
    """
    This function will do following task
    1. This is the mapper function to move the file from source to staging
    """
    if source_type == 'WINDOWS':
        ingest_to_windows_staging(source_path, filename)
    elif source_type == 'S3':
        pass
    elif source_type == 'Azure-Blob':
        pass
    elif source_type == 'API':
        pass
    else:
        print(f'Ingestion code for {source_type} is not written')
    logger.info('Wrapper call to move the data from source to staging')

