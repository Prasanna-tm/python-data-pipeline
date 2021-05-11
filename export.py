import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from config import EXPORT_PATH

logger = logging.getLogger(__name__)


def export_to_parquet(data):
    """
    This function will do following task
    1. Exports the data to parquet file format
    """
    table = pa.Table.from_pandas(data)
    pq.write_to_dataset(table
                        , root_path=os.path.join(os.getcwd(),EXPORT_PATH['PARQUET_BASE_PATH'])
                        , partition_cols=['YEAR', 'MONTH', 'DAY'])
    logger.info('Input file has been exported to Parquet format')
    return True


def export_data_to_csv(data,filename):
    """
    This function will do following task
    1. Exports the data to CSV file in given path
    """
    data.to_csv(filename)
    logger.info(f'Data frame exported to CSV format {filename}')
    return True

