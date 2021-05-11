import sqlite3
import logging
from util import get_delimited_file_data
import pandas as pd
from config import STATIC_FILES

logger = logging.getLogger(__name__)


def transform_data(data):
    """
    This function will do following task
    1. Calculates the discount
    2. Calculated the sales variance
    """
    discount = get_delimited_file_data(STATIC_FILES['discount_file'],',')
    conn = sqlite3.connect(':memory:')
    data.to_sql('data', conn, index=False)
    discount.to_sql('discount', conn, index=False)
    qry = '''
        select  
            da.*,
            di.discount/100 as DISCOUNT
        from
            data da
                left join discount di 
                    on  da.QUANTITYORDERED >= di.lower 
                    and (da.QUANTITYORDERED < IFNULL(di.upper,da.QUANTITYORDERED+1))
        '''
    data = pd.read_sql_query(qry, conn)
    data['SALES'] = data['SALES'] - (data['SALES']*data['DISCOUNT'])
    data['SALES_BY_MSRP'] = data['MSRP'] * data['QUANTITYORDERED']
    data['SALES_VARIANCE'] = data['SALES'] - data['SALES_BY_MSRP']
    logger.info('Transformation logic applied')
    return data


def aggregate_by_status_year(data):
    """
    This function will do following task
    1. Aggregates the data to year and status
    """
    data = data.groupby(['YEAR','STATUS'])['SALES'].sum()
    logger.info('Data aggregated to get sum of sales by year and status')
    return data


def aggregate_unique_product_per_line(data):
    """
    This function will do following task
    1. Aggregates the data to product line and product code
    """
    data = data[['PRODUCTCODE','PRODUCTLINE']]
    data = data.groupby(['PRODUCTLINE','PRODUCTCODE']).count()
    logger.info('Data aggregated to get unique prodcut line and product code')
    return data


def aggregate_sales_variance(data):
    """
    This function will do following task
    1. Aggregates the data to year
    """
    logger.info('Data aggregated to find sales variance')
    return data.groupby(['YEAR'])['SALES_VARIANCE'].sum()


def aggregate_by_status_year_line(data):
    """
    This function will do following task
    1. Aggregates the data to year, status and product line
    """
    data = data.groupby(['YEAR','STATUS','PRODUCTLINE'])['SALES'].sum()
    logger.info('Data aggregated to get total sales by year, status and product line')
    return data


