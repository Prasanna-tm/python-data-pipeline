import glob
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def get_total_sales_by_status_year(path, year=['All'], status=['All']):
    """
    This function will do following task
    1. Function get the total sales by status and year
    """
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
    df = pd.concat(dfs, ignore_index=True)

    if year != ['All']:
        df = df[df['YEAR'].isin(year)]
    if status != ['All']:
        df = df[df['STATUS'].isin(status)]
    logger.info('For API call - Calculated total sales by status and year')
    return df['SALES'].sum()


def get_distinct_products_per_product_line(path, product_line=['All']):
    """
    This function will do following task
    1. Function get distinct product count per product line
    """
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
    df = pd.concat(dfs)

    if product_line != ['All']:
        df = df[df['PRODUCTLINE'].isin(product_line)]

    df = df.groupby(['PRODUCTLINE'])['PRODUCTCODE'].nunique()
    logger.info('For API call - Calculated distinct product per product line items')
    return df


def get_total_sales_variance(path, year=['All']):
    """
    This function will do following task
    1. Function get total sales variance
    """
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
    df = pd.concat(dfs)

    if year != ['All']:
        df = df[df['YEAR'].isin(year)]

    df = df['SALES_VARIANCE'].sum()
    logger.info('For API call - Calculated total sales variance')
    return df


def get_percentage_change_sales_yoy(path, status=['All'], product_line=['All'], year=[]):
    """
    This function will do following task
    1. Function get get percentage of change in sales for YoY
    """
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
    df = pd.concat(dfs)

    if product_line != ['All']:
        df = df[df['PRODUCTLINE'].isin(product_line)]
    if status != ['All']:
        df = df[df['STATUS'].isin(status)]

    df = df.groupby(['YEAR'])['SALES'].sum().to_frame()
    df.sort_values(by=['YEAR'],inplace=True, ascending=True)
    df.reset_index(level=0, inplace=True)
    df['YOY'] = 0
    for i in range(1,len(df)):
        df.loc[i,['YOY']] = round(((float(df.loc[i, ['SALES']]) - float(df.loc[i-1,['SALES']]))/float(df.loc[i-1, ['SALES']]))*100,2)
    if len(year) > 0:
        df = df[df['YEAR'].isin(year)]
    logger.info('For API call - Calculated percentage of sales YOY')
    return df

