import os
import logging
from datetime import datetime
from util import get_delimited_file_data
from config import SOURCE,STATIC_FILES,EXPORT_PATH,LOG_PATH
from ingestion import ingest_to_staging
from preprocess import process_file
from transformation import transform_data, aggregate_by_status_year, aggregate_unique_product_per_line\
    , aggregate_sales_variance, aggregate_by_status_year_line
from read_kpi import get_distinct_products_per_product_line, get_total_sales_by_status_year\
    , get_total_sales_variance, get_percentage_change_sales_yoy
from export import export_to_parquet, export_data_to_csv

current_time = datetime.now()
log_file_name = 'pipeline_'+current_time.strftime("%Y%m%d_%H%M%S")+'.log'


def main():
    """
    This is the main function called from app
    Following are the loggical task done using the main function
    1. Reads file by file using loop
    2. Generate results for the 5 queries
    """
    try:
        logger = logging.getLogger()
        handler = logging.FileHandler(filename=os.path.join(os.getcwd(), LOG_PATH['LOGPATH'], log_file_name), mode='a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.info('Collecting files list to do the ETL process')
        tables = get_delimited_file_data(STATIC_FILES['files_to_process'],',')
        for table in tables['filename']:
            filename = table.split('.')[0]
            logger.info(f'Processing file {filename}')
            ingest_to_staging(SOURCE['FOLDER_PATH'],table,SOURCE['TYPE'])
            processed_data = process_file(table)
            transformed_data = transform_data(processed_data)
            res = export_to_parquet(transformed_data)
            logger.info('Parquet file exported')
            aggregated_data = aggregate_by_status_year(transformed_data)
            export_filename = os.path.join(os.getcwd(),EXPORT_PATH['AGG_YEAR_STATUS'],filename+'_agg_year_status.csv')
            res = export_data_to_csv(aggregated_data,export_filename)
            aggregated_data = aggregate_unique_product_per_line(transformed_data)
            export_filename = os.path.join(os.getcwd(),EXPORT_PATH['AGG_PRD_PER_LINE'],filename+'_unique_prd_per_line.csv')
            res = export_data_to_csv(aggregated_data,export_filename)
            aggregated_data = aggregate_sales_variance(transformed_data)
            export_filename = os.path.join(os.getcwd(), EXPORT_PATH['AGG_VARIANCE'],filename + '_unique_agg_variance.csv')
            res = export_data_to_csv(aggregated_data, export_filename)
            aggregated_data = aggregate_by_status_year_line(transformed_data)
            export_filename = os.path.join(os.getcwd(), EXPORT_PATH['AGG_YEAR_STATUS_LINE'], filename + '_agg_year_status_line.csv')
            res = export_data_to_csv(aggregated_data, export_filename)
            logger.info('Data aggregated and exported successfully')
        logger.info('ETL pipeline has been completed')

        status_list = ['Cancelled']
        value = get_total_sales_by_status_year(path=os.path.join(os.getcwd(),EXPORT_PATH['AGG_YEAR_STATUS'])
                                                     ,status=status_list)
        print(f'Query 1: \nThe total sales value of the {status_list} orders : {value}')

        year_list = [2005]
        status_list = ['On Hold']
        value = get_total_sales_by_status_year(path=os.path.join(os.getcwd(), EXPORT_PATH['AGG_YEAR_STATUS']),
                                             status=status_list, year = year_list)
        print(f'Query 2: \nThe total sales value of the orders currently on hold for the year {year_list} and for status {status_list} : {value}')

        productline_list = ['All']
        value = get_distinct_products_per_product_line(path=os.path.join(os.getcwd(), EXPORT_PATH['AGG_PRD_PER_LINE'])
                                                             , product_line = productline_list)
        print(f'Query 3: \nCount of distinct products per {productline_list} line items :')
        print(value)

        year_list = ['All']
        value = get_total_sales_variance(path=os.path.join(os.getcwd(), EXPORT_PATH['AGG_VARIANCE'])
                                                             , year = year_list)
        print(f'Query 4: \nTotal sales variance for sales calculated at both sales price and MSRP for the year {year_list} : {value}')

        status_list = ['Shipped']
        productline_list = ['Classic Cars']
        year_list = [2004,2005]
        value = get_percentage_change_sales_yoy(path=os.path.join(os.getcwd(),EXPORT_PATH['AGG_YEAR_STATUS_LINE'])
                                                     ,status=status_list, product_line=productline_list, year=year_list)
        print(f'Query 5: \nPercentage change in sales YoY for status {status_list} and product line {productline_list} : ')
        print(value)
        logger.info('Data Extracted as per the query')
        logger.info('Completed!!!')
    except Exception as e:
        logger.error('Error occurred ' + str(e))
        print('Error occurred ' + str(e))


if __name__ == '__main__':
    main()



