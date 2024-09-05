from datetime import date, timedelta
from os import getenv, listdir
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

from data_mart_v2_handler import aggregate_data

MIN_PART_DATE = date.fromisoformat('2018-09-30')
RESULT_FILE_NAME = 'data_mart_v2_'

if __name__ == '__main__':

    folder_with_data = str(getenv('DATA_FOLDER'))
    result_folder = str(getenv('RESULTS_FOLDER'))

    part_date = argv[1]

    part_date_date_format = date.fromisoformat(part_date)
    prev_months = []

    while part_date_date_format > MIN_PART_DATE and len(prev_months) < 2:
        part_date_date_format = part_date_date_format.replace(day=1) - timedelta(days=1)
        prev_months.append(part_date_date_format.isoformat())

    if prev_months:
        existing_data_marts = listdir(result_folder)
        for prev_month in prev_months:
            if (RESULT_FILE_NAME + prev_month) in existing_data_marts:
                prev_months.remove(prev_month)

    prev_months.append(part_date)

    dm_currency = argv[2].upper()

    spark = (SparkSession.builder
             .master('local[4]')
             .appName('data_mart_v2')
             .config('spark.sql.files.maxPartitionBytes', '350mb')
             .config('spark.scheduler.mode', 'FAIR')
             .getOrCreate())

    # CUSTOMERS
    customers_file_path = folder_with_data + '/customers.csv'
    customers = spark.read.csv(path=customers_file_path, inferSchema=True, header=True)

    customers = customers.select(customers.customer_id.alias('id'),
                                 when(customers.age.isNull() | (customers.age < 23), 'S')
                                 .when(customers.age > 59, 'R')
                                 .otherwise('A')
                                 .alias('customer_group_by_age'))

    # ARTICLES
    articles_file_path = folder_with_data + '/articles.csv'
    articles = spark.read.csv(path=articles_file_path, inferSchema=True, header=True)
    articles = articles.select(articles.article_id.alias('id'), articles.product_group_name)

    # TRANSACTIONS
    transactions_file_path = folder_with_data + '/transactions_train_with_currency.csv'
    transactions = spark.read.csv(path=transactions_file_path, header=True)

    for _date in prev_months:
        data_mart = aggregate_data(part_date=_date,
                                   dm_currency=dm_currency,
                                   transactions=transactions,
                                   customers=customers,
                                   articles=articles)
        data_mart.write.mode('overwrite').parquet(result_folder + '/' + RESULT_FILE_NAME + _date)

    spark.stop()
