from collections import OrderedDict
from datetime import date, timedelta
from os import getenv, listdir
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

from data_mart_v3_handler import aggregate_data

MIN_PART_DATE = date.fromisoformat('2018-09-30')
RESULT_FILE_NAME = 'data_mart_v3_'

if __name__ == '__main__':

    FOLDER_WITH_DATA = str(getenv('DATA_FOLDER'))
    RESULT_FOLDER = str(getenv('RESULTS_FOLDER'))

    PART_DATE = argv[1]
    DM_CURRENCY = argv[2].upper()
    LOYALITY_LEVEL = int(argv[3])

    existing_data_marts = set(listdir(RESULT_FOLDER))

    month_for_calculating = OrderedDict()
    closest_existing_data_mart = ''

    previous_month_date = date.fromisoformat(PART_DATE)
    previous_month_date = previous_month_date.replace(day=1) - timedelta(days=1)
    previous_month_loyality_level = LOYALITY_LEVEL - 1
    while previous_month_date >= MIN_PART_DATE and previous_month_loyality_level >= 1:
        # break if we found calculated period
        if RESULT_FILE_NAME + previous_month_date.isoformat() in existing_data_marts:
            closest_existing_data_mart = RESULT_FILE_NAME + previous_month_date.isoformat()
            break
        month_for_calculating.update({previous_month_date.isoformat(): previous_month_loyality_level})
        previous_month_date = previous_month_date.replace(day=1) - timedelta(days=1)
        previous_month_loyality_level -= 1

    spark = (SparkSession.builder
             .master('local[4]')
             .appName('data_mart_v3')
             .config('spark.sql.files.maxPartitionBytes', '350mb')
             .config('spark.scheduler.mode', 'FAIR')
             .getOrCreate())

    # CUSTOMERS
    customers_file_path = FOLDER_WITH_DATA + '/customers.csv'
    customers = spark.read.csv(path=customers_file_path, inferSchema=True, header=True)

    customers = customers.select(customers.customer_id.alias('id'),
                                 customers.club_member_status,
                                 customers.fashion_news_frequency,
                                 when(customers.age.isNull() | (customers.age < 23), 'S')
                                 .when(customers.age > 59, 'R')
                                 .otherwise('A')
                                 .alias('customer_group_by_age'))

    # ARTICLES
    articles_file_path = FOLDER_WITH_DATA + '/articles.csv'
    articles = spark.read.csv(path=articles_file_path, inferSchema=True, header=True)
    articles = articles.select(articles.article_id.alias('id'), articles.product_group_name)

    # TRANSACTIONS
    transactions_file_path = FOLDER_WITH_DATA + '/transactions_train_with_currency.csv'
    transactions = spark.read.csv(path=transactions_file_path, header=True)

    # calculate data marts for previous periods
    while month_for_calculating:
        _date, level = month_for_calculating.popitem()
        data_mart = aggregate_data(part_date=_date,
                                   dm_currency=DM_CURRENCY,
                                   loyality_level=level,
                                   transactions=transactions,
                                   customers=customers,
                                   articles=articles)

        data_mart.write.mode('overwrite').parquet(RESULT_FOLDER + '/' + RESULT_FILE_NAME + _date)

    # calculate data mart
    data_mart = aggregate_data(part_date=PART_DATE,
                               dm_currency=DM_CURRENCY,
                               loyality_level=LOYALITY_LEVEL,
                               transactions=transactions,
                               customers=customers,
                               articles=articles)
    data_mart.write.mode('overwrite').parquet(RESULT_FOLDER + '/' + RESULT_FILE_NAME + PART_DATE)

    spark.stop()

