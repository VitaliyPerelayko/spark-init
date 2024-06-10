import sys

from pyspark import SparkConf, SparkContext, SparkFiles

import job

if __name__ == '__main__':

    conf = (SparkConf()
            .setMaster('local[4]')
            .setAppName('task_1')
            .set('spark.sql.adaptive.coalescePartitions.initialPartitionNum', '12')
            .set('spark.sql.files.maxPartitionBytes', '350mb'))
    sc = SparkContext(conf=conf)

    customers_file = SparkFiles.get('customers.csv')
    articles_file = SparkFiles.get('articles.csv')
    transaction_file = SparkFiles.get('transactions_train.csv')
    result_file = './result.csv'
    part_date = sys.argv[1]

    result = job.build_data_mart(
        customers=sc.textFile(customers_file),
        articles=sc.textFile(articles_file),
        transaction_train=sc.textFile(transaction_file, 12),
        part_date=part_date
    ).collect()

    file = open(result_file, 'x')
    file.write(
        'part_date,customer_id,customer_group_by_age,transaction_amount,most_exp_article_id,number_of_articles,number_of_product_groups\n')

    for line in result:
        file.write(line + '\n')

    file.close()

    sc.stop()
