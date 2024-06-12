import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':

    # zip archive with txt files
    dataset = sys.argv[1]

    spark = (SparkSession.builder
             .master('local[4]')
             .appName('word_count')
             .getOrCreate())

    data = (spark.read
            .format('txt')
            .option('compression', 'zip')
            .text(dataset))

    spark.stop()
