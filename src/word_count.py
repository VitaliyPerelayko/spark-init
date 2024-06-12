from os import listdir
from os.path import isfile
from re import compile
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, desc, explode
from pyspark.sql.types import ArrayType, StringType

if __name__ == '__main__':
    data_dir = ''

    spark = (SparkSession.builder
             .master('local[4]')
             .appName('word_count')
             .getOrCreate())

    data = (spark.read
            .format('txt')
            .text([data_dir + f for f in listdir(data_dir) if isfile(data_dir + f)])
            )

    pattern = compile("[^a-z1-9']+")


    @udf(returnType=ArrayType(StringType()))
    def split_words(line: str) -> List[str]:
        words = line.strip().split()
        true_words = []
        for word in words:
            word = word.strip().lower()
            word = pattern.sub('', word)
            if word != '':
                true_words.append(word)
        return true_words


    word_to_counter = (data.filter(data.value != "")
                       .select(explode(split_words('value')).alias('word'))
                       .groupBy('word')
                       .count()
                       .sort(desc('count'))
                       )
    word_to_counter.coalesce(1).write.csv('wc_result.scv', header=True)

    spark.stop()
