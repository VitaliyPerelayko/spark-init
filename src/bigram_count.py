import sys
from os import listdir
from os.path import isfile
from re import compile
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, desc, explode
from pyspark.sql.types import ArrayType, StringType

if __name__ == '__main__':
    data_dir = sys.argv[1]

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
        bigrams = []
        prev_word = ''
        words = line.strip().split()

        for word in words:
            word = word.strip().lower()
            word = pattern.sub('', word)
            if word != '':
                if prev_word != '':
                    bigrams.append(prev_word + ' ' + word)
                prev_word = word

        return bigrams

    word_to_counter = (data.filter(data.value != "")
                       .select(explode(split_words('value')).alias('word'))
                       .groupBy('word')
                       .count()
                       .sort(desc('count'))
                       )
    word_to_counter.coalesce(1).toPandas().to_csv('bigrams_result.csv', index=False)

    spark.stop()
