# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession

def create_spark_session():
    sparkSession = SparkSession.builder.appName("my_spark_session").getOrCreate()
    return sparkSession


def create_dateframe(sparkSession):
    return sparkSession.read.text("hdfs://yiwen-macbook:9000/test/spark/readme.md")

def count(dataframe):
    numA = dataframe.filter(dataframe.value.contains('a')).count()
    numB = dataframe.filter(dataframe.value.contains('b')).count()
    print("lines with a: %i, lines with b: %i" % (numA, numB))

def stop_spark_session(sparkSession):
    sparkSession.stop()


if __name__ == '__main__':
    sparkSession = create_spark_session()
    dataframe = create_dateframe(sparkSession)
    count(dataframe)
    stop_spark_session(sparkSession)
