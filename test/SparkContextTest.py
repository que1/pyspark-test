# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext


def fun():
    conf = SparkConf().setMaster("local").setAppName("my-app")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("hdfs://yiwen-macbook:9000/test/mapreduce/mapjoin/input/order.txt")
    lines.persist()
    print(lines.count())
    print(lines.first())
    for line in lines.take(10):
        print(line)


if __name__ == '__main__':
    fun()



