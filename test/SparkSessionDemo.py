# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql import Column
import pandas as pd
import numpy as np


"""
spark1.x的入口是SparkContext
conf = SparkConf()
conf.set('master', 'local')
sparkContext = SparkContext(conf=conf)
"""

"""
spark2.0之后入口都是SparkSession
sparkSession = SparkSession.builder.appName("my-spark-app-name").getOrCreate()
"""


def create_json_file():
    df = pd.DataFrame(np.random.rand(5, 5), columns=['a', 'b', 'c', 'd', 'e']).applymap(lambda x: int(x*10))
    file = r"/Users/bytedance/app/spark/random.csv"
    df.to_csv(file, index=False)

def create_df_from_rdd():
    # spark2.0之后入口都是SparkSession
    sparkSession = SparkSession.builder.appName("my-spark-app-name").getOrCreate()
    # 从集合中创建新的RDD
    stringCSVRDD = sparkSession.sparkContext.parallelize([(123, "Katie", 19, "brown"),
                                                          (456, "Michael", 22, "green"),
                                                          (789, "Simone", 23, "blue")])

    # 设置dataFrame将要使用的数据模型，定义列名、类型和是否能为空
    schema = StructType([StructField("id", LongType(), True),
                         StructField("name", StringType(), True),
                         StructField("age", LongType(), True),
                         StructField("eyeColor", StringType(), True)])

    # 创建DataFrame
    swimmers = sparkSession.createDataFrame(stringCSVRDD, schema)
    # 注册为临时表
    swimmers.registerTempTable("swimmers")
    # 使用sql语句
    data = sparkSession.sql("select * from swimmers")

    print(data.collect())
    data.show()
    print("{}{}".format("swimmer numbers : ", swimmers.count()))

    # 关闭sparkSession
    sparkSession.stop()

def create_df_from_json():
    # spark2.0之后入口都是SparkSession
    sparkSession = SparkSession.builder.appName("my-spark-app-name").getOrCreate()
    df = sparkSession.read.json("hdfs://yiwen-macbook:9000/test/spark/pandainfo.json")
    df.show()
    # 关闭sparkSession
    sparkSession.stop()

def create_df_from_csv():
    # spark2.0之后入口都是SparkSession
    sparkSession = SparkSession.builder.appName("my-spark-app-name").getOrCreate()
    df = sparkSession.read.csv('random.csv', header=True, inferSchema=True)
    df.show()
    # 关闭sparkSession
    sparkSession.stop()

def create_df_from_jdbc():
    # spark2.0之后入口都是SparkSession
    sparkSession = SparkSession.builder.appName("my-spark-app-name").getOrCreate()
    df = sparkSession.read.format('jdbc').options(driver="com.mysql.jdbc.Driver",
                                                  url='jdbc:mysql://yiwen-macbook:3306/test',
                                                  dbtable='employees',
                                                  user='test',
                                                  password='test').load()
    df.show()
    df.write.mode("overwrite").options(header="true").json("hdfs://yiwen-macbook:9000/test/spark/df-employees.json")
    # 关闭sparkSession
    sparkSession.stop()

def create_df_from_hive():
    # 创建支持Hive的SparkSession
    appName = "pyspark-hive-example"
    master = "local"

    # 必须有enableHiveSupport()
    sparkSession = SparkSession.builder.appName(appName).master(master)\
        .config("hive.metastore.uris", "thrift://yiwen-macbook:9083")\
        .config("spark.sql.warehouse.dir", "/hive/warehouse")\
        .enableHiveSupport()\
        .getOrCreate()

    df = sparkSession.sql("select * from test.t1")
    df.show()

    # 将数据保存到Hive表
    df.write.mode("overwrite").format("parquet").saveAsTable("test.t2")
    # 查看数据
    df2 = sparkSession.sql("select * from test.t2")
    df2.show()

    # 关闭sparkSession
    sparkSession.stop()


def create_df_from_hive_by_sparkcontext():
    # spark1.x的入口是SparkContext
    conf = SparkConf()
    conf.set('master', 'local')
    sparkContext = SparkContext(conf=conf)
    sqlContext = HiveContext(sparkContext)

    df = sqlContext.sql("select * from test.t1")
    df.show()
    sparkContext.stop()


if __name__=='__main__':
    # create_json_file()
    # create_df_from_jdbc()
    create_df_from_hive()
    # create_df_from_hive_by_sparkcontext()

