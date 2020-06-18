from pyspark.sql import SparkSession


partition = "20200616"


appName = "pyspark-hive-example"
master = "local"
sparkSession = SparkSession.builder.appName(appName).master(master) \
            .config("hive.metastore.uris", "thrift://yiwen-macbook:9083") \
            .config("spark.sql.warehouse.dir", "/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()


partition_df = sparkSession.sql("show partitions test.table1")
partition_df.show()

partition_list = partition_df.collect()
# [print(item) for item in partition_list]
# Row类型
max_partition = ""
for item in partition_list:
    if max_partition < item["partition"]:
        max_partition = item["partition"]
    else:
        continue
print("max partition: {}".format(max_partition))


full_df = sparkSession.read.parquet("hdfs://yiwen-macbook:9000/hive/warehouse/test.db/table2")
# print(dataframe)
full_df.printSchema()

"""
方案1
通过在dataframe中执行sql，找到最大值
"""
"""
full_df.registerTempTable("df_table")
first_row = sparkSession.sql("select max(p_date) as max_p_date from df_table").first()
value = first_row["max_p_date"]
print(value)
"""

"""
方案2
dataframe转rdd，找最大值
"""
p_date_df = full_df.select("p_date")
row_data = p_date_df.distinct().rdd.max()
print(row_data[0])


"""
方案3
dataframe通过collect()转python对象，遍历找最大值
"""
"""
p_date_df = full_df.select("p_date")
p_date_list = p_date_df.distinct().collect()
max_p_date = 0
for item in p_date_list:
    if max_p_date < item["p_date"]:
        max_p_date = item["p_date"]
    else:
        continue
print("分区最大值: {}".format(max_p_date))
"""

sparkSession.stop()