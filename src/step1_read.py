from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Hoc-Buoc-1").getOrCreate()

raw = spark.read.parquet("hdfs://namenode:8020/user/tlc/raw/yellow_tripdata_2023-01.parquet")

print("So dong: ", raw.count())
print("So partition: ", raw.rdd.getNumPartitions())
raw.printSchema()
raw.show(5)

print("Phan bo dong theo partition:")
raw.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()
spark.stop()