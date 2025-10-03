from pyspark.sql import SparkSession, functions as F # type: ignore

spark = SparkSession.builder.appName("reading_writing").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")

(df.withColumn("year", F.year(F.to_timestamp("tpep_pickup_datetime")))
    .withColumn("month", F.month(F.to_timestamp("tpep_pickup_datetime")))
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .parquet("output/taxi_parquet"))

df_parquet = spark.read.parquet("output/taxi_parquet")
df_parquet.show(5)
spark.stop()
