from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read_csv").master("local[*]").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("raw_data/taxi_zone_lookup.csv")

df.printSchema()
df.show(5, truncate=False)
print("Rows: ", df.count())
spark.stop()
