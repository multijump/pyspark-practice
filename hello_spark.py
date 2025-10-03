from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hello") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.show()
print("Spark UI: ", spark.sparkContext.uiWebUrl)
spark.stop()
