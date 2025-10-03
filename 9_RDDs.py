from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder.appName("rdds").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5])
squared = rdd.map(lambda x: x*x).collect()
print(squared)
spark.stop()
