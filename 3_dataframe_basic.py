from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("dataframe").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")

df2 = (df
       .withColumn("trip_distance_km", F.col("trip_distance") * 1.60934)
       .withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
       .filter(F.col("trip_distance") > 0)
       .select("tpep_pickup_datetime", "passenger_count", "trip_distance", "trip_distance_km", "total_amount")
       )
df2.show(5)
spark.stop()
