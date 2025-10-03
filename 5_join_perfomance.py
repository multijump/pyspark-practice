from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("join_performance").getOrCreate()
trips = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")
zones = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/taxi_zone_lookup.csv")

joined = trips.join(zones, trips.PULocationID == zones.LocationID, "left")
joined.select("tpep_pickup_datetime", "Borough", "Zone").show(5)

joined_b = trips.join(broadcast(zones), trips.PULocationID == zones.LocationID)
joined_b.select("tpep_pickup_datetime", "Borough", "Zone").show(5)

spark.stop()
