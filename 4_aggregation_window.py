from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("aggregation_window").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")

top_pu = (df.groupBy("PULocationID")
          .agg(F.count("*").alias("trips"), F.avg("total_amount").alias("avg_fare"))
          .orderBy(F.desc("trips"))
          .limit(10))
top_pu.show();

w = Window.partitionBy("PULocationID").orderBy(F.desc("trip_distance"))
ranked = df.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= 3)
ranked.select("passenger_count", "trip_distance", "rank").show()
spark.stop()
