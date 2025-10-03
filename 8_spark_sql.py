from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder.appName("spark_sql").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")
df.createOrReplaceTempView("trips")

res = spark.sql("""
                SELECT date_trunc('hour', to_timestamp(tpep_pickup_datetime)) as hour,
                        count(*) as cnt, avg(total_amount) as avg_fare
                FROM trips
                GROUP BY 1
                ORDER BY 1
                LIMIT 20
                """)
res.show(20, False)
spark.stop()
